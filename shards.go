package mph

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"os"
	"path"
	"sync"

	"golang.org/x/sync/errgroup"
)

type tabFile struct {
	*os.File
	buff *bufio.Writer
}

func newTabFile(tFile *os.File, buffSzBts int) *tabFile {
	return &tabFile{
		File: tFile,
		buff: bufio.NewWriterSize(tFile, buffSzBts),
	}
}

func (tf *tabFile) Write(p []byte) (n int, err error) {
	return tf.buff.Write(p)
}

func (tf *tabFile) Close() error {
	if err := tf.buff.Flush(); err != nil {
		return err
	}
	return tf.File.Close()
}

type ShardedTable struct {
	counts       []uint
	prefBits     int
	keyLen       int
	buffSzBts    int
	mphDirPath   string
	tables       []*Table
	tabFiles     []*tabFile
	tabFilePaths []string
}

func NewShardedTable(
	keyLen, prefBits, buffSzBts int,
	mphDirPath string,
) (*ShardedTable, error) {
	if prefBits < 1 {
		return nil, fmt.Errorf("prefixBits must be >= 1")
	}
	if prefBits > 32 {
		return nil, fmt.Errorf("prefixBits must be <= 32 (memory constraints)")
	}
	tabFiles := make([]*tabFile, 1<<prefBits)
	counts := make([]uint, 1<<prefBits)
	return &ShardedTable{
		keyLen:     keyLen,
		buffSzBts:  buffSzBts,
		prefBits:   prefBits,
		mphDirPath: mphDirPath,
		tabFiles:   tabFiles,
		counts:     counts,
	}, nil
}

func (st *ShardedTable) Put(key []byte) error {
	if len(key) != st.keyLen {
		return fmt.Errorf("invalid key length %d, expected %d", len(key), st.keyLen)
	}
	shardIdx, err := shardIndex(key, st.prefBits)
	if err != nil {
		return err
	}
	if st.tabFiles[shardIdx] == nil {
		tabFilePath := path.Join(st.mphDirPath, fmt.Sprintf("%d.bin", shardIdx))
		tblFile, err := os.OpenFile(
			tabFilePath,
			os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
			0644,
		)
		if err != nil {
			return fmt.Errorf("failed to open table file %s: %v", tabFilePath, err)
		}
		st.tabFiles[shardIdx] = newTabFile(tblFile, st.buffSzBts)
	}
	if _, err = st.tabFiles[shardIdx].Write(key); err != nil {
		return err
	}
	st.counts[shardIdx]++
	return nil
}

func (st *ShardedTable) Commit(grp *errgroup.Group) error {
	mu := &sync.Mutex{}
	st.tables = make([]*Table, len(st.tabFiles))
	st.tabFilePaths = make([]string, len(st.tabFiles))
	for i, tblFile := range st.tabFiles {
		if tblFile == nil {
			continue
		}
		st.tabFilePaths[i] = tblFile.Name()
		idx := i
		keyFile := tblFile
		commitFn := func() error {
			err := keyFile.Close()
			if err != nil {
				return err
			}
			tFile, err := os.Open(keyFile.Name())
			if err != nil {
				return err
			}
			table, err := BuildFromFile(tFile, st.keyLen)
			if err != nil {
				return err
			}
			mu.Lock()
			st.tables[idx] = table
			mu.Unlock()
			return nil
		}
		if grp != nil {
			grp.Go(commitFn)
		} else {
			if err := commitFn(); err != nil {
				return err
			}
		}
	}
	st.tabFiles = nil
	return nil
}

func (st *ShardedTable) Lookup(s []byte) (n uint32, ok bool) {
	if len(s) != st.keyLen {
		return 0, false
	}
	if st.tables == nil {
		return 0, false
	}
	shardIdx, err := shardIndex(s, st.prefBits)
	if err != nil {
		return 0, false
	}
	if st.tables[shardIdx] == nil {
		return 0, false
	}
	return st.tables[shardIdx].Lookup(s)
}

func (st *ShardedTable) GetCounts() []uint {
	return st.counts
}

func (st *ShardedTable) DumpToFile(filePath string) error {
	dumpFile, err := os.OpenFile(
		filePath,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0644,
	)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(dumpFile)
	if err = encoder.Encode(st.counts); err != nil {
		return err
	}
	if err = encoder.Encode(st.prefBits); err != nil {
		return err
	}
	if err = encoder.Encode(st.keyLen); err != nil {
		return err
	}
	if err = encoder.Encode(st.mphDirPath); err != nil {
		return err
	}
	if err = encoder.Encode(st.tabFilePaths); err != nil {
		return err
	}
	for _, table := range st.tables {
		if table == nil {
			continue
		}
		if err = table.DumpToKeysFile(); err != nil {
			return err
		}
	}
	return dumpFile.Close()
}

func LoadShardedTableFromFile(filePath string) (*ShardedTable, error) {
	dumpFile, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer dumpFile.Close()

	gobDecoder := gob.NewDecoder(dumpFile)
	var st ShardedTable
	if err = gobDecoder.Decode(&st.counts); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&st.prefBits); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&st.keyLen); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&st.mphDirPath); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&st.tabFilePaths); err != nil {
		return nil, err
	}
	st.tables = make([]*Table, len(st.counts))
	for i, cnt := range st.counts {
		if cnt == 0 {
			continue
		}
		tblFile, err := os.Open(st.tabFilePaths[i])
		if err != nil {
			return nil, err
		}
		st.tables[i], err = LoadFromKeysFile(tblFile)
		if err != nil {
			return nil, err
		}
	}
	return &st, nil
}

func shardIndex(key []byte, prefBits int) (uint64, error) {
	numBytes, rem := prefBits>>3, prefBits&7
	if len(key) < numBytes || (rem > 0 && len(key) <= numBytes) {
		return 0, fmt.Errorf("key too short for %d-bit prefix", prefBits)
	}
	var shardIdx uint64
	for i := 0; i < numBytes; i++ {
		shardIdx |= uint64(key[i]) << (8 * i)
	}
	if rem > 0 {
		remainingBits := key[numBytes] >> (8 - rem)
		shardIdx |= uint64(remainingBits) << (8 * numBytes)
	}
	return shardIdx, nil
}
