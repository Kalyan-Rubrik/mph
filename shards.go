package mph

import (
	"encoding/gob"
	"fmt"
	"os"
	"path"
	"sync"

	"golang.org/x/sync/errgroup"
)

type ShardedTable struct {
	counts       []uint
	prefBits     int
	keyLen       int
	suffixOnly   bool
	mphDirPath   string
	tables       []*Table
	tabFiles     []*os.File
	tabFilePaths []string
}

func NewShardedTable(
	keyLen, prefBits int,
	suffixOnly bool,
	mphDirPath string,
) (*ShardedTable, error) {
	if prefBits < 1 {
		return nil, fmt.Errorf("prefixBits must be >= 1")
	}
	if prefBits > 32 {
		return nil, fmt.Errorf("prefixBits must be <= 32 (memory constraints)")
	}
	tabFiles := make([]*os.File, 1<<prefBits)
	counts := make([]uint, 1<<prefBits)
	return &ShardedTable{
		keyLen:     keyLen,
		prefBits:   prefBits,
		suffixOnly: suffixOnly,
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
		tabFile, err := os.OpenFile(
			tabFilePath,
			os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
			0644,
		)
		if err != nil {
			return fmt.Errorf("failed to open table file %s: %v", tabFilePath, err)
		}
		st.tabFiles[shardIdx] = tabFile
	}
	suffix := keySuffix(key, st.suffixOnly, st.prefBits)
	if _, err = st.tabFiles[shardIdx].Write(suffix); err != nil {
		return err
	}
	st.counts[shardIdx]++
	return nil
}

func (st *ShardedTable) Commit(grp *errgroup.Group) error {
	mu := &sync.Mutex{}
	st.tables = make([]*Table, len(st.tabFiles))
	st.tabFilePaths = make([]string, len(st.tabFiles))
	for i, tabFile := range st.tabFiles {
		if tabFile == nil {
			continue
		}
		st.tabFilePaths[i] = tabFile.Name()
		idx := i
		keyFile := tabFile
		commitFn := func() error {
			err := keyFile.Close()
			if err != nil {
				return err
			}
			keyFile, err = os.Open(keyFile.Name())
			if err != nil {
				return err
			}
			table, err := BuildFromFile(keyFile, st.keyLen)
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
	suffix := keySuffix(s, st.suffixOnly, st.prefBits)
	return st.tables[shardIdx].Lookup(suffix)
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
	if err = encoder.Encode(st.suffixOnly); err != nil {
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
	if err = gobDecoder.Decode(&st.suffixOnly); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&st.mphDirPath); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&st.tabFilePaths); err != nil {
		return nil, err
	}
	st.tables = make([]*Table, len(st.counts))
	st.tabFiles = make([]*os.File, len(st.counts))
	for i, cnt := range st.counts {
		if cnt == 0 {
			continue
		}
		st.tabFiles[i], err = os.Open(st.tabFilePaths[i])
		if err != nil {
			return nil, err
		}
		st.tables[i], err = LoadFromKeysFile(st.tabFiles[i])
		if err != nil {
			return nil, err
		}
	}
	return &st, nil
}

func keySuffix(key []byte, suffixOnly bool, prefBits int) []byte {
	if !suffixOnly {
		return key
	}
	numBytes, rem := prefComps(prefBits)
	suffix := append([]byte{}, key[numBytes:]...)
	if rem > 0 && len(suffix) > 0 {
		suffix[0] = key[numBytes] & (0xff >> rem)
	}
	return suffix
}

func shardIndex(key []byte, prefBits int) (uint64, error) {
	numBytes, rem := prefComps(prefBits)
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

func prefComps(prefBits int) (int, int) {
	numBytes := prefBits >> 3
	rem := prefBits & 7
	return numBytes, rem
}
