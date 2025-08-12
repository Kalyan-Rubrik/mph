package mph

import (
	"encoding/gob"
	"fmt"
	"os"
	"sync"

	"golang.org/x/sync/errgroup"
)

type ShardedTable struct {
	counts     []uint
	tables     []*Table
	prefBits   int
	suffixOnly bool
}

func (st *ShardedTable) Lookup(s []byte) (n uint32, ok bool) {
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
	if err = encoder.Encode(st.suffixOnly); err != nil {
		return err
	}
	for _, table := range st.tables {
		if table == nil {
			continue
		}
		if err = table.encode(encoder); err != nil {
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
	if err = gobDecoder.Decode(&st.suffixOnly); err != nil {
		return nil, err
	}
	st.tables = make([]*Table, len(st.counts))
	for i, cnt := range st.counts {
		if cnt == 0 {
			continue
		}
		st.tables[i], err = decode(gobDecoder)
		if err != nil {
			return nil, err
		}
	}
	return &st, nil
}

func BuildSharded(
	keys [][]byte,
	prefBits int,
	suffixOnly bool,
	grp *errgroup.Group,
) (*ShardedTable, error) {
	if prefBits < 1 {
		return nil, fmt.Errorf("prefixBits must be >= 1")
	}
	if prefBits > 32 {
		return nil, fmt.Errorf("prefixBits must be <= 32 (memory constraints)")
	}
	tables := make([]*Table, 1<<prefBits)
	shardedKeys := make([][][]byte, 1<<prefBits)
	counts := make([]uint, 1<<prefBits)
	for _, key := range keys {
		shardIdx, err := shardIndex(key, prefBits)
		if err != nil {
			return nil, err
		}
		suffix := keySuffix(key, suffixOnly, prefBits)
		shardedKeys[shardIdx] = append(shardedKeys[shardIdx], suffix)
		counts[shardIdx]++
	}
	mu := &sync.Mutex{}
	for i, shardKeys := range shardedKeys {
		if len(shardKeys) == 0 {
			continue
		}
		idx := i
		sKeys := shardKeys
		buildFn := func() error {
			table, err := Build(sKeys)
			if err != nil {
				return err
			}
			mu.Lock()
			tables[idx] = table
			mu.Unlock()
			return nil
		}
		if grp != nil {
			grp.Go(buildFn)
		} else {
			if err := buildFn(); err != nil {
				return nil, err
			}
		}
	}
	return &ShardedTable{
		counts:     counts,
		tables:     tables,
		prefBits:   prefBits,
		suffixOnly: suffixOnly,
	}, nil
}

func keySuffix(key []byte, suffixOnly bool, prefBits int) []byte {
	if !suffixOnly {
		return key
	}
	numBytes, rem := prefComps(prefBits)
	suffix := key[numBytes:]
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
