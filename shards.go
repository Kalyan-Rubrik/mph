package mph

import (
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
)

type ShardedTable struct {
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
	for _, key := range keys {
		shardIdx, err := shardIndex(key, prefBits)
		if err != nil {
			return nil, err
		}
		suffix := keySuffix(key, suffixOnly, prefBits)
		shardedKeys[shardIdx] = append(shardedKeys[shardIdx], suffix)
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
