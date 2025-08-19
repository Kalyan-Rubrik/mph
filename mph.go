// Package mph implements a minimal perfect hash table over strings.
package mph

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
)

// A Table is an immutable hash table that provides constant-time lookups of key
// indices using a minimal perfect hash.
type Table struct {
	keysFile   *os.File
	keyLen     int
	keys       [][]byte
	level0     []uint32 // power of 2 size
	level0Mask int      // len(Level0) - 1
	level1     []uint32 // power of 2 size >= len(keys)
	level1Mask int      // len(Level1) - 1
}

// Build builds a Table from keys using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
// Returns an error if duplicate keys are detected.
func Build(keys [][]byte) (*Table, error) {
	var (
		level0        = make([]uint32, nextPow2(len(keys)/4))
		level0Mask    = len(level0) - 1
		level1        = make([]uint32, nextPow2(len(keys)))
		level1Mask    = len(level1) - 1
		sparseBuckets = make([][]int, len(level0))
		zeroSeed      = murmurSeed(0)
	)
	for i, s := range keys {
		n := int(zeroSeed.hash(s)) & level0Mask
		sparseBuckets[n] = append(sparseBuckets[n], i)
	}
	var buckets []indexBucket
	for n, vals := range sparseBuckets {
		if len(vals) > 0 {
			buckets = append(buckets, indexBucket{n, vals})
		}
	}
	sort.Sort(bySize(buckets))

	occ := make([]bool, len(level1))
	var tmpOcc []int
	for _, bucket := range buckets {
		var seed murmurSeed
		remAttempts := math.MaxUint32
	trySeed:
		if remAttempts == 0 {
			return nil, fmt.Errorf("failed to find slots for bucket (likely due to duplicate keys)")
		}
		remAttempts--
		tmpOcc = tmpOcc[:0]
		for _, i := range bucket.vals {
			n := int(seed.hash(keys[i])) & level1Mask
			if occ[n] {
				for _, n := range tmpOcc {
					occ[n] = false
				}
				seed++
				goto trySeed
			}
			occ[n] = true
			tmpOcc = append(tmpOcc, n)
			level1[n] = uint32(i)
		}
		level0[bucket.n] = uint32(seed)
	}

	return &Table{
		keys:       keys,
		keyLen:     len(keys),
		level0:     level0,
		level0Mask: level0Mask,
		level1:     level1,
		level1Mask: level1Mask,
	}, nil
}

func BuildFromFile(keysFile *os.File, keyLen int) (*Table, error) {
	numKeys, err := getNumKeys(keysFile, keyLen)
	if err != nil {
		return nil, err
	}

	var (
		level0        = make([]uint32, nextPow2(int(numKeys)/4))
		level0Mask    = len(level0) - 1
		level1        = make([]uint32, nextPow2(int(numKeys)))
		level1Mask    = len(level1) - 1
		sparseBuckets = make([][]int, len(level0))
		zeroSeed      = murmurSeed(0)
	)

	for i := 0; ; i++ {
		key := make([]byte, keyLen)
		_, err = io.ReadFull(keysFile, key)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		n := int(zeroSeed.hash(key)) & level0Mask
		sparseBuckets[n] = append(sparseBuckets[n], i)
	}

	_, err = keysFile.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	var buckets []indexBucket
	for n, vals := range sparseBuckets {
		if len(vals) > 0 {
			buckets = append(buckets, indexBucket{n, vals})
		}
	}
	sort.Sort(bySize(buckets))

	occ := make([]bool, len(level1))
	var tmpOcc []int
	for _, bucket := range buckets {
		bucketKeys := make(map[int][]byte, len(bucket.vals))
		err = keysAtIndexes(keysFile, bucketKeys, keyLen, bucket.vals...)
		if err != nil {
			return nil, err
		}
		var seed murmurSeed
		remAttempts := math.MaxUint32
	trySeed:
		if remAttempts == 0 {
			return nil, fmt.Errorf("failed to find slots for bucket (likely due to duplicate keys)")
		}
		remAttempts--
		tmpOcc = tmpOcc[:0]
		for _, i := range bucket.vals {
			n := int(seed.hash(bucketKeys[i])) & level1Mask
			if occ[n] {
				for _, n := range tmpOcc {
					occ[n] = false
				}
				seed++
				goto trySeed
			}
			occ[n] = true
			tmpOcc = append(tmpOcc, n)
			level1[n] = uint32(i)
		}
		level0[bucket.n] = uint32(seed)
	}

	return &Table{
		keysFile:   keysFile,
		keyLen:     keyLen,
		level0:     level0,
		level0Mask: level0Mask,
		level1:     level1,
		level1Mask: level1Mask,
	}, nil
}

func getNumKeys(keysFile *os.File, keyLen int) (int64, error) {
	keysFileStats, err := keysFile.Stat()
	if err != nil {
		return 0, err
	}
	keysFileLen := keysFileStats.Size()
	if keysFileLen%int64(keyLen) != 0 {
		return 0, fmt.Errorf(
			"keys file length (%d) is not a multiple of key length (%d)",
			keysFileLen,
			keyLen,
		)
	}
	return keysFileLen / int64(keyLen), nil
}

func keyAtIdx(keysFile *os.File, idx, keyLen int) ([]byte, error) {
	if _, err := keysFile.Seek(int64(idx*keyLen), 0); err != nil {
		return nil, err
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(keysFile, key); err != nil {
		return nil, err
	}
	return key, nil
}

func keysAtIndexes(
	keysFile *os.File,
	bucketKeys map[int][]byte,
	keyLen int,
	indexes ...int,
) error {
	for _, idx := range indexes {
		key, err := keyAtIdx(keysFile, idx, keyLen)
		if err != nil {
			return err
		}
		bucketKeys[idx] = key
	}
	return nil
}

func nextPow2(n int) int {
	for i := 1; ; i *= 2 {
		if i >= n {
			return i
		}
	}
}

// Lookup searches for s in t and returns its index and whether it was found.
func (t *Table) Lookup(s []byte) (n uint32, ok bool) {
	if t.keys != nil {
		return t.lookupInMem(s)
	}
	return t.lookupFromFile(s)
}

func (t *Table) lookupFromFile(s []byte) (n uint32, ok bool) {
	i0 := int(murmurSeed(0).hash(s)) & t.level0Mask
	seed := t.level0[i0]
	i1 := int(murmurSeed(seed).hash(s)) & t.level1Mask
	n = t.level1[i1]
	key, err := keyAtIdx(t.keysFile, int(n), t.keyLen)
	if err != nil {
		return 0, false
	}
	return n, bytes.Equal(s, key)
}

func (t *Table) lookupInMem(s []byte) (n uint32, ok bool) {
	i0 := int(murmurSeed(0).hash(s)) & t.level0Mask
	seed := t.level0[i0]
	i1 := int(murmurSeed(seed).hash(s)) & t.level1Mask
	n = t.level1[i1]
	return n, bytes.Equal(s, t.keys[int(n)])
}

func (t *Table) DumpToKeysFile() error {
	if t.keysFile == nil {
		return fmt.Errorf("keys file not set")
	}
	numKeys, err := getNumKeys(t.keysFile, t.keyLen)
	if err != nil {
		return fmt.Errorf("error fetching key count: %v", err)
	}
	if err = t.keysFile.Close(); err != nil {
		return err
	}

	t.keysFile, err = os.OpenFile(
		t.keysFile.Name(),
		os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return err
	}

	encoder := gob.NewEncoder(t.keysFile)
	if err = encoder.Encode(t.level0); err != nil {
		return err
	}
	if err = encoder.Encode(t.level0Mask); err != nil {
		return err
	}
	if err = encoder.Encode(t.level1); err != nil {
		return err
	}
	if err = encoder.Encode(t.level1Mask); err != nil {
		return err
	}
	err = binary.Write(t.keysFile, binary.LittleEndian, uint32(t.keyLen))
	if err != nil {
		return err
	}
	err = binary.Write(t.keysFile, binary.LittleEndian, uint32(numKeys))
	if err != nil {
		return err
	}
	return t.keysFile.Close()
}

func (t *Table) DumpToFile(filePath string) error {
	dumpFile, err := os.OpenFile(
		filePath,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0644,
	)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(dumpFile)
	if err = t.encode(encoder); err != nil {
		return err
	}
	return dumpFile.Close()
}

func (t *Table) encode(encoder *gob.Encoder) (err error) {
	if t.keys != nil {
		if err = encoder.Encode(0); err != nil {
			return err
		}
		if err = encoder.Encode(t.keys); err != nil {
			return err
		}
	}
	if t.keysFile != nil {
		if err = encoder.Encode(1); err != nil {
			return err
		}
		if err = encoder.Encode(t.keysFile.Name()); err != nil {
			return err
		}
	}
	if err = encoder.Encode(t.keyLen); err != nil {
		return err
	}
	if err = encoder.Encode(t.level0); err != nil {
		return err
	}
	if err = encoder.Encode(t.level0Mask); err != nil {
		return err
	}
	if err = encoder.Encode(t.level1); err != nil {
		return err
	}
	if err = encoder.Encode(t.level1Mask); err != nil {
		return err
	}
	return
}

func LoadFromKeysFile(keysFile *os.File) (*Table, error) {
	_, err := keysFile.Seek(-8, 2)
	if err != nil {
		return nil, err
	}
	buff := make([]byte, 8)
	_, err = io.ReadFull(keysFile, buff)
	if err != nil {
		return nil, err
	}

	var numKeys, keyLen uint32
	_, err = binary.Decode(buff[4:], binary.LittleEndian, &numKeys)
	if err != nil {
		return nil, err
	}

	_, err = binary.Decode(buff[:4], binary.LittleEndian, &keyLen)
	if err != nil {
		return nil, err
	}

	t := Table{keysFile: keysFile, keyLen: int(keyLen)}
	_, err = keysFile.Seek(int64(numKeys)*int64(t.keyLen), 0)
	if err != nil {
		return nil, err
	}

	gobDecoder := gob.NewDecoder(keysFile)
	if err = gobDecoder.Decode(&t.level0); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&t.level0Mask); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&t.level1); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&t.level1Mask); err != nil {
		return nil, err
	}

	return &t, nil
}

func LoadFromFile(filePath string) (*Table, error) {
	dumpFile, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer dumpFile.Close()

	gobDecoder := gob.NewDecoder(dumpFile)
	return decode(gobDecoder)
}

func decode(gobDecoder *gob.Decoder) (*Table, error) {
	var t Table
	var err error
	var tag int
	if err = gobDecoder.Decode(&tag); err != nil {
		return nil, err
	}
	if tag == 0 {
		if err = gobDecoder.Decode(&t.keys); err != nil {
			return nil, err
		}
	}
	if tag == 1 {
		var keysFilePath string
		if err = gobDecoder.Decode(&keysFilePath); err != nil {
			return nil, err
		}
		t.keysFile, err = os.Open(keysFilePath)
		if err != nil {
			return nil, err
		}
	}
	if err = gobDecoder.Decode(&t.keyLen); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&t.level0); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&t.level0Mask); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&t.level1); err != nil {
		return nil, err
	}
	if err = gobDecoder.Decode(&t.level1Mask); err != nil {
		return nil, err
	}
	return &t, nil
}

type indexBucket struct {
	n    int
	vals []int
}

type bySize []indexBucket

func (s bySize) Len() int           { return len(s) }
func (s bySize) Less(i, j int) bool { return len(s[i].vals) > len(s[j].vals) }
func (s bySize) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
