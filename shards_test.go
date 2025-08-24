package mph

import (
	"crypto/sha1"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestBuildSharded(t *testing.T) {
	const prefBits = 3
	keys := [][]byte{
		binary.BigEndian.AppendUint16([]byte{}, 0b1100000000000001),
		binary.BigEndian.AppendUint16([]byte{}, 0b1110000000000010),
		binary.BigEndian.AppendUint16([]byte{}, 0b0110000000000110),
		binary.BigEndian.AppendUint16([]byte{}, 0b0100000000000011),
		binary.BigEndian.AppendUint16([]byte{}, 0b1110000000000111),
		binary.BigEndian.AppendUint16([]byte{}, 0b0100000000000100),
		binary.BigEndian.AppendUint16([]byte{}, 0b1100000000000101),
	}

	mphDir := filepath.Join(os.TempDir(), "mph_test_suffix")
	err := os.MkdirAll(mphDir, 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(mphDir)

	keyLen := len(keys[0])
	st, err := NewShardedTable(keyLen, prefBits, 1024, mphDir)
	if err != nil {
		t.Fatal(err)
	}

	for _, key := range keys {
		err = st.Put(key)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = st.Commit(nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, key := range keys {
		if _, ok := st.Lookup(key); !ok {
			t.Errorf("Lookup(%s): got !ok; want ok", string(key))
		}
	}

	shardedFilePath := filepath.Join(os.TempDir(), "sharded.mph")
	err = st.DumpToFile(shardedFilePath)
	if err != nil {
		t.Fatal(err)
	}
	st, err = LoadShardedTableFromFile(shardedFilePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if _, ok := st.Lookup(key); !ok {
			t.Errorf("Lookup(%s): got !ok; want ok", string(key))
		}
	}
}

func TestBuildShardedOnLargeDataset(t *testing.T) {
	const (
		numKeys         = 1_000_000
		prefBits        = 16
		mphDir          = "/tmp/mph_test"
		shardedFilePath = "/tmp/sharded.mph"
		buffSzBts       = 65536 * 1024
	)
	err := os.MkdirAll(mphDir, 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(mphDir)

	st, err := NewShardedTable(sha1.Size, prefBits, buffSzBts, mphDir)
	if err != nil {
		t.Fatal(err)
	}

	keys := make([][]byte, numKeys)
	hasher := sha1.New()
	startTime := time.Now()
	for i := 0; i < numKeys; i++ {
		hasher.Write([]byte("key" + strconv.Itoa(i)))
		keys[i] = hasher.Sum(nil)
		if err = st.Put(keys[i]); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("Put took %v sec", time.Since(startTime).Seconds())

	startTime = time.Now()
	err = st.Commit(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Commit took %v sec", time.Since(startTime).Seconds())

	startTime = time.Now()
	for _, key := range keys {
		if _, ok := st.Lookup(key); !ok {
			t.Errorf("Lookup(%s): got !ok; want ok", string(key))
		}
	}
	t.Logf("Lookup took %v sec", time.Since(startTime).Seconds())

	err = st.DumpToFile(shardedFilePath)
	if err != nil {
		t.Fatal(err)
	}

	st, err = LoadShardedTableFromFile(shardedFilePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if _, ok := st.Lookup(key); !ok {
			t.Errorf("Lookup(%s): got !ok; want ok", string(key))
		}
	}
}
