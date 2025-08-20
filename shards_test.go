package mph

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildShardedSuffixKeys(t *testing.T) {
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
	st, err := NewShardedTable(keyLen, prefBits, true, mphDir)
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

func TestBuildShardedFullKeys(t *testing.T) {
	const (
		prefBits     = 3
		expNumShards = 1 << prefBits
		expNumTabs   = 4
	)
	keys := [][]byte{
		binary.BigEndian.AppendUint16([]byte{}, 0b1100000000000001),
		binary.BigEndian.AppendUint16([]byte{}, 0b1110000000000010),
		binary.BigEndian.AppendUint16([]byte{}, 0b0110000000000110),
		binary.BigEndian.AppendUint16([]byte{}, 0b0100000000000011),
		binary.BigEndian.AppendUint16([]byte{}, 0b1110000000000111),
		binary.BigEndian.AppendUint16([]byte{}, 0b0100000000000100),
		binary.BigEndian.AppendUint16([]byte{}, 0b1100000000000101),
	}

	keySets := [][][]byte{
		{
			binary.BigEndian.AppendUint16([]byte{}, 0b0100000000000011),
			binary.BigEndian.AppendUint16([]byte{}, 0b0100000000000100),
		},
		{
			binary.BigEndian.AppendUint16([]byte{}, 0b0110000000000110),
		},
		{
			binary.BigEndian.AppendUint16([]byte{}, 0b1100000000000001),
			binary.BigEndian.AppendUint16([]byte{}, 0b1100000000000101),
		},
		{
			binary.BigEndian.AppendUint16([]byte{}, 0b1110000000000010),
			binary.BigEndian.AppendUint16([]byte{}, 0b1110000000000111),
		},
	}

	var err error
	expTables := make([]*Table, expNumTabs)
	expTables[0], err = Build(keySets[0])
	if err != nil {
		t.Fatal(err)
	}

	expTables[1], err = Build(keySets[1])
	if err != nil {
		t.Fatal(err)
	}

	expTables[2], err = Build(keySets[2])
	if err != nil {
		t.Fatal(err)
	}

	expTables[3], err = Build(keySets[3])
	if err != nil {
		t.Fatal(err)
	}

	tmpDir := filepath.Join(os.TempDir(), "mph_test_full")
	err = os.MkdirAll(tmpDir, 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	keyLen := len(keys[0])
	st, err := NewShardedTable(keyLen, prefBits, false, tmpDir)
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
