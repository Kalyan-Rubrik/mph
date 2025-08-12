package mph

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildShardedSuffixKeys(t *testing.T) {
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

	keySetSuffixes := [][][]byte{
		{
			binary.BigEndian.AppendUint16([]byte{}, 0b0000000000000011),
			binary.BigEndian.AppendUint16([]byte{}, 0b0000000000000100),
		},
		{
			binary.BigEndian.AppendUint16([]byte{}, 0b0000000000000110),
		},
		{
			binary.BigEndian.AppendUint16([]byte{}, 0b0000000000000001),
			binary.BigEndian.AppendUint16([]byte{}, 0b0000000000000101),
		},
		{
			binary.BigEndian.AppendUint16([]byte{}, 0b0000000000000010),
			binary.BigEndian.AppendUint16([]byte{}, 0b0000000000000111),
		},
	}

	var err error
	expTables := make([]*Table, expNumTabs)
	expTables[0], err = Build(keySetSuffixes[0])
	if err != nil {
		t.Fatal(err)
	}

	expTables[1], err = Build(keySetSuffixes[1])
	if err != nil {
		t.Fatal(err)
	}

	expTables[2], err = Build(keySetSuffixes[2])
	if err != nil {
		t.Fatal(err)
	}

	expTables[3], err = Build(keySetSuffixes[3])
	if err != nil {
		t.Fatal(err)
	}

	st, err := BuildSharded(keys, prefBits, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	const shardedDumpPath = "/tmp/sharded.mph"
	err = st.DumpToFile(shardedDumpPath)
	if err != nil {
		t.Fatal(err)
	}

	st, err = LoadShardedTableFromFile(shardedDumpPath)
	if err != nil {
		t.Fatal(err)
	}

	numShards := len(st.tables)
	if numShards != expNumShards {
		t.Errorf("expected %d slots, got %d", expNumShards, numShards)
	}
	var tables []*Table
	for _, table := range st.tables {
		if table != nil {
			tables = append(tables, table)
		}
	}
	if len(tables) != expNumTabs {
		t.Errorf("expected %d tables, got %d", expNumTabs, len(tables))
	}

	tabFilePath := filepath.Join(os.TempDir(), "tab.mph")
	defer os.Remove(tabFilePath)
	expFilePath := filepath.Join(os.TempDir(), "exp.mph")
	defer os.Remove(expFilePath)

	for i, tab := range tables {
		for _, key := range keySetSuffixes[i] {
			if _, ok := tab.Lookup(key); !ok {
				t.Errorf("Lookup(%s): got !ok; want ok", string(key))
			}
		}

		for j := 0; j < len(keySetSuffixes); j++ {
			if i == j {
				continue
			}
			for _, key := range keySetSuffixes[j] {
				if _, ok := tab.Lookup(key); ok {
					t.Errorf("Lookup(%s): got ok; want !ok", string(key))
				}
			}
		}

		err = tab.DumpToFile(tabFilePath)
		if err != nil {
			t.Fatal(err)
		}
		tabBts, err := os.ReadFile(tabFilePath)
		if err != nil {
			t.Fatal(err)
		}
		err = expTables[i].DumpToFile(expFilePath)
		if err != nil {
			t.Fatal(err)
		}
		expBts, err := os.ReadFile(expFilePath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(tabBts, expBts) {
			t.Errorf("table %d does not match", i)
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

	st, err := BuildSharded(keys, prefBits, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	const shardedDumpPath = "/tmp/sharded.mph"
	err = st.DumpToFile(shardedDumpPath)
	if err != nil {
		t.Fatal(err)
	}

	st, err = LoadShardedTableFromFile(shardedDumpPath)
	if err != nil {
		t.Fatal(err)
	}

	numShards := len(st.tables)
	if numShards != expNumShards {
		t.Errorf("expected %d slots, got %d", expNumShards, numShards)
	}
	var tables []*Table
	for _, table := range st.tables {
		if table != nil {
			tables = append(tables, table)
		}
	}
	if len(tables) != expNumTabs {
		t.Errorf("expected %d tables, got %d", expNumTabs, len(tables))
	}

	tabFilePath := filepath.Join(os.TempDir(), "tab.mph")
	defer os.Remove(tabFilePath)
	expFilePath := filepath.Join(os.TempDir(), "exp.mph")
	defer os.Remove(expFilePath)

	for i, tab := range tables {
		for _, key := range keySets[i] {
			if _, ok := tab.Lookup(key); !ok {
				t.Errorf("Lookup(%s): got !ok; want ok", string(key))
			}
		}

		for j := 0; j < len(keySets); j++ {
			if i == j {
				continue
			}
			for _, key := range keySets[j] {
				if _, ok := tab.Lookup(key); ok {
					t.Errorf("Lookup(%s): got ok; want !ok", string(key))
				}
			}
		}

		err = tab.DumpToFile(tabFilePath)
		if err != nil {
			t.Fatal(err)
		}
		tabBts, err := os.ReadFile(tabFilePath)
		if err != nil {
			t.Fatal(err)
		}
		err = expTables[i].DumpToFile(expFilePath)
		if err != nil {
			t.Fatal(err)
		}
		expBts, err := os.ReadFile(expFilePath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(tabBts, expBts) {
			t.Errorf("table %d does not match", i)
		}
	}
}
