package mph

import (
	"bufio"
	"crypto/sha1"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestBuild_simple(t *testing.T) {
	testTable(t, []string{"foo", "foo2", "bar", "baz"}, []string{"quux"})
}

func TestBuild_dup(t *testing.T) {
	// Test that duplicate keys are detected and return an error
	ks := make([][]byte, 2)
	ks[0] = []byte("foo")
	ks[1] = []byte("foo")

	_, err := Build(ks)
	if err == nil {
		t.Error("Build with duplicate keys: got nil error; want error")
	}
}

func TestBuild_stress(t *testing.T) {
	var keys, extra []string
	for i := 0; i < 20000; i++ {
		s := strconv.Itoa(i)
		if i < 10000 {
			keys = append(keys, s)
		} else {
			extra = append(extra, s)
		}
	}
	testTable(t, keys, extra)
}

func TestBuild(t *testing.T) {
	const numKeys = 10_000_000
	keys := make([][]byte, numKeys)
	hasher := sha1.New()
	for i := range keys {
		hasher.Write([]byte("key" + strconv.Itoa(i)))
		keys[i] = hasher.Sum(nil)
	}

	start := time.Now()
	tbl, err := Build(keys)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Build took %v sec", time.Since(start).Seconds())

	const testKey = "hello"
	if _, ok := tbl.Lookup([]byte(testKey)); ok {
		t.Errorf("Lookup(%s): got ok; want !ok", testKey)
	}

	for i := range keys {
		if _, ok := tbl.Lookup(keys[i]); !ok {
			t.Errorf("Lookup(%s): got !ok; want ok", string(keys[i]))
		}
	}

	dumpFilePath := "/tmp/test.mph"
	if err = tbl.DumpToFile(dumpFilePath); err != nil {
		t.Fatal(err)
	}

	tbl, err = LoadFromFile(dumpFilePath)
	if err != nil {
		t.Fatal(err)
	}

	for i := range keys {
		if _, ok := tbl.Lookup(keys[i]); !ok {
			t.Errorf("Lookup(%s): got !ok; want ok", string(keys[i]))
		}
	}
}

func TestBuildFromFile(t *testing.T) {
	const numKeys = 10_000_000
	hasher := sha1.New()
	keysFile, err := os.Create("/tmp/keys.bin")
	if err != nil {
		t.Fatal(err)
	}
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		hasher.Write([]byte("key" + strconv.Itoa(numKeys)))
		keys[i] = hasher.Sum(nil)
		_, err = keysFile.Write(keys[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	err = keysFile.Close()
	if err != nil {
		t.Fatal(err)
	}

	keysFile, err = os.Open("/tmp/keys.bin")
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	tbl, err := BuildFromFile(keysFile, sha1.Size)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("BuildFromFile took %v sec", time.Since(start).Seconds())

	const testKey = "hello"
	if _, ok := tbl.Lookup([]byte(testKey)); ok {
		t.Errorf("Lookup(%s): got ok; want !ok", testKey)
	}

	for i := range keys {
		if _, ok := tbl.Lookup(keys[i]); !ok {
			t.Errorf("Lookup(%s): got !ok; want ok", string(keys[i]))
		}
	}

	dumpFilePath := "/tmp/test.mph"
	if err = tbl.DumpToFile(dumpFilePath); err != nil {
		t.Fatal(err)
	}

	tbl, err = LoadFromFile(dumpFilePath)
	if err != nil {
		t.Fatal(err)
	}

	for i := range keys {
		if _, ok := tbl.Lookup(keys[i]); !ok {
			t.Errorf("Lookup(%s): got !ok; want ok", string(keys[i]))
		}
	}
}

func testTable(t *testing.T, keys []string, extra []string) {
	ks := make([][]byte, len(keys))
	for i, key := range keys {
		ks[i] = []byte(key)
	}
	table, err := Build(ks)
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range keys {
		n, ok := table.Lookup([]byte(key))
		if !ok {
			t.Errorf("Lookup(%s): got !ok; want ok", key)
			continue
		}
		if int(n) != i {
			t.Errorf("Lookup(%s): got n=%d; want %d", key, n, i)
		}
	}
	for _, key := range extra {
		if _, ok := table.Lookup([]byte(key)); ok {
			t.Errorf("Lookup(%s): got ok; want !ok", key)
		}
	}
}

var (
	words      [][]byte
	wordsOnce  sync.Once
	benchTable *Table
)

func BenchmarkBuild(b *testing.B) {
	wordsOnce.Do(loadBenchTable)
	if len(words) == 0 {
		b.Skip("unable to load dictionary file")
	}
	for i := 0; i < b.N; i++ {
		_, err := Build(words)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTable(b *testing.B) {
	wordsOnce.Do(loadBenchTable)
	if len(words) == 0 {
		b.Skip("unable to load dictionary file")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(words)
		n, ok := benchTable.Lookup(words[j])
		if !ok {
			b.Fatal("missing key")
		}
		if n != uint32(j) {
			b.Fatal("bad result index")
		}
	}
}

// For comparison against BenchmarkTable.
func BenchmarkTableMap(b *testing.B) {
	wordsOnce.Do(loadBenchTable)
	if len(words) == 0 {
		b.Skip("unable to load dictionary file")
	}
	m := make(map[string]uint32)
	for i, word := range words {
		m[string(word)] = uint32(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(words)
		n, ok := m[string(words[j])]
		if !ok {
			b.Fatal("missing key")
		}
		if n != uint32(j) {
			b.Fatal("bad result index")
		}
	}
}

func loadBenchTable() {
	for _, dict := range []string{"/usr/share/dict/words", "/usr/dict/words"} {
		var err error
		words, err = loadDict(dict)
		if err == nil {
			break
		}
	}
	if len(words) > 0 {
		var err error
		benchTable, err = Build(words)
		if err != nil {
			panic(err)
		}
	}
}

func loadDict(dict string) ([][]byte, error) {
	f, err := os.Open(dict)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var words [][]byte
	for scanner.Scan() {
		words = append(words, scanner.Bytes())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return words, nil
}
