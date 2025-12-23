package database

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"strings"
	"testing"
)

// assertKV checks if a key-value pair at the given index matches expected values
func assertKV(t *testing.T, node BNode, idx uint16, expectedKey, expectedVal []byte) {
	t.Helper()
	currentKey, err := node.getKey(idx)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}
	if !bytes.Equal(currentKey, expectedKey) {
		t.Fatalf("key mismatch at index %d: got %s, want %s", idx, currentKey, expectedKey)
	}
	currentVal, err := node.getVal(idx)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}
	if !bytes.Equal(currentVal, expectedVal) {
		t.Fatalf("value mismatch at index %d: got %s, want %s", idx, currentVal, expectedVal)
	}
}

// TestTypeConstants verifies that Type enum constants are correctly defined
func TestWriteAndRead(t *testing.T) {
	node := make(BNode, BTREE_PAGE_SIZE)
	node.setHeader(BNODE_LEAF, 2)
	node.AppendKV(0, 0, []byte("k1"), []byte("hi"))
	node.AppendKV(1, 0, []byte("k3"), []byte("hello"))

	// Hexdump output
	t.Logf("Node hexdump:\n%s", hex.Dump(node))

	assertKV(t, node, 0, []byte("k1"), []byte("hi"))
	assertKV(t, node, 1, []byte("k3"), []byte("hello"))
	bytes, err := node.usedBytes()
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	if bytes != 43 {
		t.Fatalf("wrong size is: %d", bytes)
	}
}

func TestInsertValue(t *testing.T) {
	old := make(BNode, BTREE_PAGE_SIZE)
	old.setHeader(BNODE_LEAF, 2)
	old.AppendKV(0, 0, []byte("k1"), []byte("hi"))
	old.AppendKV(1, 0, []byte("k3"), []byte("hello"))

	new := old.InsertValue(1, []byte("k2"), []byte("world"))

	assertKV(t, new, 0, []byte("k1"), []byte("hi"))
	assertKV(t, new, 1, []byte("k2"), []byte("world"))
	assertKV(t, new, 2, []byte("k3"), []byte("hello"))
}

func TestUpdateValue(t *testing.T) {
	old := make(BNode, BTREE_PAGE_SIZE)
	old.setHeader(BNODE_LEAF, 3)
	old.AppendKV(0, 0, []byte("k1"), []byte("hi"))
	old.AppendKV(1, 0, []byte("k2"), []byte("world"))
	old.AppendKV(2, 0, []byte("k3"), []byte("hello"))

	new := old.UpdateValue(1, []byte("k2"), []byte("Erde"))

	assertKV(t, new, 0, []byte("k1"), []byte("hi"))
	assertKV(t, new, 1, []byte("k2"), []byte("Erde"))
	assertKV(t, new, 2, []byte("k3"), []byte("hello"))
}

func TestLookupLE(t *testing.T) {
	node := make(BNode, BTREE_PAGE_SIZE)
	node.setHeader(BNODE_LEAF, 3)
	node.AppendKV(0, 0, []byte("k1"), []byte("hi"))
	node.AppendKV(1, 0, []byte("k3"), []byte("world"))

	idx, err := node.LookupLE([]byte("k2"))
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	if idx != 0 {
		t.Fatalf("wrong idx is: %d", idx)
	}
}

func TestSplit(t *testing.T) {
	node := make(BNode, BTREE_PAGE_SIZE)
	node.setHeader(BNODE_LEAF, 2)
	node.AppendKV(0, 0, []byte("k1"), []byte("hi"))
	node.AppendKV(1, 0, []byte("k3"), []byte("hello"))

	left, right := node.Split()

	assertKV(t, left, 0, []byte("k1"), []byte("hi"))
	assertKV(t, right, 0, []byte("k3"), []byte("hello"))
}

type MockStorage struct {
	storage map[uint64][]byte
	testing *testing.T
}

func (m *MockStorage) DumpPages() {
	for k, v := range m.storage {
		m.testing.Logf("Node hexdump (key: %d):\n%s", k, hex.Dump(v))
	}
}

// Delete implements Storage.
func (m *MockStorage) Delete(i uint64) {
	m.testing.Logf("deleting page: %d", i)
	delete(m.storage, i)
}

// Get implements Storage.
func (m *MockStorage) Get(i uint64) []byte {
	return m.storage[i]
}

// New implements Storage.
func (m *MockStorage) New(d []byte) uint64 {
	if len(d) > BTREE_PAGE_SIZE {
		m.testing.Logf("Node hexdump:\n%s", hex.Dump(d))
		m.testing.Errorf("New() called with %d bytes, exceeds BTREE_PAGE_SIZE (%d)", len(d), BTREE_PAGE_SIZE)
	}
	idx := rand.Uint64()
	node := BNode(d)
	m.testing.Logf("creating page: %d type: %d", idx, node.nodeType())
	m.storage[idx] = d
	return idx
}

var _ Storage = &MockStorage{}

func TestInsertTree(t *testing.T) {
	tree := NewBTree(&MockStorage{
		testing: t,
		storage: map[uint64][]byte{}},
	)
	tree.Insert([]byte("hello"), []byte("world"))
	tree.Insert([]byte("hallo"), []byte("welt"))

	result, ok, err := tree.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	if !ok || !bytes.Equal(result, []byte("world")) {
		t.Fatalf("value mismatch got %s, want %s", result, "world")
	}

	result, ok, err = tree.Get([]byte("hallo"))
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	if !ok || !bytes.Equal(result, []byte("welt")) {
		t.Fatalf("value mismatch got %s, want %s", result, "welft")
	}

	result, ok, err = tree.Get([]byte("servus"))
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	if ok || bytes.Equal(result, []byte("welt")) {
		t.Fatalf("value mismatch got %s, want %s", result, "welft")
	}
}

func TestUpdateTree(t *testing.T) {
	tree := NewBTree(&MockStorage{
		testing: t,
		storage: map[uint64][]byte{}},
	)
	tree.Insert([]byte("hello"), []byte("world"))

	result, ok, err := tree.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	if !ok || !bytes.Equal(result, []byte("world")) {
		t.Fatalf("value mismatch got %s, want %s", result, "world")
	}

	tree.Insert([]byte("hello"), []byte("welt"))

	result, ok, err = tree.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	if !ok || !bytes.Equal(result, []byte("welt")) {
		t.Fatalf("value mismatch got %s, want %s", result, "welt")
	}
}

func TestInsertTooLargeKey(t *testing.T) {
	tree := NewBTree(&MockStorage{
		testing: t,
		storage: map[uint64][]byte{}},
	)
	err := tree.Insert([]byte(strings.Repeat("a", 1001)), []byte("world"))
	if err == nil {
		t.Fatal("should raised err")
	}
}

func TestInsertTooLargeValue(t *testing.T) {
	tree := NewBTree(&MockStorage{
		testing: t,
		storage: map[uint64][]byte{}},
	)
	err := tree.Insert([]byte("hello"), []byte(strings.Repeat("a", 3001)))
	if err == nil {
		t.Fatal("should raised err")
	}
}

func TestInsertTooForceSplit(t *testing.T) {
	storage := MockStorage{
		testing: t,
		storage: map[uint64][]byte{},
	}
	tree := NewBTree(&storage)

	aKey := []byte(strings.Repeat("ak", 500))
	aVal := []byte(strings.Repeat("av", 1500))
	err := tree.Insert(aKey, aVal)
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	bKey := []byte(strings.Repeat("bk", 500))
	bVal := []byte(strings.Repeat("bv", 1500))
	err = tree.Insert(bKey, bVal)
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	result, ok, err := tree.Get(aKey)
	if err != nil {
		storage.DumpPages()
		t.Fatalf("should not raised err: %v", err)
	}
	if !ok {
		storage.DumpPages()
		t.Fatalf("should get ok as result, but got ok=%v", ok)
	}
	if !bytes.Equal(result, aVal) {
		t.Fatal("should get ok as result")
	}
}

func TestInsertTooForceThreeWaySplit(t *testing.T) {
	aKey := []byte(strings.Repeat("ak", 200))
	aVal := []byte(strings.Repeat("av", 700))

	bKey := []byte(strings.Repeat("bk", 500))
	bVal := []byte(strings.Repeat("bv", 1500))

	cKey := []byte(strings.Repeat("ck", 200))
	cVal := []byte(strings.Repeat("cv", 700))

	storage := MockStorage{
		testing: t,
		storage: map[uint64][]byte{},
	}
	tree := NewBTree(&storage)

	err := tree.Insert(aKey, aVal)
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	err = tree.Insert(cKey, cVal)
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	if len(storage.storage) != 1 {
		t.Fatalf("should only have 1 page")
	}

	err = tree.Insert(bKey, bVal)
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	if len(storage.storage) != 4 {
		t.Fatalf("should have 4 page, has: %d", len(storage.storage))
	}
}

func TestInsertIfRootIsInternalNode(t *testing.T) {
	storage := MockStorage{
		testing: t,
		storage: map[uint64][]byte{},
	}
	tree := NewBTree(&storage)

	aKey := []byte(strings.Repeat("ak", 500))
	aVal := []byte(strings.Repeat("av", 1500))
	err := tree.Insert(aKey, aVal)
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	bKey := []byte(strings.Repeat("bk", 500))
	bVal := []byte(strings.Repeat("bv", 1500))
	err = tree.Insert(bKey, bVal)
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	cKey := []byte(strings.Repeat("ck", 500))
	cVal := []byte(strings.Repeat("cv", 1500))
	err = tree.Insert(cKey, cVal)
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	result, ok, err := tree.Get(cKey)
	if err != nil {
		storage.DumpPages()
		t.Fatalf("should not raised err: %v", err)
	}
	if !ok {
		storage.DumpPages()
		t.Fatalf("should get ok as result, but got ok=%v", ok)
	}
	if !bytes.Equal(result, cVal) {
		t.Fatal("should get ok as result")
	}

}
