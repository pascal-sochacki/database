package database

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"testing"
)

// assertKV checks if a key-value pair at the given index matches expected values
func assertKV(t *testing.T, node BNode, idx uint16, expectedKey, expectedVal []byte) {
	t.Helper()
	if !bytes.Equal(node.getKey(idx), expectedKey) {
		t.Fatalf("key mismatch at index %d: got %s, want %s", idx, node.getKey(idx), expectedKey)
	}
	if !bytes.Equal(node.getVal(idx), expectedVal) {
		t.Fatalf("value mismatch at index %d: got %s, want %s", idx, node.getVal(idx), expectedVal)
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
	if node.nbytes() != 43 {
		t.Fatalf("wrong size is: %d", node.nbytes())
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

	idx := node.LookupLE([]byte("k2"))

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
}

// Delete implements Storage.
func (m *MockStorage) Delete(i uint64) {
	delete(m.storage, i)
}

// Get implements Storage.
func (m *MockStorage) Get(i uint64) []byte {
	return m.storage[i]
}

// New implements Storage.
func (m *MockStorage) New(d []byte) uint64 {
	idx := rand.Uint64()
	m.storage[idx] = d
	return idx
}

var _ Storage = &MockStorage{}

func TestInsertTree(t *testing.T) {
	tree := NewBTree(&MockStorage{storage: map[uint64][]byte{}})
	tree.Insert([]byte("hello"), []byte("world"))

	result, ok := tree.Get([]byte("hello"))
	if ok && bytes.Equal(result, []byte("world")) {

	} else {
		t.Fatalf("value mismatch got %s, want %s", result, "world")

	}

}
