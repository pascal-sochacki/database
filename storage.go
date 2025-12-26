package database

import (
	"encoding/hex"
	"math/rand/v2"
	"testing"
)

type Storage interface {
	Get(uint64) ([]byte, error)
	New([]byte) (uint64, error)
	Delete(uint64) error
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
func (m *MockStorage) Delete(i uint64) error {
	m.testing.Logf("deleting page: %d", i)
	delete(m.storage, i)
	return nil
}

// Get implements Storage.
func (m *MockStorage) Get(i uint64) ([]byte, error) {
	return m.storage[i], nil
}

// New implements Storage.
func (m *MockStorage) New(d []byte) (uint64, error) {
	if len(d) > BTREE_PAGE_SIZE {
		m.testing.Logf("Node hexdump:\n%s", hex.Dump(d))
		m.testing.Errorf("New() called with %d bytes, exceeds BTREE_PAGE_SIZE (%d)", len(d), BTREE_PAGE_SIZE)
	}
	idx := rand.Uint64()
	node := BNode(d)
	m.testing.Logf("creating page: %d type: %d", idx, node.Type())
	m.storage[idx] = d
	return idx, nil
}

var _ Storage = &MockStorage{}
