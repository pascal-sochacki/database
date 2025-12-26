package database

import (
	"os"
	"path/filepath"
	"testing"
)

// TestKVBasic demonstrates the basic setup for integration testing
func TestKVBasic(t *testing.T) {
	// Create a temporary directory for the test database
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Open a new database
	db := &MMapStorage{Path: dbPath}
	if err := db.Open(); err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Insert a key-value pair using the BTree
	err := db.tree.Insert([]byte("hello"), []byte("world"))
	if err != nil {
		t.Fatalf("failed to insert key: %v", err)
	}

	// Retrieve the value
	val, ok, err := db.tree.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}
	if !ok {
		t.Fatal("key not found")
	}
	if string(val) != "world" {
		t.Fatalf("value mismatch: got %s, want world", string(val))
	}

	// Retrieve a non-existent key
	_, ok, err = db.tree.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("failed to get non-existent key: %v", err)
	}
	if ok {
		t.Fatal("non-existent key should not be found")
	}

	t.Log("basic test passed")
}

// TestKVPersistence demonstrates persistence across Open/Close
func TestKVPersistence(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db1, err := newKV(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db1.Insert([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("failed to insert key1: %v", err)
	}

	if err := db1.Insert([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("failed to insert key2: %v", err)
	}

	if err := db1.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	db2, err := newKV(dbPath)
	// Phase 2: Reopen and verify data
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Verify key1
	val, ok, err := db2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("failed to get key1: %v", err)
	}
	if !ok || string(val) != "value1" {
		t.Fatalf("key1 mismatch: got %s, ok=%v", string(val), ok)
	}

	// Verify key2
	val, ok, err = db2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("failed to get key2: %v", err)
	}
	if !ok || string(val) != "value2" {
		t.Fatalf("key2 mismatch: got %s, ok=%v", string(val), ok)
	}

	t.Log("persistence test passed")
}

// TestKVDelete demonstrates deletion operations
func TestKVDelete(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db := &MMapStorage{Path: dbPath}
	if err := db.Open(); err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Insert a key
	if err := db.tree.Insert([]byte("delete-me"), []byte("value")); err != nil {
		t.Fatalf("failed to insert key: %v", err)
	}

	// Verify it exists
	val, ok, err := db.tree.Get([]byte("delete-me"))
	if err != nil || !ok || string(val) != "value" {
		t.Fatalf("key not found before delete: %v, ok=%v", err, ok)
	}

	// Delete the key
	if err := db.tree.Delete([]byte("delete-me")); err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	// Verify it's gone
	val, ok, err = db.tree.Get([]byte("delete-me"))
	if err != nil {
		t.Fatalf("error getting deleted key: %v", err)
	}
	if ok {
		t.Fatalf("deleted key should not exist, got value: %s", string(val))
	}

	t.Log("delete test passed")
}

// TestKVMmapChunks demonstrates detecting mmap chunk allocation
func TestKVMmapChunks(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db := &MMapStorage{Path: dbPath}
	if err := db.Open(); err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Check initial mmap chunks
	initialChunks := len(db.mmap.chunks)
	t.Logf("Initial mmap chunks: %d", initialChunks)

	// Insert a key-value pair
	if err := db.tree.Insert([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("failed to insert key: %v", err)
	}

	// Check mmap chunks after insert
	afterInsertChunks := len(db.mmap.chunks)
	t.Logf("Mmap chunks after insert: %d", afterInsertChunks)

	// For now, we should have at least the initial chunk
	if initialChunks < 1 {
		t.Error("should have at least 1 mmap chunk initially")
	}

	t.Log("mmap chunks test passed")
}

// TestKVMetaPage demonstrates meta page validation
func TestKVMetaPage(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db := &MMapStorage{Path: dbPath}
	if err := db.Open(); err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Check initial meta values
	if db.Metadata.Root == 0 {
		t.Log("Root pointer is 0 (no data yet)")
	} else {
		t.Logf("Root pointer: %d", db.Metadata.Root)
	}

	t.Logf("Flushed pages: %d", db.Metadata.Flushed)

	// Insert data to update meta
	if err := db.tree.Insert([]byte("test"), []byte("data")); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Root should now point to a page
	if db.Metadata.Root == 0 {
		t.Error("root should not be 0 after insert")
	}

	t.Logf("Root pointer after insert: %d", db.Metadata.Root)

	t.Log("meta page test passed")
}

// TestKVBadFile demonstrates error handling for invalid files
func TestKVBadFile(t *testing.T) {
	tempDir := t.TempDir()

	// Test 1: Open nonexistent file (should create it)
	dbPath := filepath.Join(tempDir, "nonexistent.db")
	db := &MMapStorage{Path: dbPath}
	if err := db.Open(); err != nil {
		t.Fatalf("should create new file: %v", err)
	}
	db.Close()

	// Test 2: Corrupt the file with bad size
	dbPathBad := filepath.Join(tempDir, "badsize.db")
	if err := os.WriteFile(dbPathBad, []byte("not a multiple of page size"), 0o644); err != nil {
		t.Fatalf("failed to create bad file: %v", err)
	}

	dbBad := &MMapStorage{Path: dbPathBad}
	err := dbBad.Open()
	if err == nil {
		dbBad.Close()
		t.Fatal("should reject file with bad size")
	}
	t.Logf("Expected error for bad file size: %v", err)

	t.Log("bad file test passed")
}
