package database

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

const (
	DB_SIG          = "BuildYourOwnDB"
	META_SIZE       = 32
	INITIAL_MMAP_MB = 1 // 1MB initial chunk
)

type MMapStorage struct {
	Path  string
	Fsync func(int) error // overridable for testing

	// File and mmap
	file *os.File
	fd   int
	tree BTree

	mmap struct {
		total  int      // total mmap'd bytes
		chunks [][]byte // mmap chunks
	}

	page struct {
		flushed uint64   // pages persisted to disk
		temp    [][]byte // new pages in memory
	}

	failed bool // crash recovery flag
}

func (db *MMapStorage) loadMeta(data []byte) {
	db.tree.Root = binary.LittleEndian.Uint64(data[16:24])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:32])
}

func (db *MMapStorage) saveMeta() []byte {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:24], db.tree.Root)
	binary.LittleEndian.PutUint64(data[24:32], db.page.flushed)
	return data[:]
}

// Delete implements Storage.
func (db *MMapStorage) Delete(ptr uint64) {
	// Pages are freed automatically by copy-on-write
	// For now, no-op - we can implement free list later
}

// Get implements Storage.
func (db *MMapStorage) Get(ptr uint64) []byte {
	// Check temp pages first
	if ptr >= db.page.flushed {
		idx := ptr - db.page.flushed
		if int(idx) < len(db.page.temp) {
			return db.page.temp[idx]
		}
		panic("bad ptr")
	}

	// Check mmap pages
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+BTREE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}

// New implements Storage.
func (db *MMapStorage) New(node []byte) uint64 {
	if len(node) != BTREE_PAGE_SIZE {
		panic("invalid page size")
	}
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node)
	return ptr
}

func (db *MMapStorage) flushPages() error {
	if len(db.page.temp) == 0 {
		return nil
	}

	// Calculate file offset
	offset := int64(db.page.flushed * BTREE_PAGE_SIZE)

	// Write all temp pages
	for _, page := range db.page.temp {
		if _, err := unix.Pwrite(db.fd, page, offset); err != nil {
			return fmt.Errorf("pwrite page: %w", err)
		}
		offset += int64(BTREE_PAGE_SIZE)
	}

	// Fsync file
	if err := db.Fsync(db.fd); err != nil {
		return fmt.Errorf("fsync pages: %w", err)
	}

	// Update flushed count
	db.page.flushed += uint64(len(db.page.temp))

	// Extend mmap if needed
	newSize := int64(db.page.flushed * BTREE_PAGE_SIZE)
	if int(newSize) > db.mmap.total {
		if err := db.extendMmap(int(newSize)); err != nil {
			return err
		}
	}

	// Clear temp pages
	db.page.temp = nil

	return nil
}

func (db *MMapStorage) Open() error {
	// Step 1: Setup callbacks
	if db.Fsync == nil {
		db.Fsync = unix.Fsync
	}

	// Step 2: Open/create file
	f, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	db.file = f
	db.fd = int(f.Fd())

	// Step 3: Get file size
	stat, err := f.Stat()
	if err != nil {
		db.Close()
		return fmt.Errorf("stat file: %w", err)
	}
	fileSize := stat.Size()

	// Step 4: Validate file size is multiple of BTREE_PAGE_SIZE
	if fileSize%BTREE_PAGE_SIZE != 0 {
		db.Close()
		return errors.New("file size not multiple of page size")
	}

	// Step 5: Create initial mmap
	if err := db.extendMmap(int(fileSize)); err != nil {
		db.Close()
		return fmt.Errorf("extend mmap: %w", err)
	}

	// Step 6: Handle empty file - create new database
	if fileSize == 0 {
		db.page.flushed = 1 // Meta page is page 0
		db.tree = NewBTree(db)
		if err := db.flushPages(); err != nil {
			db.Close()
			return err
		}
		if err := db.writeMetaPage(); err != nil {
			db.Close()
			return err
		}
		return nil
	}

	// Step 7: Load existing meta
	metaData := db.mmap.chunks[0][:META_SIZE]
	db.loadMeta(metaData)

	// Step 8: Validate meta page
	sig := string(metaData[:14])
	if sig != DB_SIG {
		db.Close()
		return errors.New("bad meta signature")
	}

	maxPages := uint64(fileSize / BTREE_PAGE_SIZE)
	if !(0 < db.page.flushed && db.page.flushed <= maxPages) {
		db.Close()
		return errors.New("bad flushed count")
	}

	if !(0 < db.tree.Root && db.tree.Root < db.page.flushed) {
		db.Close()
		return errors.New("bad root pointer")
	}

	// Step 9: Create BTree with loaded root
	db.tree = NewBTree(db)
	db.tree.Root = binary.LittleEndian.Uint64(metaData[16:24])

	// Step 10: Return success
	return nil
}

func (db *MMapStorage) writeMetaPage() error {
	metaBytes := db.saveMeta()
	_, err := unix.Pwrite(db.fd, metaBytes, 0)
	if err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	if err := db.Fsync(db.fd); err != nil {
		return fmt.Errorf("fsync meta page: %w", err)
	}
	return nil
}

func (db *MMapStorage) extendMmap(size int) error {
	if size <= db.mmap.total {
		return nil // enough range
	}

	// Start with 1MB, double as needed
	alloc := INITIAL_MMAP_MB << 20 // 1MB
	if db.mmap.total > 0 {
		alloc = db.mmap.total
	}

	for db.mmap.total+alloc < size {
		alloc *= 2
	}

	chunk, err := unix.Mmap(
		db.fd, int64(db.mmap.total), alloc,
		unix.PROT_READ, unix.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

func (db *MMapStorage) Sync() error {
	if err := db.flushPages(); err != nil {
		return err
	}
	if err := db.writeMetaPage(); err != nil {
		return err
	}
	return nil
}

func (db *MMapStorage) Close() error {
	for _, chunk := range db.mmap.chunks {
		if err := unix.Munmap(chunk); err != nil {
			return fmt.Errorf("munmap: %w", err)
		}
	}
	if db.file != nil {
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("close file: %w", err)
		}
	}
	return nil
}

var _ Storage = (*MMapStorage)(nil)
