package storage

import (
	"testing"
)

func TestFreeListEmptyList(t *testing.T) {
	storage := &MockStorage{
		testing: t,
		storage: map[uint64][]byte{}}
	list, err := NewFreeList(storage, NewMetadata(make([]byte, BTREE_PAGE_SIZE)))
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	_, found, err := list.PopHead()
	if found {
		t.Fatalf("should not found value")
	}
}

func TestFreeList(t *testing.T) {
	storage := &MockStorage{
		testing: t,
		storage: map[uint64][]byte{}}
	list, err := NewFreeList(storage, NewMetadata(make([]byte, BTREE_PAGE_SIZE)))
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	_, found, err := list.PopHead()
	if found {
		t.Fatalf("should not found value")
	}
	err = list.PushTail(10)
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	val, found, err := list.PopHead()
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	if !found {
		t.Fatalf("should found value")
	}
	if val != 10 {
		t.Fatalf("should return 10 but got: %d", val)
	}
	val, found, err = list.PopHead()
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	if found {
		t.Fatalf("should not found value")
	}

}

func TestFreeListPushTailAndPopHead(t *testing.T) {
	storage := &MockStorage{
		testing: t,
		storage: map[uint64][]byte{}}
	list, err := NewFreeList(storage, NewMetadata(make([]byte, BTREE_PAGE_SIZE)))
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	if list.metadata.HeadPage == 0 {
		t.Fatalf("should have set HeadPage")
	}

	err = list.PushTail(10)

	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	val, found, err := list.PopHead()

	if !found {
		t.Fatalf("should found value")
	}
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	if val != 10 {
		t.Fatalf("should return 10 but got: %d", val)
	}
}

func TestFreeListInsertUntilNewPage(t *testing.T) {
	storage := &MockStorage{
		testing: t,
		storage: map[uint64][]byte{}}
	list, err := NewFreeList(storage, NewMetadata(make([]byte, BTREE_PAGE_SIZE)))
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}
	for i := range FREE_LIST_CAP {
		err = list.PushTail(uint64(i))
		if err != nil {
			t.Fatalf("should not raised err: %v", err)
		}
	}

	if list.metadata.TailSeq != 0 {
		t.Fatalf("should have: %d get: %d", 0, list.metadata.TailSeq)
	}

	storage.DumpPages()
	err = list.PushTail(FREE_LIST_CAP)
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	for i := range FREE_LIST_CAP + 1 {
		val, found, err := list.PopHead()
		if !found {
			t.Fatalf("should found value")
		}
		if err != nil {
			t.Fatalf("should not raised err: %v", err)
		}
		if val != uint64(i) {
			t.Fatalf("should have: %d got: %d", i, val)

		}
	}
}
