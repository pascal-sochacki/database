package database

import (
	"testing"
)

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

	val, err := list.PopHead()
	if err != nil {
		t.Fatalf("should not raised err: %v", err)
	}

	if val != 10 {
		t.Fatalf("should return 10 but got: %d", val)
	}
}
