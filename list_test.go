package database

import (
	"testing"
)

func TestFreeListPushTailAndPopHead(t *testing.T) {
	storage := &MockStorage{
		testing: t,
		storage: map[uint64][]byte{}}
	list := NewFreeList(storage)

	err := list.PushTail(10)

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
