package storage

import (
	"testing"
)

func TestMetadata_Save(t *testing.T) {
	data := NewMetadata(make([]byte, BTREE_PAGE_SIZE))
	data.Flushed = 10
	data.Root = 5

	buf := data.Save()

	data2 := NewMetadata(buf)

	if data.Root != data2.Root {
		t.Fatal("wrong root")
	}

	if data.Flushed != data2.Flushed {
		t.Fatal("wrong flushed")
	}
}
