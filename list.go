package database

type FreeList struct {
	storage Storage
}

func NewFreeList(storage Storage) *FreeList {
	return &FreeList{
		storage: storage,
	}
}

func (fl *FreeList) PopHead() (uint64, error) {
	return 0, nil
}

func (fl *FreeList) PushTail(ptr uint64) error {
	return nil
}

type LNode []byte

const FREE_LIST_HEADER = 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8
