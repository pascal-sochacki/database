package database

import "encoding/binary"

type FreeList struct {
	storage  Storage
	metadata *Metadata
}

func NewFreeList(storage Storage, metadata *Metadata) (FreeList, error) {
	head := LNode(make([]byte, BTREE_PAGE_SIZE))
	idx, err := storage.New(head)
	if err != nil {
		return FreeList{}, err

	}
	metadata.HeadPage = idx
	metadata.TailPage = idx
	return FreeList{
		storage:  storage,
		metadata: metadata,
	}, nil
}

func (node LNode) getPtr(idx int) uint64 {
	offset := FREE_LIST_HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[offset:])
}

func (node LNode) setPtr(idx int, ptr uint64) {
	offset := FREE_LIST_HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[offset:], ptr)
}

func (fl *FreeList) PopHead() (uint64, error) {
	headPtr := fl.metadata.HeadPage
	headPage, err := fl.storage.Get(headPtr)
	if err != nil {
		return 0, err
	}
	node := LNode(headPage)
	result := node.getPtr(int(fl.metadata.HeadSeq))
	fl.metadata.HeadSeq += 1

	return result, nil
}

func (fl *FreeList) PushTail(ptr uint64) error {
	tailPtr := fl.metadata.TailPage
	tailPage, err := fl.storage.Get(tailPtr)
	if err != nil {
		return err
	}
	node := LNode(tailPage)
	node.setPtr(int(fl.metadata.TailSeq), ptr)
	fl.metadata.TailSeq += 1

	return nil
}

type LNode []byte

const FREE_LIST_HEADER = 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8
