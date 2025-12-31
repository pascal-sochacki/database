package storage

import "encoding/binary"

type FreeList struct {
	storage  Storage
	metadata *Metadata
}

const FREE_LIST_HEADER = 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

func SequenceToIndex(seq uint64) int {
	return int(seq % FREE_LIST_CAP)
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

func (node LNode) setNext(ptr uint64) {
	binary.LittleEndian.PutUint64(node[0:], ptr)
}

func (node LNode) getNext() uint64 {
	return binary.LittleEndian.Uint64(node[0:])
}

func (fl *FreeList) PopHead() (uint64, bool, error) {
	if fl.metadata.HeadPage == fl.metadata.TailPage {
		if fl.metadata.HeadSeq == fl.metadata.TailSeq {
			return 0, false, nil
		}
	}
	headPtr := fl.metadata.HeadPage
	headPage, err := fl.storage.Get(headPtr)
	if err != nil {
		return 0, false, err
	}
	node := LNode(headPage)
	result := node.getPtr(int(fl.metadata.HeadSeq))
	fl.metadata.HeadSeq = uint64(SequenceToIndex(fl.metadata.HeadSeq + 1))
	if fl.metadata.HeadSeq == 0 {
		fl.metadata.HeadPage = node.getNext()

	}

	return result, true, nil
}

func (fl *FreeList) PushTail(ptr uint64) error {
	tailPtr := fl.metadata.TailPage
	tailPage, err := fl.storage.Get(tailPtr)
	if err != nil {
		return err
	}
	node := LNode(tailPage)
	node.setPtr(int(fl.metadata.TailSeq), ptr)
	fl.metadata.TailSeq = uint64(SequenceToIndex(fl.metadata.TailSeq + 1))

	if fl.metadata.TailSeq == 0 {
		nextPtr, err := fl.storage.New(make([]byte, BTREE_PAGE_SIZE))
		if err != nil {
			return err
		}
		node.setNext(nextPtr)
		fl.metadata.TailPage = nextPtr
	}

	return nil
}

type LNode []byte
