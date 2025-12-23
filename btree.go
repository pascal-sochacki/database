package database

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000
const HEADER = 4

func init() {
	node1max := 4 + 1*8 + 1*2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
		panic("assertion failure")
	}
}

type Storage interface {
	Get(uint64) []byte
	New([]byte) uint64
	Delete(uint64)
}

type BTree struct {
	root    uint64
	storage Storage
}

func NewBTree(storage Storage) BTree {
	root := BNode(make([]byte, BTREE_PAGE_SIZE))
	root.setHeader(BNODE_LEAF, 0)
	idx := storage.New(root)

	return BTree{
		root:    idx,
		storage: storage,
	}
}

func (tree *BTree) Get(key []byte) ([]byte, bool, error) {
	if tree.root == 0 {
		return nil, false, nil
	}

	root := BNode(tree.storage.Get(tree.root))

	current := root
	for {
		if current.btype() == BNODE_LEAF {
			idx, ok, err := current.Lookup(key)
			if err != nil {
				return nil, false, err
			}
			if ok {
				val, err := current.getVal(idx)
				return val, ok, err
			}
			return nil, false, nil
		}

		idx, err := current.LookupLE(key)
		if err != nil {
			return nil, false, err
		}

		ptr, err := current.getPtr(idx)
		if err != nil {
			return nil, false, err
		}

		current = BNode(tree.storage.Get(ptr))
	}

}

func (t *BTree) Insert(key []byte, val []byte) error {
	if len(key) > BTREE_MAX_KEY_SIZE {
		return fmt.Errorf("key to large")
	}
	if len(val) > BTREE_MAX_VAL_SIZE {
		return fmt.Errorf("value to large")
	}
	current := BNode(t.storage.Get(t.root))

	switch current.btype() {
	case BNODE_LEAF:

		idx, ok, err := current.Lookup(key)
		if err != nil {
			return err
		}

		var new BNode
		if ok {
			new = current.UpdateValue(idx, key, val)
		} else {
			new = current.InsertValue(idx, key, val)
		}

		bytes, _ := new.nbytes()
		if bytes < BTREE_PAGE_SIZE {
			new = new[:BTREE_PAGE_SIZE]
		} else {
			left, right := new.Split()
			if len(left) > BTREE_PAGE_SIZE || len(right) > BTREE_PAGE_SIZE {
				return fmt.Errorf("size too large after split")
			}
			new = BNode(make([]byte, BTREE_PAGE_SIZE))
			new.setHeader(BNODE_NODE, 2)

			leftPtr := t.storage.New(left)
			leftKey, err := left.getKey(0)
			if err != nil {
				return err
			}
			new.AppendKV(0, leftPtr, leftKey, nil)

			rightPtr := t.storage.New(right)
			rightKey, err := right.getKey(0)
			if err != nil {
				return err
			}
			new.AppendKV(1, rightPtr, rightKey, nil)

			if len(new) > BTREE_PAGE_SIZE {
				return fmt.Errorf("size too large after split in root")
			}
			new = new[:BTREE_PAGE_SIZE]
		}

		old := t.root
		t.root = t.storage.New(new)
		t.storage.Delete(old)
	case BNODE_NODE:
	}
	return nil
}

// | type | nkeys | pointers   | offsets    | key-values | unused |
// | 2B   | 2B    | nkeys × 8B | nkeys × 2B | ...        |        |
type BNode []byte

// | key_size | val_size | key | val |
// | 2B       | 2B       | ... | ... |

type Type uint16

var (
	BNODE_NODE Type = 1 // internal nodes without values
	BNODE_LEAF Type = 2 // leaf nodes with values
)

func (node BNode) btype() Type {
	return Type(binary.LittleEndian.Uint16(node[0:2]))
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) getPtr(idx uint16) (uint64, error) {
	if idx >= node.nkeys() {
		return 0, fmt.Errorf("out of bound")
	}
	pos := 4 + 8*idx
	return binary.LittleEndian.Uint64(node[pos:]), nil
}

func (node BNode) setPtr(idx uint16, val uint64) error {
	if idx >= node.nkeys() {
		return fmt.Errorf("out of bound")
	}
	pos := 4 + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
	return nil
}

func (node BNode) setHeader(btype Type, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], uint16(btype))
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := 4 + 8*node.nkeys() + 2*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
}

func (node BNode) kvPos(idx uint16) (uint16, error) {
	if idx > node.nkeys() {
		return 0, fmt.Errorf("out of bound")
	}
	return 4 + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx), nil
}

func (node BNode) getKey(idx uint16) ([]byte, error) {
	pos, err := node.kvPos(idx)
	if err != nil {
		return []byte{}, err
	}
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen], nil
}

func (node BNode) getVal(idx uint16) ([]byte, error) {
	pos, err := node.kvPos(idx)
	if err != nil {
		return []byte{}, err
	}
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen], nil
}

func offsetPos(node BNode, idx uint16) (uint16, error) {
	if 1 > idx || idx > node.nkeys() {
		return 0, fmt.Errorf("out of bound")

	}
	return HEADER + 8*node.nkeys() + 2*(idx-1), nil
}

func (node BNode) nbytes() (uint16, error) {
	return node.kvPos(node.nkeys()) // uses the offset value of the last key
}

func (node BNode) setOffset(idx uint16, offset uint16) error {
	pos, err := offsetPos(node, idx)
	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint16(node[pos:], offset)
	return nil
}

func (node BNode) AppendKV(idx uint16, ptr uint64, key []byte, val []byte) error {
	// ptrs
	node.setPtr(idx, ptr)
	// KVs
	pos, err := node.kvPos(idx) // uses the offset value of the previous key
	if err != nil {
		return err
	}
	// 4-bytes KV sizes
	binary.LittleEndian.PutUint16(node[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node[pos+2:], uint16(len(val)))
	// KV data
	copy(node[pos+4:], key)
	copy(node[pos+4+uint16(len(key)):], val)
	// update the offset value for the next key
	node.setOffset(idx+1, node.getOffset(idx)+4+uint16((len(key)+len(val))))
	return nil
}

func (old BNode) InsertValue(idx uint16, key []byte, val []byte) BNode {
	new := make(BNode, BTREE_PAGE_SIZE*2)
	new.setHeader(BNODE_LEAF, old.nkeys()+1)

	new.AppendRange(old, 0, 0, idx)                   // copy the keys before `idx`
	new.AppendKV(idx, 0, key, val)                    // the new key
	new.AppendRange(old, idx+1, idx, old.nkeys()-idx) // keys from `idx`
	return new
}

func (old BNode) UpdateValue(
	idx uint16, key []byte, val []byte,
) BNode {
	new := make(BNode, BTREE_PAGE_SIZE)
	new.setHeader(BNODE_LEAF, old.nkeys())
	new.AppendRange(old, 0, 0, idx)
	new.AppendKV(idx, 0, key, val)
	new.AppendRange(old, idx+1, idx+1, old.nkeys()-(idx+1))
	return new
}

func (new BNode) AppendRange(old BNode, dstNew uint16, srcOld uint16, n uint16) error {
	for i := range n {
		dst, src := dstNew+i, srcOld+i
		srcPtr, err := old.getPtr(src)
		if err != nil {
			return err
		}
		srcKey, err := old.getKey(src)
		if err != nil {
			return err
		}
		srcVal, err := old.getVal(src)
		if err != nil {
			return err
		}
		err = new.AppendKV(dst, srcPtr, srcKey, srcVal)
		if err != nil {
			return err
		}
	}
	return nil
}

func (node BNode) LookupLE(key []byte) (uint16, error) {
	nkeys := node.nkeys()
	var i uint16
	for i = 0; i < nkeys; i++ {
		currentKey, err := node.getKey(i)
		if err != nil {
			return 0, err
		}
		cmp := bytes.Compare(currentKey, key)
		if cmp == 0 {
			return i, nil
		}
		if cmp > 0 {
			// Key is smaller than current key
			if i == 0 {
				// Key is smaller than all keys, return first pointer (index 0)
				return 0, nil
			}
			return i - 1, nil
		}
	}
	// Key is larger than or equal to all keys, return last index
	return nkeys - 1, nil
}

func (node BNode) Lookup(key []byte) (uint16, bool, error) {
	nkeys := node.nkeys()
	var i uint16
	for i = range nkeys {
		currentKey, err := node.getKey(i)
		if err != nil {
			return 0, false, err
		}
		cmp := bytes.Compare(currentKey, key)
		if cmp == 0 {
			return i, true, nil
		}
		if cmp > 0 {
			// Key should be inserted before this key
			return i, false, nil
		}
	}
	// Key is larger than all existing keys, insert at the end
	return nkeys, false, nil
}

func (node BNode) Split() (BNode, BNode) {
	nleft := node.nkeys() / 2
	nright := node.nkeys() - nleft

	left, right := make(BNode, BTREE_PAGE_SIZE), make(BNode, BTREE_PAGE_SIZE)
	left.setHeader(BNODE_LEAF, nleft)
	right.setHeader(BNODE_LEAF, nright)

	left.AppendRange(node, 0, 0, nleft)
	right.AppendRange(node, 0, nleft, nright)

	return left, right
}
