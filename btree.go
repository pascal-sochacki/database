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

// Node format
// | type | nkeys | pointers   | offsets    | key-values | unused |
// | 2B   | 2B    | nkeys × 8B | nkeys × 2B | ...        |        |

// Key Values format
// | key_size | val_size | key | val |
// | 2B       | 2B       | ... | ... |

func init() {
	node1max := HEADER + 1*8 + 1*2 + 2 + BTREE_MAX_KEY_SIZE + 2 + BTREE_MAX_VAL_SIZE
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
	Root    uint64
	storage Storage
}

func NewBTree(storage Storage) BTree {
	root := BNode(make([]byte, BTREE_PAGE_SIZE))
	root.setHeader(BNODE_LEAF, 0)
	idx := storage.New(root)

	return BTree{
		Root:    idx,
		storage: storage,
	}
}

func (tree *BTree) Get(key []byte) ([]byte, bool, error) {
	if tree.Root == 0 {
		return nil, false, nil
	}

	root := BNode(tree.storage.Get(tree.Root))
	current := root
	for {
		if current.Type() == BNODE_LEAF {
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

	current := BNode(t.storage.Get(t.Root))

	ctx := &insertContext{storage: t.storage, toDelete: []uint64{}}
	new, err := current.Insert(key, val, ctx)
	if err != nil {
		return err
	}

	old := t.Root
	t.Root = t.storage.New(new)

	// Only delete old pages after root is safely updated
	ctx.toDelete = append(ctx.toDelete, old)
	ctx.CommitDeletions()

	return nil
}

func (t *BTree) Delete(key []byte) error {

	current := BNode(t.storage.Get(t.Root))
	ctx := &insertContext{storage: t.storage, toDelete: []uint64{}}
	new, err := current.Delete(key, ctx)
	if err != nil {
		return err
	}

	old := t.Root
	t.Root = t.storage.New(new)

	// Only delete old pages after root is safely updated
	ctx.toDelete = append(ctx.toDelete, old)
	ctx.CommitDeletions()
	return nil
}

type insertContext struct {
	storage  Storage
	toDelete []uint64
}

func (ctx *insertContext) Get(ptr uint64) []byte {
	return ctx.storage.Get(ptr)
}

func (ctx *insertContext) New(data []byte) uint64 {
	return ctx.storage.New(data)
}

func (ctx *insertContext) Delete(ptr uint64) {
	// Don't delete immediately - add to journal
	ctx.toDelete = append(ctx.toDelete, ptr)
}

func (ctx *insertContext) CommitDeletions() {
	for _, ptr := range ctx.toDelete {
		ctx.storage.Delete(ptr)
	}
}

type BNode []byte

func (node BNode) Insert(key []byte, val []byte, storage Storage) (BNode, error) {
	if node.Type() == BNODE_NODE {
		return node.insertIntoInternal(key, val, storage)
	}

	if node.Type() == BNODE_LEAF {
		return node.insertIntoLeaf(key, val, storage)
	}

	return nil, fmt.Errorf("should not happen")
}

func (node BNode) Delete(key []byte, ctx *insertContext) (BNode, error) {
	if node.Type() == BNODE_LEAF {
		idx, ok, err := node.Lookup(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return node, nil
		}

		return node.DeleteValue(idx), nil

	}
	panic("unimplemented")
}

type Type uint16

var (
	BNODE_NODE Type = 1 // internal nodes without values
	BNODE_LEAF Type = 2 // leaf nodes with values
)

func (node BNode) Type() Type {
	return Type(binary.LittleEndian.Uint16(node[0:2]))
}

func (node BNode) Keys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) getPtr(idx uint16) (uint64, error) {
	if idx >= node.Keys() {
		return 0, fmt.Errorf("out of bound")
	}
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:]), nil
}

func (node BNode) setPtr(idx uint16, val uint64) error {
	if idx >= node.Keys() {
		return fmt.Errorf("out of bound")
	}
	pos := HEADER + 8*idx
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
	pos := HEADER + 8*node.Keys() + 2*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
}

func (node BNode) keyValuePosition(idx uint16) (uint16, error) {
	if idx > node.Keys() {
		return 0, fmt.Errorf("out of bound")
	}
	return HEADER + 8*node.Keys() + 2*node.Keys() + node.getOffset(idx), nil
}

func (node BNode) getKey(idx uint16) ([]byte, error) {
	pos, err := node.keyValuePosition(idx)
	if err != nil {
		return []byte{}, err
	}
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+HEADER:][:klen], nil
}

func (node BNode) getVal(idx uint16) ([]byte, error) {
	pos, err := node.keyValuePosition(idx)
	if err != nil {
		return []byte{}, err
	}
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+HEADER+klen:][:vlen], nil
}

func offsetPos(node BNode, idx uint16) (uint16, error) {
	if 1 > idx || idx > node.Keys() {
		return 0, fmt.Errorf("out of bound")

	}
	return HEADER + 8*node.Keys() + 2*(idx-1), nil
}

func (node BNode) usedBytes() (uint16, error) {
	return node.keyValuePosition(node.Keys()) // uses the offset value of the last key
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
	pos, err := node.keyValuePosition(idx) // uses the offset value of the previous key
	if err != nil {
		return err
	}
	// 4-bytes KV sizes
	binary.LittleEndian.PutUint16(node[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node[pos+2:], uint16(len(val)))
	// KV data
	copy(node[pos+HEADER:], key)
	copy(node[pos+HEADER+uint16(len(key)):], val)
	// update the offset value for the next key
	node.setOffset(idx+1, node.getOffset(idx)+HEADER+uint16((len(key)+len(val))))
	return nil
}

func (old BNode) InsertValue(idx uint16, key []byte, val []byte) BNode {
	new := make(BNode, BTREE_PAGE_SIZE*2)
	new.setHeader(BNODE_LEAF, old.Keys()+1)
	new.AppendRange(old, 0, 0, idx)                  // copy the keys before `idx`
	new.AppendKV(idx, 0, key, val)                   // the new key
	new.AppendRange(old, idx+1, idx, old.Keys()-idx) // keys from `idx`
	return new
}

func (old BNode) UpdatePtr(idx uint16, ptr uint64) BNode {
	new := make(BNode, BTREE_PAGE_SIZE*2)
	new.setHeader(old.Type(), old.Keys())
	new.AppendRange(old, 0, 0, idx)                        // copy the keys before `idx`
	new.AppendKV(idx, ptr, []byte{}, []byte{})             // update the ptr at idx
	new.AppendRange(old, idx+1, idx+1, old.Keys()-(idx+1)) // copy keys after `idx`
	return new
}

func (old BNode) DeleteValue(idx uint16) BNode {
	new := make(BNode, BTREE_PAGE_SIZE)
	new.setHeader(BNODE_LEAF, old.Keys())
	new.AppendRange(old, 0, 0, idx)
	new.AppendRange(old, idx, idx+1, old.Keys()-(idx))
	return new
}

func (old BNode) UpdateValue(
	idx uint16, key []byte, val []byte,
) BNode {
	new := make(BNode, BTREE_PAGE_SIZE)
	new.setHeader(BNODE_LEAF, old.Keys())
	new.AppendRange(old, 0, 0, idx)
	new.AppendKV(idx, 0, key, val)
	new.AppendRange(old, idx+1, idx+1, old.Keys()-(idx+1))
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
	nkeys := node.Keys()
	var i uint16
	for i = range nkeys {
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
	nkeys := node.Keys()
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
	nleft := node.Keys() / 2
	nright := node.Keys() - nleft

	left, right := make(BNode, BTREE_PAGE_SIZE*2), make(BNode, BTREE_PAGE_SIZE*2)
	left.setHeader(BNODE_LEAF, nleft)
	right.setHeader(BNODE_LEAF, nright)

	left.AppendRange(node, 0, 0, nleft)
	right.AppendRange(node, 0, nleft, nright)

	return left, right
}

// splitIfNeeded splits a node if it exceeds page size and returns the result node
func (node BNode) splitIfNeeded(storage Storage) (BNode, error) {
	bytes, err := node.usedBytes()
	if err != nil {
		return nil, err
	}

	if bytes < BTREE_PAGE_SIZE {
		return node[:BTREE_PAGE_SIZE], nil
	}

	// Node is too large - need to split
	return node.splitLarge(storage)
}

// splitLarge splits an oversized node into multiple children and creates a parent
func (node BNode) splitLarge(storage Storage) (BNode, error) {
	nodes := []BNode{}
	left, right := node.Split()

	leftSize, err := left.usedBytes()
	if err != nil {
		return nil, err
	}
	rightSize, err := right.usedBytes()
	if err != nil {
		return nil, err
	}

	// Check if left child needs further splitting
	if leftSize > BTREE_PAGE_SIZE {
		l1, l2 := left.Split()
		nodes = append(nodes, l1[:BTREE_PAGE_SIZE])
		nodes = append(nodes, l2[:BTREE_PAGE_SIZE])
	} else {
		nodes = append(nodes, left[:BTREE_PAGE_SIZE])
	}

	// Check if right child needs further splitting
	if rightSize > BTREE_PAGE_SIZE {
		r1, r2 := right.Split()
		nodes = append(nodes, r1[:BTREE_PAGE_SIZE])
		nodes = append(nodes, r2[:BTREE_PAGE_SIZE])
	} else {
		nodes = append(nodes, right[:BTREE_PAGE_SIZE])
	}

	// Create parent node with all split children
	result := BNode(make([]byte, BTREE_PAGE_SIZE))
	result.setHeader(BNODE_NODE, uint16(len(nodes)))

	for i, v := range nodes {
		ptr := storage.New(v)
		key, err := v.getKey(0)
		if err != nil {
			return nil, err
		}
		result.AppendKV(uint16(i), ptr, key, nil)
	}

	if len(result) > BTREE_PAGE_SIZE {
		return nil, fmt.Errorf("size too large after split")
	}
	return result[:BTREE_PAGE_SIZE], nil
}

// insertIntoInternal handles insertion into an internal node
func (node BNode) insertIntoInternal(key []byte, val []byte, storage Storage) (BNode, error) {
	idx, err := node.LookupLE(key)
	if err != nil {
		return nil, err
	}

	ptr, err := node.getPtr(idx)
	if err != nil {
		return nil, err
	}

	child := BNode(storage.Get(ptr))
	newChild, err := child.Insert(key, val, storage)
	if err != nil {
		return nil, err
	}

	// Store new child and mark old for deletion
	newChildPtr := storage.New(newChild)
	storage.Delete(ptr)

	// Update the child pointer at this index
	new := node.UpdatePtr(idx, newChildPtr)

	// Split if needed
	return new.splitIfNeeded(storage)
}

// insertIntoLeaf handles insertion into a leaf node
func (node BNode) insertIntoLeaf(key []byte, val []byte, storage Storage) (BNode, error) {
	idx, ok, err := node.Lookup(key)
	if err != nil {
		return nil, err
	}

	var new BNode
	if ok {
		new = node.UpdateValue(idx, key, val)
	} else {
		new = node.InsertValue(idx, key, val)
	}

	// Split if needed
	return new.splitIfNeeded(storage)
}
