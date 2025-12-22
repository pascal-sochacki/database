package database

import (
	"bytes"
	"encoding/binary"
)

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000
const HEADER = 4

func init() {
	node1max := 4 + 1*8 + 1*2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(node1max <= BTREE_PAGE_SIZE) // maximum KV
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

func (node BNode) getPtr(idx uint16) uint64 {
	assert(idx < node.nkeys())
	pos := 4 + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx <= node.nkeys())
	pos := 4 + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
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

func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())
	return 4 + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys()) // uses the offset value of the last key
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

func (node BNode) AppendKV(idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	node.setPtr(idx, ptr)
	// KVs
	pos := node.kvPos(idx) // uses the offset value of the previous key
	// 4-bytes KV sizes
	binary.LittleEndian.PutUint16(node[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node[pos+2:], uint16(len(val)))
	// KV data
	copy(node[pos+4:], key)
	copy(node[pos+4+uint16(len(key)):], val)
	// update the offset value for the next key
	node.setOffset(idx+1, node.getOffset(idx)+4+uint16((len(key)+len(val))))
}

func (old BNode) InsertValue(idx uint16, key []byte, val []byte) BNode {
	new := make(BNode, BTREE_PAGE_SIZE)
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

func (new BNode) AppendRange(old BNode, dstNew uint16, srcOld uint16, n uint16) {
	for i := range n {
		dst, src := dstNew+i, srcOld+i
		new.AppendKV(dst, old.getPtr(src), old.getKey(src), old.getVal(src))
	}
}

func (node BNode) LookupLE(key []byte) uint16 {
	nkeys := node.nkeys()
	var i uint16
	for i = 0; i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			return i
		}
		if cmp > 0 {
			return i - 1
		}
	}
	return i - 1
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

func assert(cond bool) {
	if !cond {
		panic("assertion failure")
	}
}
