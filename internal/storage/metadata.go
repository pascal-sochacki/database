package storage

import "encoding/binary"

type Metadata struct {
	Root    uint64
	Flushed uint64

	HeadPage uint64 // pointer to the list head node
	HeadSeq  uint64 // monotonic sequence number to index into the list head
	TailPage uint64
	TailSeq  uint64
}

func NewMetadata(d []byte) *Metadata {
	metadata := &Metadata{
		Root:    binary.LittleEndian.Uint64(d[16:24]),
		Flushed: binary.LittleEndian.Uint64(d[24:32]),

		HeadPage: binary.LittleEndian.Uint64(d[32:40]),
		HeadSeq:  binary.LittleEndian.Uint64(d[40:48]),

		TailPage: binary.LittleEndian.Uint64(d[48:56]),
		TailSeq:  binary.LittleEndian.Uint64(d[56:64]),
	}
	return metadata
}

func (data Metadata) Save() []byte {
	var d [4096]byte

	copy(d[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(d[16:24], data.Root)
	binary.LittleEndian.PutUint64(d[24:32], data.Flushed)
	binary.LittleEndian.PutUint64(d[32:40], data.HeadPage)
	binary.LittleEndian.PutUint64(d[40:48], data.HeadSeq)

	binary.LittleEndian.PutUint64(d[48:56], data.TailPage)
	binary.LittleEndian.PutUint64(d[56:64], data.TailSeq)
	return d[:]

}
