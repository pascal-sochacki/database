package database

import "encoding/binary"

type Metadata struct {
	Root    uint64
	Flushed uint64
}

func NewMetadata(data []byte) *Metadata {
	metadata := &Metadata{
		Root:    binary.LittleEndian.Uint64(data[16:24]),
		Flushed: binary.LittleEndian.Uint64(data[24:32]),
	}
	return metadata
}

func (data Metadata) Save() []byte {
	var d [32]byte

	copy(d[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(d[16:24], data.Root)
	binary.LittleEndian.PutUint64(d[24:32], data.Flushed)
	return d[:]

}
