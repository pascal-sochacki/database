package core

import (
	"encoding/binary"
	"fmt"
)

type DataType uint32

const (
	TYPE_BYTES DataType = 1 // string (arbitrary bytes)
	TYPE_INT64 DataType = 2 // integer (64-bit signed)
)

type Value struct {
	Type DataType
	I64  int64
	Str  []byte
}

func (v Value) encode() []byte {
	b := []byte{}
	b = binary.LittleEndian.AppendUint32(b, uint32(v.Type))
	switch v.Type {
	case TYPE_INT64:
		b = binary.LittleEndian.AppendUint64(b, uint64(v.I64))
	case TYPE_BYTES:
		b = binary.LittleEndian.AppendUint16(b, uint16(len(v.Str)))
		b = append(b, v.Str...)
	}
	return b
}

// Value constructors
func NewInt64(val int64) Value {
	return Value{
		Type: TYPE_INT64,
		I64:  val,
	}
}

func NewBytes(val []byte) Value {
	return Value{
		Type: TYPE_BYTES,
		Str:  val,
	}
}

type Column struct {
	Name string
	Type DataType
}

type Record struct {
	values map[string]Value
}

// Record methods
func NewRecord() Record {
	return Record{
		values: map[string]Value{},
	}
}

func (r *Record) Add(col string, val Value) {
	r.values[col] = val
}

func (r *Record) AddStr(col string, val []byte) {
	r.values[col] = NewBytes(val)
}

func (r *Record) AddInt64(col string, val int64) {
	r.values[col] = NewInt64(val)
}

func (r *Record) Get(col string) (Value, bool) {
	v, ok := r.values[col]
	return v, ok
}

func (r *Record) GetStr(col string) ([]byte, bool) {
	v, ok := r.values[col]
	return v.Str, ok
}

func (r *Record) GetInt64(col string) (int64, bool) {
	v, ok := r.values[col]
	return v.I64, ok
}

type ResultSet struct {
	Columns []string   // ["pk", "val"]
	Rows    [][]string // [["primary", "values"]]
}

type TableDef struct {
	Name    string
	Columns []Column
	PKeys   int    // first N columns are primary key
	Prefix  uint32 // auto-assigned for key prefixing
}

func NewTableDef(name string, pkeys []Column, keys []Column) TableDef {
	return TableDef{
		Name:    name,
		Columns: append(pkeys, keys...),
		PKeys:   len(pkeys),
	}

}

func (t *TableDef) GetColumn(name string) (Column, bool) {
	for _, v := range t.Columns {
		if v.Name == name {
			return v, true
		}
	}
	return Column{}, false
}

func (t *TableDef) GetColumnIndex(name string) int {
	for i, v := range t.Columns {
		if v.Name == name {
			return i
		}
	}
	return 0
}

func (t *TableDef) GetPrimaryKeys() []Column {
	return t.Columns[:t.PKeys]
}

func (t *TableDef) GetNonPrimaryKeys() []Column {
	return t.Columns[t.PKeys:]
}

func (t *TableDef) EncodeKey(record Record) ([]byte, error) {
	b := []byte{}
	b = binary.LittleEndian.AppendUint32(b, t.Prefix)
	for _, v := range t.GetPrimaryKeys() {
		val, ok := record.Get(v.Name)
		if !ok {
			return nil, fmt.Errorf("missing pk key: %s", v.Name)
		}
		b = append(b, val.encode()...)

	}
	return b, nil

}

func (t *TableDef) EncodeValue(record Record) ([]byte, error) {
	b := []byte{}
	for _, v := range t.GetNonPrimaryKeys() {
		val, ok := record.Get(v.Name)
		if !ok {
			return nil, fmt.Errorf("missing column: %s", v.Name)
		}
		b = append(b, val.encode()...)

	}
	return b, nil
}

func (T *TableDef) DecodeValues(b []byte) ([]Value, error) {
	result := []Value{}
	for {
		if len(b) < 4 {
			break
		}
		valType := DataType(binary.LittleEndian.Uint32(b[:4]))
		b = b[4:] // Move pointer forward
		switch valType {
		case TYPE_BYTES:
			length := binary.LittleEndian.Uint16(b[:2])
			b = b[2:] // Move pointer forward
			result = append(result, NewBytes(b[:length]))
			b = b[length:] // Move pointer forward

		case TYPE_INT64:
			result = append(result, NewInt64(int64(binary.LittleEndian.Uint64(b[:8]))))
			b = b[8:] // Move pointer forward

		}

	}
	return result, nil
}
