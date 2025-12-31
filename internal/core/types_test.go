package core

import (
	"bytes"
	"testing"
)

func TestNewRecord(t *testing.T) {
	record := NewRecord()
	record.AddInt64("int", 1337)
	record.AddStr("str", []byte("string"))

	str, found := record.GetStr("str")
	if !found {
		t.Fatal("should found str but got false")
	}
	if !bytes.Equal(str, []byte("string")) {
		t.Fatalf("should get str got %s, should %s", string(str), "string")
	}

	int, found := record.GetInt64("int")
	if !found {
		t.Fatal("should found str but got false")
	}
	if int != 1337 {
		t.Fatalf("should get int got %d, should %d", int, 1337)
	}
}

func TestEncodeKeySinglePK(t *testing.T) {
	pkeys := Column{
		Name: "pk",
		Type: TYPE_BYTES,
	}
	other := Column{
		Name: "k",
		Type: TYPE_BYTES,
	}
	table := NewTableDef("table-name", []Column{pkeys}, []Column{other})
	table.Prefix = 1

	record := NewRecord()
	record.AddStr("pk", []byte("key"))

	key, err := table.EncodeKey(record)
	if err != nil {
		t.Fatalf("should not err")
	}

	expect := []byte{1, 0, 0, 0, 1, 0, 0, 0, 3, 0, 'k', 'e', 'y'}
	if !bytes.Equal(expect, key) {
		t.Fatalf("should get str got %+v, should %+v", key, expect)
	}
}

func TestEncodeKeyTwoPK(t *testing.T) {
	pkeysOne := Column{
		Name: "pk1",
		Type: TYPE_BYTES,
	}
	pkeysTwo := Column{
		Name: "pk2",
		Type: TYPE_INT64,
	}
	other := Column{
		Name: "k",
		Type: TYPE_INT64,
	}
	table := NewTableDef("table-name", []Column{pkeysOne, pkeysTwo}, []Column{other})
	table.Prefix = 1

	record := NewRecord()
	record.AddStr("pk1", []byte("one"))
	record.AddInt64("pk2", 100)

	key, err := table.EncodeKey(record)
	if err != nil {
		t.Fatalf("should not err")
	}

	expect := []byte{1, 0, 0, 0, 1, 0, 0, 0, 3, 0, 'o', 'n', 'e', 2, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0}
	if !bytes.Equal(expect, key) {
		t.Fatalf("should get str got %+v, should %+v", key, expect)
	}

}

func TestEncodeValue(t *testing.T) {
	pkeys := Column{
		Name: "pk",
		Type: TYPE_BYTES,
	}
	other := Column{
		Name: "k",
		Type: TYPE_BYTES,
	}
	table := NewTableDef("table-name", []Column{pkeys}, []Column{other})

	record := NewRecord()
	record.AddStr("pk", []byte("key"))
	record.AddStr("k", []byte("value"))

	key, err := table.EncodeValue(record)
	if err != nil {
		t.Fatalf("should not err")
	}
	expect := []byte{1, 0, 0, 0, 5, 0, 'v', 'a', 'l', 'u', 'e'}
	if !bytes.Equal(expect, key) {
		t.Fatalf("should get str got %+v, should %+v", key, expect)
	}
	values, err := table.DecodeValues(expect)
	if err != nil {
		t.Fatalf("should not err")
	}
	if values[0].Type != TYPE_BYTES {
		t.Fatalf("wrong type")
	}
	expect = []byte{'v', 'a', 'l', 'u', 'e'}
	if !bytes.Equal(values[0].Str, expect) {
		t.Fatalf("should get str got %+v, should %+v", values[0].Str, expect)
	}
}

func TestEncodeTwoValue(t *testing.T) {
	pkeys := Column{
		Name: "pk",
		Type: TYPE_BYTES,
	}
	keyOne := Column{
		Name: "k1",
		Type: TYPE_BYTES,
	}
	keyTwo := Column{
		Name: "k2",
		Type: TYPE_INT64,
	}
	table := NewTableDef("table-name", []Column{pkeys}, []Column{keyOne, keyTwo})
	table.Prefix = 1

	record := NewRecord()
	record.AddStr("k1", []byte("one"))
	record.AddInt64("k2", 100)

	key, err := table.EncodeValue(record)
	if err != nil {
		t.Fatalf("should not err")
	}
	expect := []byte{1, 0, 0, 0, 3, 0, 'o', 'n', 'e', 2, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0}
	if !bytes.Equal(expect, key) {
		t.Fatalf("should get str got %+v, should %+v", key, expect)
	}
	values, err := table.DecodeValues(expect)
	if err != nil {
		t.Fatalf("should not err")
	}
	if values[0].Type != TYPE_BYTES {
		t.Fatalf("wrong type")
	}
	expect = []byte{'o', 'n', 'e'}
	if !bytes.Equal(values[0].Str, expect) {
		t.Fatalf("should get str got %+v, should %+v", values[0].Str, expect)
	}

	if values[1].Type != TYPE_INT64 {
		t.Fatalf("wrong type")
	}
	if values[1].I64 != 100 {
		t.Fatalf("should get int got %+v, should %+v", values[1].I64, 100)
	}
}
