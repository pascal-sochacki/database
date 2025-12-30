package database

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func TestJsonMarshal(t *testing.T) {

	val, err := json.Marshal(TableDef{
		Prefix: 1,
		Name:   "@meta",
		Columns: []Column{
			{
				Name: "key",
				Type: TYPE_BYTES,
			},
			{
				Name: "val",
				Type: TYPE_BYTES,
			},
		},
		PKeys: 1,
	})

	if err != nil {
		t.Fatalf("should not err")
	}

	result := TableDef{}
	err = json.Unmarshal(val, &result)
	if err != nil {
		t.Fatalf("should not err")
	}
}

func CreateTempDB(t *testing.T) *DB {
	t.Helper()
	tempDir := t.TempDir()
	file := filepath.Join(tempDir, "test.db")

	db, err := NewDB(file)
	if err != nil {
		t.Fatalf("should not err")
	}
	return db
}

func TestCreateTableAndInsert(t *testing.T) {
	db := CreateTempDB(t)
	defer db.Close()

	table := NewTableDef("test", []Column{
		{
			Name: "pk",
			Type: TYPE_BYTES,
		},
	}, []Column{
		{
			Name: "key",
			Type: TYPE_BYTES,
		},
	})

	err := db.CreateTable(&table)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}
	rec := NewRecord()
	rec.AddStr("pk", []byte("key"))
	rec.AddStr("key", []byte("value"))

	err = db.Insert("test", rec)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}

	query := NewRecord()
	query.AddStr("pk", []byte("key"))
	err = db.Get("test", &query)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}

	value, ok := query.GetStr("key")
	if !ok {
		t.Fatalf("should have key")
	}
	expect := []byte("value")

	if !bytes.Equal(value, expect) {
		t.Fatalf("value dont match got: %s, wanted: %s", value, expect)
	}

}

func TestUpdate(t *testing.T) {
	db := CreateTempDB(t)
	defer db.Close()

	table := NewTableDef("test", []Column{
		{
			Name: "pk",
			Type: TYPE_BYTES,
		},
	}, []Column{
		{
			Name: "key",
			Type: TYPE_BYTES,
		},
	})

	err := db.CreateTable(&table)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}
	rec := NewRecord()
	rec.AddStr("pk", []byte("key"))
	rec.AddStr("key", []byte("value"))

	err = db.Insert("test", rec)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}

	query := NewRecord()
	query.AddStr("pk", []byte("key"))
	err = db.Get("test", &query)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}

	value, ok := query.GetStr("key")
	if !ok {
		t.Fatalf("should have key")
	}
	expect := []byte("value")

	if !bytes.Equal(value, expect) {
		t.Fatalf("value dont match got: %s, wanted: %s", value, expect)
	}

	rec2 := NewRecord()
	rec2.AddStr("pk", []byte("key"))
	rec2.AddStr("key", []byte("updated-value"))

	err = db.Update("test", rec2)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}

	err = db.Get("test", &query)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}

	value, ok = query.GetStr("key")
	if !ok {
		t.Fatalf("should have key")
	}
	expect = []byte("updated-value")

	if !bytes.Equal(value, expect) {
		t.Fatalf("value dont match got: %s, wanted: %s", value, expect)
	}
}

func TestDelete(t *testing.T) {
	db := CreateTempDB(t)
	defer db.Close()

	table := NewTableDef("test", []Column{
		{
			Name: "pk",
			Type: TYPE_BYTES,
		},
	}, []Column{
		{
			Name: "key",
			Type: TYPE_BYTES,
		},
	})

	err := db.CreateTable(&table)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}
	rec := NewRecord()
	rec.AddStr("pk", []byte("key"))
	rec.AddStr("key", []byte("value"))

	err = db.Insert("test", rec)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}

	query := NewRecord()
	query.AddStr("pk", []byte("key"))
	err = db.Get("test", &query)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}

	value, ok := query.GetStr("key")
	if !ok {
		t.Fatalf("should have key")
	}
	expect := []byte("value")

	if !bytes.Equal(value, expect) {
		t.Fatalf("value dont match got: %s, wanted: %s", value, expect)
	}

	rec2 := NewRecord()
	rec2.AddStr("pk", []byte("key"))

	err = db.Delete("test", rec2)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}

	err = db.Get("test", &query)
	if !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("should err not found but got: %v", err)
	}
}

func TestExecute(t *testing.T) {
	db := CreateTempDB(t)
	defer db.Close()
	err := db.Execute("CREATE TABLE test ( pk bytes, val bytes, primary key (pk))")
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}
	rec := NewRecord()
	rec.AddStr("pk", []byte("key"))
	rec.AddStr("val", []byte("value"))

	err = db.Insert("test", rec)
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}
}
