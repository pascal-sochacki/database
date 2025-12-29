package database

import (
	"bytes"
	"encoding/json"
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
func TestCreateTableAndInsert(t *testing.T) {
	tempDir := t.TempDir()
	file := filepath.Join(tempDir, "test.db")

	db, err := NewDB(file)
	if err != nil {
		t.Fatalf("should not err")
	}
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

	err = db.CreateTable(&table)
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
