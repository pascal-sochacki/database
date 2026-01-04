package core

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

func AssertRecord(t *testing.T, got, want Record) {
	t.Helper()
	for k, v := range want.values {
		if v.Type == TYPE_BYTES {
			gotVal, ok := got.GetStr(k)
			if !ok {
				t.Fatalf("missing column %s", k)
			}
			wantVal, _ := want.GetStr(k)
			if !bytes.Equal(gotVal, wantVal) {
				t.Fatalf("column %s: got %s, want %s", k, gotVal, wantVal)
			}
		} else if v.Type == TYPE_INT64 {
			gotVal, ok := got.GetInt64(k)
			if !ok {
				t.Fatalf("missing column %s", k)
			}
			wantVal, _ := want.GetInt64(k)
			if gotVal != wantVal {
				t.Fatalf("column %s: got %d, want %d", k, gotVal, wantVal)
			}
		}
	}
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

func TestScan(t *testing.T) {
	db := CreateTempDB(t)
	defer db.Close()

	stmt := []string{
		"CREATE TABLE tes ( pk bytes, val bytes, primary key (pk))",
		"CREATE TABLE test ( pk bytes, val bytes, primary key (pk))",
		"CREATE TABLE testt ( pk bytes, val bytes, primary key (pk))",

		"INSERT INTO tes (pk, val) VALUES ('p1', 'valuesxx')",
		"INSERT INTO test (pk, val) VALUES ('p1', 'values1')",
		"INSERT INTO test (pk, val) VALUES ('p2', 'values2')",
		"INSERT INTO testt (pk, val) VALUES ('p2', 'valuesx')",
	}
	for _, v := range stmt {
		_, err := db.Execute(v)
		if err != nil {
			t.Fatalf("should not err: %v when running: %s", err, v)
		}
	}

	v1 := NewRecord()
	v1.AddStr("val", []byte("values1"))
	v1.AddStr("pk", []byte("p1"))

	v2 := NewRecord()
	v2.AddStr("val", []byte("values2"))
	v2.AddStr("pk", []byte("p2"))

	want := []Record{v1, v2}

	records, err := db.Scan("test")
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("wrong size")
	}
	for i, v := range want {
		AssertRecord(t, records[i], v)

	}
}

func TestExecute(t *testing.T) {
	db := CreateTempDB(t)
	defer db.Close()
	_, err := db.Execute("CREATE TABLE test ( pk bytes, val bytes, primary key (pk))")
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}
	_, err = db.Execute("INSERT INTO test (pk, val) VALUES ('primary', 'values')")
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}
	result, err := db.Execute("SELECT * FROM test")
	if err != nil {
		t.Fatalf("should not err: %v", err)
	}

	t.SkipNow()

	if len(result.Columns) != 2 {
		t.Fatalf("wrong size")
	}

	if result.Columns[0] != "pk" {
		t.Fatalf("first col should be pk")
	}

}
