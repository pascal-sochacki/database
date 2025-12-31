package core

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/pascal-sochacki/database/internal/engine"
	"github.com/pascal-sochacki/database/internal/storage"
)

type DB struct {
	Path string
	kv   storage.KV
}

var ErrRecordNotFound = errors.New("record not found")

var TDEF_META = &TableDef{
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
}

var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Columns: []Column{
		{
			Name: "name",
			Type: TYPE_BYTES,
		},
		{
			Name: "def",
			Type: TYPE_BYTES,
		},
	},
	PKeys: 1,
}

func NewDB(path string) (*DB, error) {
	kv, err := storage.NewKV(path)
	if err != nil {
		return nil, err
	}
	return &DB{
		Path: path,
		kv:   *kv,
	}, nil
}

func (db *DB) getTableDef(name string) (*TableDef, error) {
	if name == "@table" {
		return TDEF_TABLE, nil
	}
	if name == "@meta" {
		return TDEF_META, nil
	}

	query := NewRecord()
	query.AddStr("name", []byte(name))
	err := db.get(TDEF_TABLE, &query)

	if err != nil {
		return nil, errors.Join(err, fmt.Errorf("error while fetch table def"))
	}

	jsonDef, found := query.GetStr("def")
	if !found {
		return nil, fmt.Errorf("should not happen: %+v", query)
	}
	result := TableDef{}
	err = json.Unmarshal(jsonDef, &result)
	return &result, err
}

func (db *DB) get(tdef *TableDef, rec *Record) error {
	key, err := tdef.EncodeKey(*rec)
	if err != nil {
		return err
	}
	result, found, err := db.kv.Get(key)
	if err != nil {
		return err
	}
	if !found {
		return ErrRecordNotFound
	}
	values, err := tdef.DecodeValues(result)
	if err != nil {
		return err
	}
	for i, v := range values {
		rec.Add(tdef.Columns[i+tdef.PKeys].Name, v)
	}
	return nil
}

func (db *DB) insert(tdef *TableDef, rec *Record) error {
	key, err := tdef.EncodeKey(*rec)
	if err != nil {
		return err
	}
	val, err := tdef.EncodeValue(*rec)
	if err != nil {
		return err
	}
	err = db.kv.Insert(key, val)
	return err
}
func (db *DB) delete(tdef *TableDef, rec *Record) error {
	key, err := tdef.EncodeKey(*rec)
	if err != nil {
		return err
	}
	err = db.kv.Delete(key)
	return err
}

func (db *DB) Get(table string, rec *Record) error {
	def, err := db.getTableDef(table)
	if err != nil {
		return err
	}
	return db.get(def, rec)
}

func (db *DB) CreateTable(table *TableDef) error {
	query := NewRecord()
	query.AddStr("key", []byte("next_prefix"))
	err := db.get(TDEF_META, &query)
	if !errors.Is(err, ErrRecordNotFound) {
		return err
	}
	if err != nil {
		table.Prefix = 100
		query.AddStr("val", []byte{100})
	} else {
		val, found := query.GetStr("val")
		if !found {
			return fmt.Errorf("val missing")
		}
		table.Prefix = uint32(val[0])
	}
	jsonDef, err := json.Marshal(table)

	t := NewRecord()
	t.AddStr("name", []byte(table.Name))
	t.AddStr("def", jsonDef)

	key, err := TDEF_TABLE.EncodeKey(t)
	if err != nil {
		return err
	}
	value, err := TDEF_TABLE.EncodeValue(t)
	if err != nil {
		return err
	}
	return db.kv.Insert(key, value)
}

func (db *DB) Insert(table string, rec Record) error {
	def, err := db.getTableDef(table)
	if err != nil {
		return err
	}
	return db.insert(def, &rec)
}

func (db *DB) Update(table string, rec Record) error {
	def, err := db.getTableDef(table)
	if err != nil {
		return err
	}
	return db.insert(def, &rec)
}

func (db *DB) Upsert(table string, rec Record) error {
	def, err := db.getTableDef(table)
	if err != nil {
		return err
	}
	return db.insert(def, &rec)
}

func (db *DB) Delete(table string, rec Record) error {
	def, err := db.getTableDef(table)
	if err != nil {
		return err
	}
	return db.delete(def, &rec)
}

func (db *DB) Execute(stmt string) error {
	lexer := engine.NewLexer(stmt)
	tokens := lexer.ReadAll()
	parser := engine.NewParser(tokens)
	statement, err := parser.ParseStatement()
	if err != nil {
		return err
	}
	switch s := statement.(type) {
	case *engine.InsertStmt:
		for _, v := range s.Values {
			rec := NewRecord()

			for i, col := range s.Columns {
				rec.AddStr(col, []byte(v[i]))
			}

			err = db.Insert(s.TableName, rec)
			if err != nil {
				return err
			}

		}

	case *engine.CreateTableStmt:
		primaryKeys := []Column{}
		otherKeys := []Column{}

		for _, astCol := range s.Columns {
			isPrimaryKeys := false

			for _, v := range s.PrimaryKeyColumns {
				if v == astCol.Name {
					isPrimaryKeys = true
				}
			}

			col := Column{
				Name: astCol.Name,
				Type: TYPE_BYTES,
			}

			if isPrimaryKeys {
				primaryKeys = append(primaryKeys, col)
			} else {
				otherKeys = append(otherKeys, col)
			}
		}

		table := NewTableDef(s.TableName, primaryKeys, otherKeys)
		err := db.CreateTable(&table)
		if err != nil {
			return err
		}
	case *engine.NoOpStmt:
		fmt.Printf("No op\n")
	}
	return nil
}

func (db *DB) Close() error {
	return db.kv.Close()
}
