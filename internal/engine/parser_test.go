package engine

import (
	"testing"
)

func TestParser_ParseSelectStatement(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for receiver constructor.
		input   string
		want    SelectStmt
		wantErr bool
	}{
		{
			name:    "Insert into",
			wantErr: false,
			want: SelectStmt{
				TableName: "test",
			},
			input: "SELECT * FROM test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewLexer(tt.input)
			tokens := l.ReadAll()
			p := NewParser(tokens)
			got, gotErr := p.ParseStatement()
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("ParseStatement() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("ParseStatement() succeeded unexpectedly")
			}
			createTable, ok := got.(*SelectStmt)
			if !ok {
				t.Fatalf("got wrong statement type: %+v", got)
			}

			if tt.want.TableName != createTable.TableName {
				t.Fatalf("got the name wrong of create table parse got: %s, wanted: %s", createTable.TableName, tt.want.TableName)
			}
		})
	}
}

func TestParser_ParseInsertStatement(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for receiver constructor.
		input   string
		want    InsertStmt
		wantErr bool
	}{
		{
			name:    "Insert into",
			wantErr: false,
			want: InsertStmt{
				TableName: "test",

				Columns: []string{
					"pk", "val",
				},
				Values: [][]string{
					{"primary", "values"},
				},
			},
			input: "INSERT INTO test (pk, val) VALUES ('primary', 'values')",
		},
		{
			name:    "Insert into multiple columns",
			wantErr: false,
			want: InsertStmt{
				TableName: "test",

				Columns: []string{
					"pk", "val",
				},
				Values: [][]string{
					{"primary1", "values1"},
					{"primary2", "values2"},
				},
			},
			input: "INSERT INTO test (pk, val) VALUES ('primary1', 'values1'),('primary2', 'values2')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewLexer(tt.input)
			tokens := l.ReadAll()
			p := NewParser(tokens)
			got, gotErr := p.ParseStatement()
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("ParseStatement() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("ParseStatement() succeeded unexpectedly")
			}
			createTable, ok := got.(*InsertStmt)
			if !ok {
				t.Fatalf("got wrong statement type: %+v", got)
			}

			if tt.want.TableName != createTable.TableName {
				t.Fatalf("got the name wrong of create table parse got: %s, wanted: %s", createTable.TableName, tt.want.TableName)
			}

			if len(createTable.Columns) != len(tt.want.Columns) {
				t.Fatalf("got the wrong length of columns in create table parse got: %d, wanted: %d", len(createTable.Columns), len(tt.want.Columns))
			}

		})
	}
}

func TestParser_ParseCreateTableStatement(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for receiver constructor.
		input   string
		want    CreateTableStmt
		wantErr bool
	}{
		{
			name:    "Create Table",
			wantErr: false,
			want: CreateTableStmt{
				TableName: "test",
				Columns: []AstColumn{
					{Name: "pk", Type: "bytes"},
					{Name: "val", Type: "bytes"},
				},
				PrimaryKeyColumns: []string{
					"pk",
				},
			},
			input: "CREATE TABLE test ( pk bytes, val bytes, primary key (pk))",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewLexer(tt.input)
			tokens := l.ReadAll()
			p := NewParser(tokens)
			got, gotErr := p.ParseStatement()
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("ParseStatement() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("ParseStatement() succeeded unexpectedly")
			}
			createTable, ok := got.(*CreateTableStmt)
			if !ok {
				t.Fatalf("got wrong statement type: %+v", got)
			}

			if tt.want.TableName != createTable.TableName {
				t.Fatalf("got the name wrong of create table parse got: %s, wanted: %s", createTable.TableName, tt.want.TableName)
			}

			if len(createTable.Columns) != len(tt.want.Columns) {
				t.Fatalf("got the wrong length of columns in create table parse got: %d, wanted: %d", len(createTable.Columns), len(tt.want.Columns))
			}
		})
	}
}
