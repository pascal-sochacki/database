package engine

import (
	"testing"
)

func TestParser_ParseCreateTableStatement(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for receiver constructor.
		tokens  []Token
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
			tokens: []Token{
				{Type: TOKEN_CREATE, Literal: "CREATE"},
				{Type: TOKEN_TABLE, Literal: "TABLE"},
				{Type: TOKEN_IDENTIFIER, Literal: "test"},
				{Type: TOKEN_LPAREN, Literal: "("},
				{Type: TOKEN_IDENTIFIER, Literal: "pk"},
				{Type: TOKEN_IDENTIFIER, Literal: "bytes"},
				{Type: TOKEN_COMMA, Literal: ","},
				{Type: TOKEN_IDENTIFIER, Literal: "val"},
				{Type: TOKEN_IDENTIFIER, Literal: "bytes"},
				{Type: TOKEN_COMMA, Literal: ","},
				{Type: TOKEN_PRIMARY, Literal: "primary"},
				{Type: TOKEN_KEY, Literal: "key"},
				{Type: TOKEN_LPAREN, Literal: "("},
				{Type: TOKEN_IDENTIFIER, Literal: "pk"},
				{Type: TOKEN_RPAREN, Literal: ")"},
				{Type: TOKEN_RPAREN, Literal: ")"},
				{Type: TOKEN_EOF},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.tokens)
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
