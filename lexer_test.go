package database

import (
	"testing"
)

func TestLexer_NextToken(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for receiver constructor.
		input string
		want  []Token
	}{
		{
			name:  "empty string",
			input: "",
			want: []Token{
				{Type: TOKEN_EOF},
			},
		},
		{
			name:  "create table",
			input: "CREATE TABLE test ( pk bytes, val bytes, primary key (pk))",
			want: []Token{
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
			lexer := NewLexer(tt.input)
			got := lexer.ReadAll()
			if len(got) != len(tt.want) {
				t.Fatalf("wrong length of tokens got: %d, want %d, got: %+v", len(got), len(tt.want), got)
			}
			for i := 0; i < len(tt.want); i++ {
				if got[i].Type != tt.want[i].Type {
					t.Fatalf("wrong type of token got on position: %d, got: %d, want: %d", i, got[i].Type, tt.want[i].Type)
				}
				if got[i].Literal != tt.want[i].Literal {
					t.Fatalf("wrong literal of token got on position: %d, got: %s, want: %s", i, got[i].Literal, tt.want[i].Literal)
				}

			}
		})
	}
}
