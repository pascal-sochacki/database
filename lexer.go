package database

import "strings"

type TokenType int

const (
	// Special types
	TOKEN_ILLEGAL TokenType = iota
	TOKEN_EOF

	// Identifiers + Literals
	TOKEN_IDENTIFIER // table_name, column_name

	// Keywords
	TOKEN_CREATE
	TOKEN_TABLE
	TOKEN_INDEX
	TOKEN_PRIMARY
	TOKEN_KEY

	// Punctuators
	TOKEN_LPAREN    // (
	TOKEN_RPAREN    // )
	TOKEN_COMMA     // ,
	TOKEN_SEMICOLON // ;
)

type Token struct {
	Type    TokenType
	Literal string // The actual text (e.g., "users" or "CREATE")
}

type Lexer struct {
	input        string
	position     int  // current position in input (points to current char)
	readPosition int  // current reading position in input (after current char)
	ch           byte // current char under examination
}

var keywords = map[string]TokenType{
	"CREATE":  TOKEN_CREATE,
	"TABLE":   TOKEN_TABLE,
	"INDEX":   TOKEN_INDEX,
	"PRIMARY": TOKEN_PRIMARY,
	"KEY":     TOKEN_KEY,
}

func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return TOKEN_IDENTIFIER
}

func NewLexer(input string) *Lexer {
	lexer := &Lexer{
		input:        input,
		position:     -1,
		readPosition: 0,
	}
	lexer.readChar() // init char
	return lexer
}

func (lexer *Lexer) ReadAll() []Token {
	result := []Token{}
	for {
		token := lexer.NextToken()
		result = append(result, token)
		if token.Type == TOKEN_EOF {
			break
		}
	}
	return result
}

func (lexer *Lexer) NextToken() Token {
	lexer.skipWhitespace()
	var tok Token
	switch lexer.ch {
	case '(':
		tok = Token{Type: TOKEN_LPAREN, Literal: string(lexer.ch)}
	case ')':
		tok = Token{Type: TOKEN_RPAREN, Literal: string(lexer.ch)}
	case ',':
		tok = Token{Type: TOKEN_COMMA, Literal: string(lexer.ch)}
	case ';':
		tok = Token{Type: TOKEN_SEMICOLON, Literal: string(lexer.ch)}
	case 0:
		tok = Token{Type: TOKEN_EOF, Literal: ""}
	default:
		startPos := lexer.position
		for lexer.IsLetter(lexer.peek()) {
			lexer.readChar()
		}
		// Now position is at the last letter of the word
		literal := lexer.input[startPos : lexer.position+1]

		// Crucial: Move the pointer forward so the NEXT call
		// to NextToken starts at the right place
		lexer.readChar()
		return Token{
			Type:    LookupIdent(literal),
			Literal: literal,
		}

	}
	lexer.readChar() // Always move forward after consuming a single-char token
	return tok
}

func (lexer *Lexer) skipWhitespace() {
	for lexer.ch == ' ' || lexer.ch == '\t' || lexer.ch == '\n' || lexer.ch == '\r' {
		lexer.readChar()
	}
}

func (lexer *Lexer) IsLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func (l *Lexer) peek() byte {
	if l.readPosition >= len(l.input) {
		return 0
	} else {
		return l.input[l.readPosition]
	}
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0 // ASCII code for "NUL", signals EOF
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition += 1

}
