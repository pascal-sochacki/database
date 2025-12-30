package database

import "fmt"

type Parser struct {
	tokens       []Token
	current      Token
	position     int
	readPosition int
}

func (parser *Parser) readToken() {
	if parser.position >= len(parser.tokens) {
		parser.current = Token{Type: TOKEN_EOF}
	} else {
		parser.current = parser.tokens[parser.readPosition]
	}
	parser.position = parser.readPosition
	parser.readPosition += 1
}

type Node interface {
	StatementType() string
}

func NewParser(tokens []Token) *Parser {
	parser := &Parser{
		tokens:       tokens,
		position:     -1,
		readPosition: 0,
	}
	parser.readToken()
	return parser
}

func (p *Parser) ParseStatement() (Node, error) {
	switch p.current.Type {
	case TOKEN_EOF:
		return &NoOpStmt{}, nil
	case TOKEN_CREATE:
		return p.ParseCreateStatement()
	}
	return &NoOpStmt{}, nil

}

func (p *Parser) ParseCreateStatement() (Node, error) {
	p.readToken()
	switch p.current.Type {
	case TOKEN_TABLE:
		return p.ParseCreateTableStatement()
	}
	return &NoOpStmt{}, nil
}

func (p *Parser) ParseCreateTableStatement() (Node, error) {
	p.readToken()
	result := CreateTableStmt{}

	if p.current.Type != TOKEN_IDENTIFIER {
		return nil, fmt.Errorf("should get table name")
	}
	result.TableName = p.current.Literal

	p.readToken()

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}
	for p.current.Type != TOKEN_RPAREN {
		switch p.current.Type {
		case TOKEN_PRIMARY:
			p.readToken()

			if err := p.expect(TOKEN_KEY); err != nil {
				return nil, err
			}
			if err := p.expect(TOKEN_LPAREN); err != nil {
				return nil, err
			}

			if p.current.Type != TOKEN_IDENTIFIER {
				return nil, fmt.Errorf("expected primary key identifier")
			}
			result.PrimaryKeyColumns = append(result.PrimaryKeyColumns, p.current.Literal)
			p.readToken()
			if p.current.Type != TOKEN_RPAREN {
				return nil, fmt.Errorf("expected closing parentheses")
			}

		case TOKEN_IDENTIFIER:
			col := AstColumn{}
			col.Name = p.current.Literal

			p.readToken()
			if p.current.Type != TOKEN_IDENTIFIER {
				return nil, fmt.Errorf("expected datatype got: %s", p.current.Literal)
			}

			col.Type = p.current.Literal
			p.readToken()

			if p.current.Type == TOKEN_COMMA {
				p.readToken()
			}

			result.Columns = append(result.Columns, col)
			continue
		}
		p.readToken()
	}
	return &result, nil
}

func (p *Parser) expect(t TokenType) error {
	if p.current.Type != t {
		return fmt.Errorf("expected %d, got %d", t, p.current.Type)
	}
	p.readToken()
	return nil
}
