package engine

type AstColumn struct {
	Name string
	Type string
}

var _ Node = &CreateTableStmt{}

type CreateTableStmt struct {
	TableName         string
	Columns           []AstColumn
	PrimaryKeyColumns []string
}

// StatementType implements Node.
func (c *CreateTableStmt) StatementType() string {
	return "CREATE_TABLE"
}

var _ Node = &NoOpStmt{}

type NoOpStmt struct {
}

// StatementType implements Node.
func (n *NoOpStmt) StatementType() string {
	return "NO_OP"
}

var _ Node = &InsertStmt{}

type InsertStmt struct {
	TableName string
	Columns   []string
	Values    [][]string
}

// StatementType implements Node.
func (i *InsertStmt) StatementType() string {
	return "Insert"
}
