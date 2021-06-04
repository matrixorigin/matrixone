package tree

//the INSERT statement.
type Insert struct {
	statementImpl
	Table      TableExpr
	Columns    IdentifierList
	Rows       *Select
	PartitionNames IdentifierList
}

func NewInsert(t TableExpr, c IdentifierList, r *Select, p IdentifierList) *Insert {
	return &Insert{
		Table:   t,
		Columns: c,
		Rows:    r,
		PartitionNames: p,
	}
}