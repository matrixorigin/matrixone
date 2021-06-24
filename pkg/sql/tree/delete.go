package tree

//Delete statement
type Delete struct {
	statementImpl
	Table     TableExpr
	Where     *Where
	OrderBy   OrderBy
	Limit     *Limit
}

func NewDelete(t TableExpr,w *Where,o OrderBy,l *Limit)*Delete{
	return &Delete{
		Table:         t,
		Where:         w,
		OrderBy:       o,
		Limit:         l,
	}
}