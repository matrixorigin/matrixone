package tree

//the VALUES clause
type ValuesClause struct {
	SelectStatement
	Rows []Exprs
}

func NewValuesClause(r []Exprs)*ValuesClause{
	return &ValuesClause{
		Rows:            r,
	}
}