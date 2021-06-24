package tree

type Explain interface {
	Statement
}

type explainImpl struct {
	Explain
	Statement Statement
	Format string
}

//EXPLAIN stmt statement
type ExplainStmt struct {
	explainImpl
}

func NewExplainStmt(stmt Statement,f string)*ExplainStmt{
	return &ExplainStmt{ explainImpl{Statement: stmt,Format: f}}
}

//EXPLAIN ANALYZE statement
type ExplainAnalyze struct {
	explainImpl
}

func NewExplainAnalyze(stmt Statement, f string) *ExplainAnalyze {
	return &ExplainAnalyze{explainImpl{Statement: stmt,Format: f}}
}

//EXPLAIN FOR CONNECTION statement
type ExplainFor struct {
	explainImpl
	ID uint64
}

func NewExplainFor(f string,id uint64)*ExplainFor{
	return &ExplainFor{
		explainImpl: explainImpl{Statement: nil,Format: f},
		ID:          id,
	}
}