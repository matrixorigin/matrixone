package tree

type SetVar struct {
	statementImpl
	Assignments []*VarAssignmentExpr
}

func NewSetVar(a []*VarAssignmentExpr)*SetVar{
	return &SetVar{
		Assignments:   a,
	}
}

//for variable = expr
type VarAssignmentExpr struct {
	NodePrinter
	System bool
	Global bool
	Name string
	Value Expr
	Reserved Expr
}

func NewVarAssignmentExpr(s bool, g bool, n string, v Expr, r Expr) *VarAssignmentExpr {
	return &VarAssignmentExpr{
		System:      s,
		Global:      g,
		Name:        n,
		Value:       v,
		Reserved:    r,
	}
}