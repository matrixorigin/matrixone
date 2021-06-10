package tree

import "go/constant"

//the AST for literals like string,numeric,bool and etc.
type Constant interface {
	Expr
}

//the AST for the constant numeric value.
type NumVal struct {
	Constant
	Value constant.Value

	// negative is the sign label
	negative bool

	// origString is the "original" string literals that should remain sign-less.
	origString string

	//converted result
	resInt     int64
	resFloat   float64
}

func (n *NumVal) String() string {
	return n.origString
}

func NewNumVal(value constant.Value, origString string, negative bool) *NumVal {
	return &NumVal{Value: value, origString: origString, negative: negative}
}