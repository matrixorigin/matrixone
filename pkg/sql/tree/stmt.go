package tree

import "fmt"

type Statement interface {
	fmt.Stringer
	NodePrinter
}

type statementImpl struct {
	Statement
}
