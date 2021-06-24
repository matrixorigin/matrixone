package tree

//Use statement
type Use struct {
	statementImpl
	Name string
}

func NewUse(n string) *Use {
	return &Use{Name: n}
}