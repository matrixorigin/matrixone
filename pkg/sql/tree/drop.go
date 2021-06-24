package tree

//DROP Database statement
type DropDatabase struct {
	statementImpl
	Name Identifier
	IfExists bool
}

func NewDropDatabase(n Identifier,i bool)*DropDatabase{
	return &DropDatabase{
		Name:          n,
		IfExists:      i,
	}
}

//DROP Table statement
type DropTable struct {
	statementImpl
	IfExists bool
	Names TableNames
}

func NewDropTable(i bool,n TableNames) *DropTable {
	return &DropTable{
		IfExists:      i,
		Names:         n,
	}
}