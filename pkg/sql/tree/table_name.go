package tree

type TableName struct {
	TableExpr
	objName
}

func (tn *TableName) Name() Identifier  {
	return tn.ObjectName
}

func (tn *TableName) Schema() Identifier {
	return tn.SchemaName
}

func (tn *TableName) Catalog() Identifier {
	return tn.CatalogName
}

var _ TableExpr = &TableName{}

//table name array
type TableNames []*TableName

func NewTableName(name Identifier,prefix ObjectNamePrefix)*TableName{
	return &TableName{
		objName:    objName{
			ObjectName: name,
			ObjectNamePrefix:prefix,
		},
	}
}