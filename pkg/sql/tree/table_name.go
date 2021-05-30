package tree

type TableName struct {
	TableExpr
	objName
}

var _ TableExpr = &TableName{}

func NewTableName(name Identifier,prefix ObjectNamePrefix)*TableName{
	return &TableName{
		objName:    objName{
			ObjectName: name,
			ObjectNamePrefix:prefix,
		},
	}
}