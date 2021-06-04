package tree

import "fmt"

//the common interface for qualified object names
type ObjectName interface {
	NodePrinter
}

//the internal type for a qualified object.
type objName struct {
	//the path to the object.
	ObjectNamePrefix

	//the unqualified name for the object
	ObjectName Identifier
}

//the path prefix of an object name.
type ObjectNamePrefix struct {
	CatalogName Identifier
	SchemaName  Identifier

	//true iff the catalog was explicitly specified
	ExplicitCatalog bool
	//true iff the schema was explicitly specified
	ExplicitSchema bool
}

//the unresolved qualified name for a database object (table, view, etc)
type UnresolvedObjectName struct {
	//the number of name parts; >= 1
	NumParts int

	//At most three components, in reverse order.
	//object name, db/schema, catalog.
	Parts [3]string
}

func (u *UnresolvedObjectName) ToTableName() TableName {
	return TableName{
		objName: objName{
			ObjectNamePrefix: ObjectNamePrefix{
				SchemaName: Identifier(u.Parts[1]),
				CatalogName: Identifier(u.Parts[2]),
				ExplicitSchema: u.NumParts >= 2,
				ExplicitCatalog: u.NumParts >= 3,
			},
			ObjectName:       Identifier(u.Parts[0]),
		},
	}
}

func NewUnresolvedObjectName(num int,parts [3]string)(*UnresolvedObjectName,error){
	if num < 1 || num > 3{
		return nil,fmt.Errorf("invalid number of parts.")
	}
	return &UnresolvedObjectName{
		NumParts: num,
		Parts:    parts,
	},nil
}

