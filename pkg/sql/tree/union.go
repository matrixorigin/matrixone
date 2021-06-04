package tree

//the UNION statement
type UnionClause struct {
	SelectStatement
	Type        UnionType
	//Left, Right *Select
	Left, Right SelectStatement
	All         bool
}

//set operations
type UnionType int

const (
	UNION UnionType = iota
	INTERSECT
	EXCEPT
)

var unionTypeName = []string{
	"UNION",
	"INTERSECT",
	"EXCEPT",
}

//func NewUnionClause(t UnionType,l,r *Select,a bool)*UnionClause{
func NewUnionClause(t UnionType,l,r SelectStatement,a bool)*UnionClause{
	return &UnionClause{
		Type:            t,
		Left:            l,
		Right:           r,
		All:             a,
	}
}