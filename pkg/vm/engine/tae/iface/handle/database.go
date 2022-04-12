package handle

import "io"

type Database interface {
	io.Closer

	GetID() uint64
	GetName() string
	CreateRelation(def interface{}) (Relation, error)
	DropRelationByName(name string) (Relation, error)

	GetRelationByName(name string) (Relation, error)
	RelationCnt() int64
	Relations() []Relation

	MakeRelationIt() RelationIt
	String() string
	GetMeta() interface{}
}
