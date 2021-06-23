package relation

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
)

type Segment struct {
	IsRemote bool
	Id       string
	GroupId  string
	TabletId string
	Node     metadata.Node
}

type Unit struct {
	Segs []*Segment
	N    metadata.Node
}

type Relation struct {
	S     bool // is single
	ID    string
	DB    string
	Rid   string // real id
	Us    []*Unit
	Cols  []string
	R     engine.Relation
	Attrs map[string]types.Type
}
