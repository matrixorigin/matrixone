package relation

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
)

type Relation struct {
	ID    string
	Rid   string // real id
	Segs  []string
	Cols  []string
	R     engine.Relation
	Attrs map[string]types.Type
}
