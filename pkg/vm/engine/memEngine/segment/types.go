package segment

import (
	"matrixone/pkg/vm/engine/memEngine/kv"
	"matrixone/pkg/vm/metadata"
)

type Segment struct {
	id string
	db *kv.KV
	mp map[string]metadata.Attribute
}
