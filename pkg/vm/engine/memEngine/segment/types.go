package segment

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/kv"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
)

type Segment struct {
	id string
	db *kv.KV
	mp map[string]metadata.Attribute
}
