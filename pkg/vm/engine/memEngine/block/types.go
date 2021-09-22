package block

import (
	"matrixone/pkg/vm/engine/memEngine/kv"
	"matrixone/pkg/vm/metadata"
)

type Block struct {
	id string
	db *kv.KV
	mp map[string]metadata.Attribute
}
