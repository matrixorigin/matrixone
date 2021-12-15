package block

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/kv"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
)

type Block struct {
	id string
	db *kv.KV
	mp map[string]metadata.Attribute
}
