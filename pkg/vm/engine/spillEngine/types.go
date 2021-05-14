package spillEngine

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/spillEngine/kv"
	"matrixone/pkg/vm/engine/spillEngine/meta"
	"matrixone/pkg/vm/metadata"
)

type spillEngine struct {
	path string
	cdb  *kv.KV    // column store
	db   engine.DB // kv store
}

type relation struct {
	id string
	db *kv.KV
	md meta.Metadata
	mp map[string]metadata.Attribute
}
