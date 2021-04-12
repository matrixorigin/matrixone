package spillEngine

import (
	"matrixone/pkg/vm/engine/spillEngine/kv"
	"matrixone/pkg/vm/engine/spillEngine/meta"
	"matrixone/pkg/vm/metadata"
)

type spillEngine struct {
	db   *kv.KV
	path string
}

type relation struct {
	id string
	db *kv.KV
	md meta.Metadata
	mp map[string]metadata.Attribute
}
