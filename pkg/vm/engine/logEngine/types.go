package logEngine

import (
	"matrixone/pkg/vm/engine/logEngine/kv"
	"matrixone/pkg/vm/engine/logEngine/meta"
	"matrixone/pkg/vm/metadata"
)

type logEngine struct {
	path string
	db   *kv.KV // column store
}

type relation struct {
	id string
	db *kv.KV
	md meta.Metadata
	mp map[string]metadata.Attribute
}
