package memEngine

import (
	"matrixone/pkg/vm/engine/memEngine/kv"
	"matrixone/pkg/vm/engine/memEngine/meta"
	"matrixone/pkg/vm/metadata"
)

// standalone memory engine
type memEngine struct {
	db *kv.KV
	n  metadata.Node
}

type database struct {
	id string
	db *kv.KV
	n  metadata.Node
}

type relation struct {
	rid string
	id  string
	db  *kv.KV
	n   metadata.Node
	md  meta.Metadata
}
