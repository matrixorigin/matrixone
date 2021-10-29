package memEngine

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/memEngine/kv"
	"matrixone/pkg/vm/engine/memEngine/meta"
)

// standalone memory engine
type memEngine struct {
	db *kv.KV
	n  engine.Node
}

type database struct {
	db *kv.KV
	n  engine.Node
}

type relation struct {
	id string
	db *kv.KV
	n  engine.Node
	md meta.Metadata
}
