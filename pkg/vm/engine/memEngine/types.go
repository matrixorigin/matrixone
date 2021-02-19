package memEngine

import (
	"matrixbase/pkg/mempool"
	"matrixbase/pkg/vm/engine/memEngine/kv"
	"matrixbase/pkg/vm/engine/memEngine/meta"
)

// standalone memory engine
type memEngine struct {
	db *kv.KV
	mp *mempool.Mempool
}

type relation struct {
	id string
	db *kv.KV
	md meta.Metadata
}
