package memEngine

import (
	"matrixbase/pkg/vm/engine/memEngine/kv"
	"matrixbase/pkg/vm/engine/memEngine/meta"
	"matrixbase/pkg/vm/mempool"
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
