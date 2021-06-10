package memEngine

import (
	"matrixone/pkg/vm/engine/memEngine/kv"
	"matrixone/pkg/vm/engine/memEngine/meta"
	"matrixone/pkg/vm/process"
)

// standalone memory engine
type memEngine struct {
	db   *kv.KV
	proc *process.Process
}

type database struct {
	db   *kv.KV
	proc *process.Process
}

type relation struct {
	id string
	db *kv.KV
	md meta.Metadata
}
