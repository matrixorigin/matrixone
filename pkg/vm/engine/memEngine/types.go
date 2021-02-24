package memEngine

import (
	"matrixbase/pkg/vm/engine/memEngine/kv"
	"matrixbase/pkg/vm/engine/memEngine/meta"
	"matrixbase/pkg/vm/process"
)

// standalone memory engine
type memEngine struct {
	db   *kv.KV
	proc *process.Process
}

type relation struct {
	id string
	db *kv.KV
	md meta.Metadata
}
