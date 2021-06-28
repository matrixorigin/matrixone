package memEngine

import (
	"matrixone/pkg/vm/engine/memEngine/kv"
	"matrixone/pkg/vm/engine/memEngine/meta"
	"matrixone/pkg/vm/process"
)

// standalone memory engine
type memEngine struct {
	proc *process.Process
	mp   map[string]*database
}

type database struct {
	db   *kv.KV
	mp   map[string]uint8
	proc *process.Process
}

type relation struct {
	id string
	db *kv.KV
	md meta.Metadata
}
