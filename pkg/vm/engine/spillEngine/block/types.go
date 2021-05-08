package block

import (
	"matrixone/pkg/vm/engine/spillEngine/kv"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

type Block struct {
	id   string
	db   *kv.KV
	proc *process.Process
	mp   map[string]metadata.Attribute
}
