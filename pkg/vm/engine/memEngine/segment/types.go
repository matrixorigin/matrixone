package segment

import (
	"matrixbase/pkg/vm/engine/memEngine/kv"
	"matrixbase/pkg/vm/metadata"
	"matrixbase/pkg/vm/process"
)

type Segment struct {
	id   string
	db   *kv.KV
	proc *process.Process
	mp   map[string]metadata.Attribute
}
