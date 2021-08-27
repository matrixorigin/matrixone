package segment

import (
	"matrixone/pkg/vm/engine/logEngine/kv"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

type Segment struct {
	id   string
	db   *kv.KV
	proc *process.Process
	mp   map[string]metadata.Attribute
}
