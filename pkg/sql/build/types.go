package build

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/process"
)

type build struct {
	db   string
	sql  string
	e    engine.Engine
	proc *process.Process
}
