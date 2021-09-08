package transfer

import (
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
)

type Argument struct {
	vecs []*vector.Vector
	Proc *process.Process
	Reg  *process.WaitRegister
}
