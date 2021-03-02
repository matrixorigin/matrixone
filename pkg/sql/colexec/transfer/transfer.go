package transfer

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/vm/process"
)

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(Argument)
	bat := proc.Reg.Ax.(*batch.Batch)
	n.Ch <- bat
	n.Ch <- nil
	return false, nil
}
