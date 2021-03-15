package restrict

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/register"
)

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(Argument)
	n.Attrs = n.E.Attributes()
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(Argument)
	bat := proc.Reg.Ax.(*batch.Batch)
	vec, _, err := n.E.Eval(bat, proc)
	if err != nil {
		return false, err
	}
	bat.SelsData = vec.Data
	bat.Sels = vec.Col.([]int64)
	bat.Reduce(n.Attrs, proc)
	proc.Reg.Ax = bat
	register.FreeRegisters(proc)
	return false, nil
}
