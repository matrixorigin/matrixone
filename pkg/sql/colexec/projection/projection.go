package projection

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/vm/process"
)

func Call(proc *process.Process, arg interface{}) (bool, error) {
	attrs := arg.(Argument).Attrs
	bat := proc.Reg.Ax.(*batch.Batch)
	rbat := batch.New(attrs)
	for i, attr := range attrs {
		vec, err := bat.GetVector(attr, proc)
		if err != nil {
			return false, nil
		}
		rbat.Vecs[i] = vec
	}
	proc.Reg.Ax = rbat
	return false, nil
}
