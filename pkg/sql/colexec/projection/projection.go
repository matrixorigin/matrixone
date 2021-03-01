package projection

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/register"
)

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	es := arg.(Argument).Es
	attrs := arg.(Argument).Attrs
	rbat := batch.New(attrs)
	bat := proc.Reg.Ax.(*batch.Batch)
	for i := range attrs {
		vec, _, err := es[i].Eval(bat, proc)
		if err != nil {
			rbat.Vecs = rbat.Vecs[:i]
			rbat.Free(proc)
			return false, err
		}
		rbat.Vecs[i] = vec
	}
	bat.Free(proc)
	rbat.Sels = bat.Sels
	rbat.SelsData = bat.SelsData
	proc.Reg.Ax = rbat
	register.FreeRegisters(proc)
	return false, nil
}
