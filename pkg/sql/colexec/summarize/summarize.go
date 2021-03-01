package summarize

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/register"
)

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(Argument)
	n.Attrs = make([]string, len(n.Es))
	for i, e := range n.Es {
		n.Attrs[i] = e.Alias
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(Argument)
	rbat := batch.New(n.Attrs)
	bat := proc.Reg.Ax.(*batch.Batch)
	for i, e := range n.Es {
		vec, err := bat.GetVector(e.Name, proc)
		if err != nil {
			return false, err
		}
		if err := e.Agg.Fill(bat.Sels, vec); err != nil {
			return false, err
		}
		rbat.Vecs[i], err = e.Agg.Eval(proc)
		if err != nil {
			return false, err
		}
	}
	bat.Free(proc)
	proc.Reg.Ax = rbat
	register.FreeRegisters(proc)
	return false, nil
}
