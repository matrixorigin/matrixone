package mergesum

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
	for i, c := range proc.Reg.Cs {
		v := <-c
		if v == nil {
			proc.Reg.Cs = append(proc.Reg.Cs[:i], proc.Reg.Cs[i:]...)
			continue
		}
		bat := v.(*batch.Batch)
		for _, e := range n.Es {
			vec, err := bat.GetVector(e.Name, proc)
			if err != nil {
				return false, err
			}
			if err := e.Agg.Fill(bat.Sels, vec); err != nil {
				return false, err
			}
		}
		bat.Free(proc)
	}
	rbat := batch.New(n.Attrs)
	{
		var err error
		for i, e := range n.Es {
			rbat.Vecs[i], err = e.Agg.Eval(proc)
			if err != nil {
				rbat.Vecs = rbat.Vecs[:i]
				rbat.Free(proc)
				return false, err
			}
		}
	}
	proc.Reg.Ax = rbat
	register.FreeRegisters(proc)
	return false, nil
}
