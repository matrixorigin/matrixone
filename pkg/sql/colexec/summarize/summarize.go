package summarize

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/register"
)

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Attrs = make([]string, len(n.Es))
	for i, e := range n.Es {
		n.Attrs[i] = e.Alias
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	rbat := batch.New(true, n.Attrs)
	bat := proc.Reg.Ax.(*batch.Batch)
	for i, e := range n.Es {
		vec, err := bat.GetVector(e.Name, proc)
		if err != nil {
			rbat.Vecs = rbat.Vecs[:i]
			clean(bat, rbat, proc)
			return false, err
		}
		if err := e.Agg.Fill(bat.Sels, vec); err != nil {
			rbat.Vecs = rbat.Vecs[:i]
			clean(bat, rbat, proc)
			return false, err
		}
		if rbat.Vecs[i], err = e.Agg.Eval(proc); err != nil {
			rbat.Vecs = rbat.Vecs[:i]
			clean(bat, rbat, proc)
			return false, err
		}
		copy(rbat.Vecs[i].Data, encoding.EncodeUint64(1+proc.Refer[n.Attrs[i]]))
	}
	bat.Clean(proc)
	proc.Reg.Ax = rbat
	register.FreeRegisters(proc)
	return false, nil
}

func clean(bat, rbat *batch.Batch, proc *process.Process) {
	bat.Clean(proc)
	rbat.Clean(proc)
	register.FreeRegisters(proc)
}
