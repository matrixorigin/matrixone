package projection

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/register"
)

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	var err error

	n := arg.(*Argument)
	rbat := batch.New(n.Attrs)
	bat := proc.Reg.Ax.(*batch.Batch)
	for i := range n.Attrs {
		if rbat.Vecs[i], _, err = n.Es[i].Eval(bat, proc); err != nil {
			rbat.Vecs = rbat.Vecs[:i]
			clean(bat, rbat, proc)
			return false, err
		}
		copy(rbat.Vecs[i].Data, encoding.EncodeUint64(1+proc.Refer[n.Attrs[i]]))
	}
	{
		for _, e := range n.Es {
			bat.Reduce(e.Attributes(), proc)
		}
	}
	{
		mp := make(map[string]uint8)
		{
			for _, attr := range bat.Attrs {
				mp[attr] = 0
			}
		}
		for i, attr := range rbat.Attrs {
			if _, ok := mp[attr]; !ok {
				bat.Attrs = append(bat.Attrs, attr)
				bat.Vecs = append(bat.Vecs, rbat.Vecs[i])
			}
		}
	}
	proc.Reg.Ax = bat
	register.FreeRegisters(proc)
	return false, nil
}

func clean(bat, rbat *batch.Batch, proc *process.Process) {
	bat.Clean(proc)
	rbat.Clean(proc)
	register.FreeRegisters(proc)
}
