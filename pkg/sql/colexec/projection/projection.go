package projection

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("π(")
	for i, e := range n.Es {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(e.String())
	}
	buf.WriteString(")")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	var err error

	if proc.Reg.Ax == nil {
		return false, nil
	}
	n := arg.(*Argument)
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat.Attrs == nil {
		return false, nil
	}
	rbat := batch.New(true, n.Attrs)
	for i := range n.Attrs {
		if rbat.Vecs[i], _, err = n.Es[i].Eval(bat, proc); err != nil {
			rbat.Vecs = rbat.Vecs[:i]
			clean(bat, rbat, proc)
			return false, err
		}
		if count, ok := proc.Refer[n.Attrs[i]]; ok {
			copy(rbat.Vecs[i].Data, encoding.EncodeUint64(count))
		} else {
			count = encoding.DecodeUint64(rbat.Vecs[i].Data)
			copy(rbat.Vecs[i].Data, encoding.EncodeUint64(1+count))
		}
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
