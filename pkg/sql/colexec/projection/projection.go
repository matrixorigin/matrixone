package projection

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/vm/mempool"
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
	if len(bat.Sels) > 0 {
		rbat.Sels = bat.Sels
		rbat.SelsData = bat.SelsData
		bat.Sels = nil
		bat.SelsData = nil
	}
	for i := range n.Attrs {
		if rbat.Vecs[i], _, err = n.Es[i].Eval(bat, proc); err != nil {
			rbat.Vecs = rbat.Vecs[:i]
			clean(bat, rbat, proc)
			return false, err
		}
		if _, ok := n.Es[i].(*extend.Attribute); !ok {
			copy(rbat.Vecs[i].Data[:mempool.CountSize], encoding.EncodeUint64(n.Refer[n.Attrs[i]]))
		}
	}
	{
		for _, e := range n.Es {
			if _, ok := e.(*extend.Attribute); !ok {
				bat.Reduce(e.Attributes(), proc)
			}
		}
	}
	proc.Reg.Ax = rbat
	register.FreeRegisters(proc)
	return false, nil
}

func clean(bat, rbat *batch.Batch, proc *process.Process) {
	bat.Clean(proc)
	rbat.Clean(proc)
	register.FreeRegisters(proc)
}
