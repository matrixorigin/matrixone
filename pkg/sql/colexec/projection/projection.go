package projection

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/vm/process"
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
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		return false, nil
	}
	bat.Shuffle(proc)
	n := arg.(*Argument)
	rbat := batch.New(true, n.Attrs)
	for i := range n.Attrs {
		if rbat.Vecs[i], _, err = n.Es[i].Eval(bat, proc); err != nil {
			rbat.Vecs = rbat.Vecs[:i]
			bat.Clean(proc)
			rbat.Clean(proc)
			return false, err
		}
	}
	for i, e := range n.Es {
		if _, ok := e.(*extend.Attribute); !ok {
			bat.Reduce(e.Attributes(), proc)
		}
		if name, ok := e.(*extend.Attribute); !ok || name.Name != n.Attrs[i] {
			rbat.Vecs[i].Ref = n.Refer[n.Attrs[i]]
		}
	}
	proc.Reg.Ax = rbat
	return false, nil
}
