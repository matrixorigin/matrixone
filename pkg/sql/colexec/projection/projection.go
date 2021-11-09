package projection

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("π(")
	for i, e := range n.Es {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("%s -> %s", e, n.As[i]))
	}
	buf.WriteString(")")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	var err error

	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Ring.Zs) == 0 {
		return false, nil
	}
	n := arg.(*Argument)
	rbat := batch.New(true, n.As)
	for i, e := range n.Es {
		if rbat.Vecs[i], _, err = e.Eval(bat, proc); err != nil {
			rbat.Vecs = rbat.Vecs[:i]
			batch.Clean(bat, proc.Mp)
			batch.Clean(rbat, proc.Mp)
			return false, err
		}
		rbat.Vecs[i].Ref = n.Rs[i]
	}
	for _, e := range n.Es {
		batch.Reduce(bat, e.Attributes(), proc.Mp)
	}
	if bat.Ro {
		batch.Cow(bat)
	}
	for i := range rbat.Vecs {
		bat.Vecs = append(bat.Vecs, rbat.Vecs[i])
		bat.Attrs = append(bat.Attrs, rbat.Attrs[i])
	}
	proc.Reg.InputBatch = rbat
	return false, nil
}
