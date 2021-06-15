package myoutput

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	buf.WriteString("mysql output")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	rp := arg.(*Argument).Res
	if proc.Reg.Ax != nil {
		bat := proc.Reg.Ax.(*batch.Batch)
		if bat != nil {
			bat.Reorder(rp.Attrs)
			if err := bat.Prefetch(bat.Attrs, bat.Vecs, proc); err != nil {
				return false, err
			}
			if err := rp.FillResult(bat); err != nil {
				bat.Clean(proc)
				return false, err
			}
			bat.Clean(proc)
		}
	}
	return false, nil
}
