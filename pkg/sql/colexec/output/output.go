package output

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	buf.WriteString("sql output")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	ap := arg.(*Argument)
	if proc.Reg.Ax != nil {
		if bat := proc.Reg.Ax.(*batch.Batch); bat != nil && bat.Attrs != nil {
			if len(ap.Attrs) > 0 {
				bat.Reorder(ap.Attrs)
			}
			if err := ap.Func(ap.Data, bat); err != nil {
				bat.Clean(proc)
				return true, err
			}
			bat.Clean(proc)
		}
	} else {
		ap.Func(ap.Data, nil)
	}
	return false, nil
}
