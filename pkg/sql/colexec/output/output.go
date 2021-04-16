package output

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	buf.WriteString("output")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	if proc.Reg.Ax != nil {
		bat := proc.Reg.Ax.(*batch.Batch)
		if bat != nil {
			fmt.Printf("%s\n", bat)
			bat.Clean(proc)
		}
	}
	return false, nil
}
