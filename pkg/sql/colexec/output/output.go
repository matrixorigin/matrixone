package output

import (
	"bytes"
	"fmt"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/vm/process"
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
		fmt.Printf("%s\n", bat)
		bat.Clean(proc)
	}
	return false, nil
}
