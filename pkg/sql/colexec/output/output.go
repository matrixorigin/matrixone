package output

import (
	"fmt"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/vm/process"
)

func Call(proc *process.Process, arg interface{}) (bool, error) {
	bat := proc.Reg.Ax.(*batch.Batch)
	fmt.Printf("%s\n", bat)
	bat.Free(proc)
	return false, nil
}
