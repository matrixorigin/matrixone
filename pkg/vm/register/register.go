package register

import (
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/vm/process"
)

func FreeRegisters(proc *process.Process) {
	var vec *vector.Vector

	for _, t := range proc.Reg.Ts {
		vec = t.(*vector.Vector)
		vec.Free(proc)
	}
	proc.Reg.Ts = proc.Reg.Ts[:0]
}
