package restrict

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("σ(%s)", n.E))
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Attrs = n.E.Attributes()
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	if proc.Reg.Ax == nil {
		return false, nil
	}
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		return false, nil
	}
	if _, ok := n.E.(*extend.Attribute); ok {
		proc.Reg.Ax = bat
		return false, nil
	}
	vec, _, err := n.E.Eval(bat, proc)
	if err != nil {
		bat.Clean(proc)
		clean(proc)
		return false, err
	}
	bat.SelsData = vec.Data
	bat.Sels = vec.Col.([]int64)
	if len(bat.Sels) > 0 {
		bat.Reduce(n.Attrs, proc)
		proc.Reg.Ax = bat
	} else {
		bat.Clean(proc)
		bat.Attrs = nil
		proc.Reg.Ax = bat
	}
	register.FreeRegisters(proc)
	return false, nil
}

func clean(proc *process.Process) {
	register.FreeRegisters(proc)
}
