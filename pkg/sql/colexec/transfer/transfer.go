package transfer

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("transfer")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	reg := n.Reg
	if reg.Ch == nil {
		if proc.Reg.Ax != nil {
			bat := proc.Reg.Ax.(*batch.Batch)
			bat.Clean(proc)
		}
		return true, nil
	}
	reg.Wg.Add(1)
	reg.Ch <- proc.Reg.Ax
	n.Mmu.Alloc(proc.Size())
	proc.Gm.Free(proc.Size())
	reg.Wg.Wait()
	return false, nil
}
