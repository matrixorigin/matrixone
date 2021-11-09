package connector

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("pipe connector")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	reg := n.Reg
	if reg.Ch == nil {
		if bat := proc.Reg.InputBatch; bat != nil {
			batch.Clean(bat, proc.Mp)
		}
		process.FreeRegisters(proc)
		return true, nil
	}
	bat := proc.Reg.InputBatch
	if bat == nil {
		reg.Wg.Add(1)
		reg.Ch <- nil
		reg.Wg.Wait()
		process.FreeRegisters(proc)
		return true, nil
	}
	size := int64(0)
	vecs := n.vecs[:0]
	for i := range bat.Vecs {
		if bat.Vecs[i].Or {
			vec, err := vector.Dup(bat.Vecs[i], proc.Mp)
			if err != nil {
				return false, err
			}
			vecs = append(vecs, vec)
		} else {
			size += int64(cap(bat.Vecs[i].Data))
		}
	}
	for i := range bat.Vecs {
		if bat.Vecs[i].Or {
			bat.Vecs[i] = vecs[0]
			vecs = vecs[1:]
		}
	}
	reg.Wg.Add(1)
	reg.Ch <- proc.Reg.InputBatch
	n.Mmu.Alloc(size)
	proc.Mp.Gm.Free(size)
	reg.Wg.Wait()
	return false, nil
}
