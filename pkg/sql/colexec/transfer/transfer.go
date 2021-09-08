package transfer

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("=>")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	reg := n.Reg
	if reg.Ch == nil {
		if proc.Reg.Ax != nil {
			if bat := proc.Reg.Ax.(*batch.Batch); bat != nil {
				bat.Clean(proc)
			}
		}
		mempool.Pool.Put(proc.Mp)
		register.FreeRegisters(proc)
		return true, nil
	}
	if proc.Reg.Ax == nil {
		mempool.Pool.Put(proc.Mp)
		register.FreeRegisters(proc)
		reg.Wg.Add(1)
		reg.Ch <- nil
		reg.Wg.Wait()
		return true, nil
	}
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		reg.Wg.Add(1)
		reg.Ch <- bat
		reg.Wg.Wait()
		return false, nil
	}
	size := int64(0)
	vecs := n.vecs[:0]
	for i := range bat.Vecs {
		if bat.Vecs[i].Or {
			vec, err := bat.Vecs[i].Dup(n.Proc)
			if err != nil {
				clean(vecs, n.Proc)
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
	reg.Ch <- bat
	n.Proc.Alloc(size)
	proc.Gm.Free(size)
	reg.Wg.Wait()
	return false, nil
}

func clean(vecs []*vector.Vector, proc *process.Process) {
	for _, vec := range vecs {
		vec.Clean(proc)
	}
}
