package limit

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("limit(%v)", n.Limit))
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	if proc.Reg.Ax == nil {
		return false, nil
	}
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat.Attrs == nil {
		return false, nil
	}
	n := arg.(*Argument)
	if n.Seen >= n.Limit {
		proc.Reg.Ax = nil
		bat.Clean(proc)
		return true, nil
	}
	if length := uint64(len(bat.Sels)); length > 0 {
		newSeen := n.Seen + length
		if newSeen >= n.Limit { // limit - seen
			bat.Sels = bat.Sels[:n.Limit-n.Seen]
			proc.Reg.Ax = bat
			n.Seen = newSeen
			register.FreeRegisters(proc)
			return true, nil
		}
		n.Seen = newSeen
		proc.Reg.Ax = bat
		register.FreeRegisters(proc)
		return false, nil
	}
	length, err := bat.Length(proc)
	if err != nil {
		clean(bat, proc)
		return false, err
	}
	newSeen := n.Seen + uint64(length)
	if newSeen >= n.Limit { // limit - seen
		data, sels, err := newSels(int64(n.Limit-n.Seen), proc)
		if err != nil {
			clean(bat, proc)
			return true, err
		}
		bat.Sels = sels
		bat.SelsData = data
		proc.Reg.Ax = bat
		n.Seen = newSeen
		register.FreeRegisters(proc)
		return true, nil
	}
	n.Seen = newSeen
	proc.Reg.Ax = bat
	register.FreeRegisters(proc)
	return false, nil
}

func newSels(count int64, proc *process.Process) ([]byte, []int64, error) {
	data, err := proc.Alloc(count * 8)
	if err != nil {
		return nil, nil, err
	}
	sels := encoding.DecodeInt64Slice(data[mempool.CountSize:])
	for i := int64(0); i < count; i++ {
		sels[i] = i
	}
	return data, sels[:count], nil
}

func clean(bat *batch.Batch, proc *process.Process) {
	bat.Clean(proc)
	register.FreeRegisters(proc)
}
