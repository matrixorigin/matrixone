package offset

import (
	"bytes"
	"fmt"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/register"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("offset(%v)", n.Offset))
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	bat := proc.Reg.Ax.(*batch.Batch)
	if n.Seen > n.Offset {
		proc.Reg.Ax = bat
		register.FreeRegisters(proc)
		return false, nil
	}
	if length := uint64(len(bat.Sels)); length > 0 {
		if n.Seen+length > n.Offset {
			bat.Sels = bat.Sels[n.Offset-n.Seen:]
			proc.Reg.Ax = bat
			n.Seen += length
			register.FreeRegisters(proc)
			return false, nil
		}
		n.Seen += length
		bat.Clean(proc)
		proc.Reg.Ax = batch.New(true, nil)
		register.FreeRegisters(proc)
		return false, nil
	}
	length, err := bat.Length(proc)
	if err != nil {
		clean(bat, proc)
		return false, err
	}
	if n.Seen+uint64(length) > n.Offset {
		data, sels, err := newSels(int64(n.Offset-n.Seen), int64(length)-int64(n.Offset-n.Seen), proc)
		if err != nil {
			clean(bat, proc)
			return true, err
		}
		n.Seen += uint64(length)
		bat.Sels = sels
		bat.SelsData = data
		proc.Reg.Ax = bat
		register.FreeRegisters(proc)
		return false, nil
	}
	n.Seen += uint64(length)
	bat.Clean(proc)
	proc.Reg.Ax = batch.New(true, nil)
	register.FreeRegisters(proc)
	return false, nil
}

func newSels(start, count int64, proc *process.Process) ([]byte, []int64, error) {
	data, err := proc.Alloc(count * 8)
	if err != nil {
		return nil, nil, err
	}
	sels := encoding.DecodeInt64Slice(data[mempool.CountSize:])
	for i := int64(0); i < count; i++ {
		sels[i] = start + i
	}
	return data, sels[:count], nil
}

func clean(bat *batch.Batch, proc *process.Process) {
	bat.Clean(proc)
	register.FreeRegisters(proc)
}
