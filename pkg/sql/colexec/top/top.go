package top

import (
	"bytes"
	"container/heap"
	"fmt"
	"matrixbase/pkg/compare"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/register"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("τ([")
	for i, f := range n.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString(fmt.Sprintf("], %v)", n.Limit))
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	ctr := &n.Ctr
	{
		ctr.attrs = make([]string, len(n.Fs))
		for i, f := range n.Fs {
			ctr.attrs[i] = f.Attr
		}
	}
	{
		data, err := proc.Alloc(n.Limit * 8)
		if err != nil {
			return err
		}
		sels := encoding.DecodeInt64Slice(data[mempool.CountSize:])
		for i := int64(0); i < n.Limit; i++ {
			sels[i] = i
		}
		n.Ctr.data = data
		n.Ctr.sels = sels[:n.Limit]
	}
	n.Ctr.n = len(n.Fs)
	n.Ctr.cmps = make([]compare.Compare, len(n.Fs))
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	var err error

	if proc.Reg.Ax == nil {
		return false, nil
	}
	n := arg.(*Argument)
	ctr := &n.Ctr
	bat := proc.Reg.Ax.(*batch.Batch)
	bat.Reorder(ctr.attrs)
	{
		for i, f := range n.Fs {
			n.Ctr.cmps[i] = compare.New(bat.Vecs[i].Typ.Oid, f.Type == Descending)
		}
	}
	if err = bat.Prefetch(ctr.attrs, bat.Vecs, proc); err != nil {
		ctr.clean(bat, proc)
		return false, err
	}
	ctr.processBatch(n.Limit, bat)
	data, err := proc.Alloc(int64(len(ctr.sels) * 8))
	if err != nil {
		ctr.clean(bat, proc)
		return false, err
	}
	sels := encoding.DecodeInt64Slice(data[mempool.CountSize:])
	sels = sels[:len(ctr.sels)]
	for i, j := 0, len(ctr.sels); i < j; i++ {
		sels[len(sels)-1-i] = heap.Pop(ctr).(int64)
	}
	if len(bat.Sels) > 0 {
		proc.Free(bat.SelsData)
	}
	bat.Sels = sels
	bat.SelsData = data
	proc.Reg.Ax = bat
	ctr.clean(nil, proc)
	register.FreeRegisters(proc)
	return false, nil
}

func (ctr *Container) processBatch(limit int64, bat *batch.Batch) {
	for i, cmp := range ctr.cmps {
		cmp.Set(0, bat.Vecs[i])
		cmp.Set(1, bat.Vecs[i])
	}
	if length := int64(len(bat.Sels)); length > 0 {
		if length < limit {
			for i := int64(0); i < length; i++ {
				ctr.sels[i] = bat.Sels[i]
			}
			ctr.sels = ctr.sels[:length]
			heap.Init(ctr)
			return
		}
		for i := int64(0); i < limit; i++ {
			ctr.sels[i] = bat.Sels[i]
		}
		heap.Init(ctr)
		for i, j := limit, length; i < j; i++ {
			if ctr.compare(bat.Sels[i], ctr.sels[0]) < 0 {
				ctr.sels[0] = bat.Sels[i]
			}
			heap.Fix(ctr, 0)
		}
		return
	}
	length := int64(bat.Vecs[0].Length())
	if length < limit {
		ctr.sels = ctr.sels[:length]
		heap.Init(ctr)
		return
	}
	heap.Init(ctr)
	for i, j := limit, length; i < j; i++ {
		if ctr.compare(i, ctr.sels[0]) < 0 {
			ctr.sels[0] = i
		}
		heap.Fix(ctr, 0)
	}
}

func (ctr *Container) clean(bat *batch.Batch, proc *process.Process) {
	if bat != nil {
		bat.Clean(proc)
	}
	if ctr.data != nil {
		proc.Free(ctr.data)
		ctr.data = nil
		ctr.sels = nil
	}
	register.FreeRegisters(proc)
}
