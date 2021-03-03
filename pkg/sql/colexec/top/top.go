package top

import (
	"container/heap"
	"matrixbase/pkg/compare"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/register"
)

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(Argument)
	data, err := proc.Alloc(n.Limit * 8)
	if err != nil {
		return err
	}
	sels := encoding.DecodeInt64Slice(data)
	for i := int64(0); i < n.Limit; i++ {
		sels[i] = i
	}
	n.Ctr.n = len(n.Fs)
	n.Ctr.sels = sels
	n.Ctr.selsData = data
	n.Ctr.vecs = make([]*vector.Vector, len(n.Fs))
	n.Ctr.cmps = make([]compare.Compare, len(n.Fs))
	for i, f := range n.Fs {
		n.Ctr.cmps[i] = compare.New(f.Oid, f.Type == Descending)
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	var err error

	n := arg.(Argument)
	bat := proc.Reg.Ax.(*batch.Batch)
	for i, f := range n.Fs {
		n.Ctr.vecs[i], err = bat.GetVector(f.Attr, proc)
		if err != nil {
			for j := 0; j < i; j++ {
				n.Ctr.vecs[i].Free(proc)
			}
			return false, err
		}
	}
	processBatch(bat, n)
	data, err := proc.Alloc(int64(len(n.Ctr.sels)) * 8)
	if err != nil {
		for _, vec := range n.Ctr.vecs {
			vec.Free(proc)
		}
		proc.Free(n.Ctr.selsData)
		return false, err
	}
	sels := encoding.DecodeInt64Slice(data)
	for i, j := 0, len(n.Ctr.sels); i < j; i++ {
		sels[len(sels)-1-i] = heap.Pop(&n.Ctr).(int64)
	}
	if len(bat.Sels) > 0 {
		proc.Free(bat.SelsData)
	}
	bat.Sels = sels
	bat.SelsData = data
	proc.Reg.Ax = bat
	register.FreeRegisters(proc)
	return false, nil
}

func processBatch(bat *batch.Batch, n Argument) {
	if length := int64(len(bat.Sels)); length > 0 {
		if length < n.Limit {
			for i := int64(0); i < length; i++ {
				n.Ctr.sels[i] = bat.Sels[i]
			}
			n.Ctr.sels = n.Ctr.sels[:length]
			heap.Init(&n.Ctr)
			return
		}
		for i := int64(0); i < n.Limit; i++ {
			n.Ctr.sels[i] = bat.Sels[i]
		}
		heap.Init(&n.Ctr)
		for i, j := n.Limit, length; i < j; i++ {
			if n.Ctr.compare(bat.Sels[i], n.Ctr.sels[0]) < 0 {
				n.Ctr.sels[0] = bat.Sels[i]
			}
			heap.Fix(&n.Ctr, 0)
		}
		return
	}
	length := int64(n.Ctr.vecs[0].Length())
	if length < n.Limit {
		n.Ctr.sels = n.Ctr.sels[:length]
		heap.Init(&n.Ctr)
		return
	}
	heap.Init(&n.Ctr)
	for i, j := n.Limit, length; i < j; i++ {
		if n.Ctr.compare(i, n.Ctr.sels[0]) < 0 {
			n.Ctr.sels[0] = i
		}
		heap.Fix(&n.Ctr, 0)
	}
}
