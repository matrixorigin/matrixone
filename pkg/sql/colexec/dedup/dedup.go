package dedup

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/hash"
	"matrixone/pkg/intmap/fastmap"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func init() {
	ZeroBools = make([]bool, UnitLimit)
	OneUint64s = make([]uint64, UnitLimit)
	for i := range OneUint64s {
		OneUint64s[i] = 1
	}
}

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("δ(%v)", n.Attrs))
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Ctr = Container{
		diffs:  make([]bool, UnitLimit),
		matchs: make([]int64, UnitLimit),
		hashs:  make([]uint64, UnitLimit),
		sels:   make([][]int64, UnitLimit),
		slots:  fastmap.Pool.Get().(*fastmap.Map),
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	if proc.Reg.Ax == nil {
		ctr.clean(nil, proc)
		return false, nil
	}
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		return false, nil
	}
	ctr.groups = make(map[uint64][]*hash.DedupGroup)
	vecs := make([]*vector.Vector, len(n.Attrs))
	if err := bat.Prefetch(n.Attrs, vecs, proc); err != nil {
		ctr.clean(bat, proc)
		return false, err
	}
	if len(bat.Sels) == 0 {
		if err := ctr.batchDedup(vecs, proc); err != nil {
			ctr.clean(bat, proc)
			return false, err
		}
	} else {
		if err := ctr.batchDedupSels(bat.Sels, vecs, proc); err != nil {
			ctr.clean(bat, proc)
			return false, err
		}
		proc.Free(bat.SelsData)
	}
	proc.Reg.Ax = bat
	bat.Sels = ctr.dedupState.sels
	bat.SelsData = ctr.dedupState.data
	ctr.dedupState.sels = nil
	ctr.dedupState.data = nil
	proc.Reg.Ax = bat
	return false, nil
}

func (ctr *Container) batchDedup(vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, vecs[0].Length(); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.unitDedup(i, length, nil, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) batchDedupSels(sels []int64, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, len(sels); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.unitDedup(0, length, sels[i:i+length], vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) unitDedup(start int, count int, sels []int64, vecs []*vector.Vector, proc *process.Process) error {
	var err error

	{
		copy(ctr.hashs[:count], OneUint64s[:count])
		if len(sels) == 0 {
			ctr.fillHash(start, count, vecs)
		} else {
			ctr.fillHashSels(count, sels, vecs)
		}
	}
	copy(ctr.diffs[:count], ZeroBools[:count])
	for i, hs := range ctr.slots.Ks {
		for j, h := range hs {
			remaining := ctr.sels[ctr.slots.Vs[i][j]]
			if gs, ok := ctr.groups[h]; ok {
				for _, g := range gs {
					if remaining, err = g.Fill(remaining, ctr.matchs, vecs, vecs, ctr.diffs, proc); err != nil {
						return err
					}
					copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
			} else {
				ctr.groups[h] = make([]*hash.DedupGroup, 0, 8)
			}
			for len(remaining) > 0 {
				g := hash.NewDedupGroup(int64(remaining[0]))
				ctr.groups[h] = append(ctr.groups[h], g)
				{
					if n := len(ctr.dedupState.sels); cap(ctr.dedupState.sels) < n+1 {
						data, err := proc.Alloc(int64(n+1) * 8)
						if err != nil {
							return err
						}
						if ctr.dedupState.data != nil {
							copy(data[mempool.CountSize:], ctr.dedupState.data[mempool.CountSize:])
							proc.Free(ctr.dedupState.data)
						}
						ctr.dedupState.sels = encoding.DecodeInt64Slice(data[mempool.CountSize : mempool.CountSize+n*8])
						ctr.dedupState.data = data
						ctr.dedupState.sels = ctr.dedupState.sels[:n]
					}
					ctr.dedupState.sels = append(ctr.dedupState.sels, remaining[0])
				}
				if remaining, err = g.Fill(remaining[1:], ctr.matchs, vecs, vecs, ctr.diffs, proc); err != nil {
					return err
				}
				copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
			}
			ctr.sels[ctr.slots.Vs[i][j]] = ctr.sels[ctr.slots.Vs[i][j]][:0]
		}
	}
	ctr.slots.Reset()
	return nil
}

func (ctr *Container) fillHash(start, count int, vecs []*vector.Vector) {
	ctr.hashs = ctr.hashs[:count]
	for _, vec := range vecs {
		hash.Rehash(count, ctr.hashs, vec)
	}
	nextslot := 0
	for i, h := range ctr.hashs {
		slot, ok := ctr.slots.Get(h)
		if !ok {
			slot = nextslot
			ctr.slots.Set(h, slot)
			nextslot++
		}
		ctr.sels[slot] = append(ctr.sels[slot], int64(i+start))
	}
}

func (ctr *Container) fillHashSels(count int, sels []int64, vecs []*vector.Vector) {
	var cnt int64

	{
		for i, sel := range sels {
			if i == 0 || sel > cnt {
				cnt = sel
			}
		}
	}
	ctr.hashs = ctr.hashs[:cnt+1]
	for _, vec := range vecs {
		hash.RehashSels(sels[:count], ctr.hashs, vec)
	}
	nextslot := 0
	for i, h := range ctr.hashs {
		slot, ok := ctr.slots.Get(h)
		if !ok {
			slot = nextslot
			ctr.slots.Set(h, slot)
			nextslot++
		}
		ctr.sels[slot] = append(ctr.sels[slot], sels[i])
	}
}

func (ctr *Container) clean(bat *batch.Batch, proc *process.Process) {
	if bat != nil {
		bat.Clean(proc)
	}
	if ctr.dedupState.data != nil {
		proc.Free(ctr.dedupState.data)
	}
	fastmap.Pool.Put(ctr.slots)
}
