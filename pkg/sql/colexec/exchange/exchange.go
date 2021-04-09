package exchange

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
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
	buf.WriteString("≺(")
	for i, attr := range n.Attrs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(attr)
	}
	buf.WriteString(")")
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Ctr = Container{
		diffs:  make([]bool, UnitLimit),
		sizes:  make([]int64, len(n.Ws)),
		matchs: make([]int64, UnitLimit),
		hashs:  make([]uint64, UnitLimit),
		sels:   make([][]int64, UnitLimit),
		bats:   make([]*batch.Batch, len(n.Ws)),
		slots:  fastmap.Pool.Get().(*fastmap.Map),
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	if proc.Reg.Ax == nil {
		ctr.clean(nil, proc)
		for _, reg := range n.Ws {
			if reg.Ch != nil {
				reg.Wg.Add(1)
				reg.Ch <- nil
				reg.Wg.Wait()
			}
		}
		return false, nil
	}
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat.Attrs == nil {
		return false, nil
	}
	bat.Reorder(n.Attrs)
	if err := bat.Prefetch(n.Attrs, bat.Vecs[:len(n.Attrs)], proc); err != nil {
		ctr.clean(bat, proc)
		return false, nil
	}
	if len(bat.Sels) > 0 {
		if err := ctr.batchExchangeSels(bat.Sels, bat, bat.Vecs[:len(n.Attrs)], proc); err != nil {
			ctr.clean(bat, proc)
			return false, nil
		}
	} else {
		if err := ctr.batchExchange(bat, bat.Vecs[:len(n.Attrs)], proc); err != nil {
			ctr.clean(bat, proc)
			return false, nil
		}
	}
	bat.Clean(proc)
	for i, reg := range n.Ws {
		if ctr.bats[i] != nil {
			if reg.Ch == nil {
				continue
			}
			reg.Wg.Add(1)
			reg.Ch <- ctr.bats[i]
			n.Ms[i].Alloc(ctr.sizes[i])
			proc.Gm.Free(ctr.sizes[i])
			reg.Wg.Wait()
			ctr.sizes[i] = 0
			ctr.bats[i] = nil
		}
	}
	return false, nil
}

func (ctr *Container) batchExchange(bat *batch.Batch, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, vecs[0].Length(); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.unitExchange(i, length, nil, bat, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) batchExchangeSels(sels []int64, bat *batch.Batch, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, len(sels); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.unitExchange(0, length, sels[i:i+length], bat, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) unitExchange(start int, count int, sels []int64, bat *batch.Batch, vecs []*vector.Vector, proc *process.Process) error {
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
			idx := h % uint64(len(ctr.bats))
			if ctr.bats[idx] == nil {
				ctr.bats[idx] = batch.New(true, bat.Attrs)
				for i, vec := range bat.Vecs {
					ctr.bats[idx].Vecs[i] = vector.New(vec.Typ)
				}
			}
			size := proc.Size()
			for matched := ctr.sels[ctr.slots.Vs[i][j]]; len(matched) > 0; matched = matched[1:] {
				for i, vec := range ctr.bats[idx].Vecs {
					if vec.Data == nil {
						if err := vec.UnionOne(bat.Vecs[i], matched[0], proc); err != nil {
							return err
						}
						copy(vec.Data[:mempool.CountSize], bat.Vecs[i].Data[:mempool.CountSize])
					} else {
						if err := vec.UnionOne(bat.Vecs[i], matched[0], proc); err != nil {
							return err
						}
					}
				}
			}
			ctr.sizes[idx] += proc.Size() - size
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
	fastmap.Pool.Put(ctr.slots)
	for _, bat := range ctr.bats {
		if bat != nil {
			bat.Clean(proc)
		}
	}
}
