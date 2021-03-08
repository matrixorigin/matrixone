package hashgroup

import (
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/hash"
	"matrixbase/pkg/vm/process"
)

func init() {
	ZeroBools = make([]bool, UnitLimit)
	OneUint64s = make([]uint64, UnitLimit)
	for i := range OneUint64s {
		OneUint64s[i] = 1
	}
}

func Prepare(proc *process.Process, arg interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	return false, nil
}

func (ctr *Container) group(count int, sels []int64, vecs []*vector.Vector, proc *process.Process) error {
	var err error

	{
		copy(ctr.hashs[:count], OneUint64s[:count])
		if len(sels) == 0 {
			ctr.fillHash(count, vecs)
		} else {
			ctr.fillHashSels(count, sels, vecs)
		}
	}
	copy(ctr.diffs[:count], ZeroBools[:count])
	for _, h := range ctr.hashs {
		slot, ok := ctr.slots[h]
		if !ok {
			continue
		}
		remaining := ctr.sels[slot]
		if gs, ok := ctr.groups[h]; ok {
			for _, g := range gs {
				if remaining, err = g.Fill(remaining, ctr.matchs, vecs, ctr.vecs, ctr.diffs, proc); err != nil {
					return err
				}
				copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
			}
		} else {
			ctr.groups[h] = make([]*hash.Group, 0, 8)
		}
		for len(remaining) > 0 {
			g := hash.NewGroup(int64(ctr.vecs[0].Length()))
			for i, vec := range vecs {
				ctr.vecs[i].UnionOne(vec, remaining[0], proc)
			}
			ctr.groups[h] = append(ctr.groups[h], g)
			if remaining, err = g.Fill(remaining, ctr.matchs, vecs, ctr.vecs, ctr.diffs, proc); err != nil {
				return err
			}
			copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])

		}
		ctr.sels[slot] = ctr.sels[slot][:0]
		delete(ctr.slots, h)
	}
	return nil
}

func (ctr *Container) fillHash(count int, vecs []*vector.Vector) {
	ctr.hashs = ctr.hashs[:count]
	for _, vec := range vecs {
		hash.Rehash(count, ctr.hashs, vec)
	}
	nextslot := 0
	for i, h := range ctr.hashs {
		slot, ok := ctr.slots[h]
		if !ok {
			slot = nextslot
			ctr.slots[h] = slot
			nextslot++
		}
		ctr.sels[slot] = append(ctr.sels[slot], int64(i))
	}
}

func (ctr *Container) fillHashSels(count int, sels []int64, vecs []*vector.Vector) {
	ctr.hashs = ctr.hashs[:count]
	for _, vec := range vecs {
		hash.RehashSels(count, sels, ctr.hashs, vec)
	}
	nextslot := 0
	for i, h := range ctr.hashs {
		slot, ok := ctr.slots[h]
		if !ok {
			slot = nextslot
			ctr.slots[h] = slot
			nextslot++
		}
		ctr.sels[slot] = append(ctr.sels[slot], sels[i])
	}
}
