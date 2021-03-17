package hashjoin

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/hash"
	"matrixbase/pkg/vm/process"
)

func Prepare(proc *process.Process, arg interface{}) error {
	return nil
}

// R ⨝ S, S is the small relation
func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(Argument)
	ctr := &n.Ctr
	if err := ctr.build(n.Sattrs, n.Distinct, proc); err != nil {
		return false, err
	}
	return false, nil
}

func (ctr *Container) build(attrs []string, distinct bool, proc *process.Process) error {
	var err error

	ch := proc.Reg.Cs[1]
	for {
		v := <-ch
		if v == nil {
			break
		}
		bat := v.(*batch.Batch)
		bat.Reorder(attrs)
		if err = bat.Prefetch(attrs, bat.Vecs, proc); err != nil {
			return err
		}
		ctr.bats = append(ctr.bats, bat)
		if len(bat.Sels) == 0 {
			if err = ctr.fillBatch(distinct, bat.Vecs[:len(attrs)], proc); err != nil {
				return err
			}
		} else {
			if err = ctr.fillBatchSels(distinct, bat.Sels, bat.Vecs[:len(attrs)], proc); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *Container) fillBatch(distinct bool, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, vecs[0].Length(); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.fillUnit(distinct, i, length, nil, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) fillBatchSels(distinct bool, sels []int64, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, len(sels); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.fillUnit(distinct, 0, length, sels[i:i+length], vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) fillUnit(distinct bool, start, count int, sels []int64,
	vecs []*vector.Vector, proc *process.Process) error {
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
					if remaining, err = g.Fill(distinct, remaining, ctr.matchs, vecs, ctr.bats, ctr.diffs, proc); err != nil {
						return err
					}
					copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
			} else {
				ctr.groups[h] = make([]*hash.JoinGroup, 0, 8)
			}
			for len(remaining) > 0 {
				g := hash.NewJoinGroup(int64(len(ctr.bats)-1), int64(remaining[0]))
				ctr.groups[h] = append(ctr.groups[h], g)
				if remaining, err = g.Fill(distinct, remaining, ctr.matchs, vecs, ctr.bats, ctr.diffs, proc); err != nil {
					return err
				}
				copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
			}
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
	ctr.hashs = ctr.hashs[:count]
	for _, vec := range vecs {
		hash.RehashSels(count, sels, ctr.hashs, vec)
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
