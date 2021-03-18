package hashgroup

import (
	"fmt"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/hash"
	"matrixbase/pkg/intmap/fastmap"
	"matrixbase/pkg/sql/colexec/aggregation"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/register"
)

func init() {
	ZeroBools = make([]bool, UnitLimit)
	OneUint64s = make([]uint64, UnitLimit)
	for i := range OneUint64s {
		OneUint64s[i] = 1
	}
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Attrs = make([]string, len(n.Es))
	for i, e := range n.Es {
		n.Attrs[i] = e.Name
	}
	n.Rattrs = make([]string, len(n.Es)+len(n.Gs))
	for i, e := range n.Es {
		n.Rattrs[i] = e.Alias
	}
	copy(n.Rattrs[len(n.Es):], n.Gs)
	n.Ctr = Container{
		diffs:  make([]bool, UnitLimit),
		hashs:  make([]uint64, UnitLimit),
		sels:   make([][]int64, UnitLimit),
		groups: make(map[uint64][]*hash.Group),
		vecs:   make([]*vector.Vector, len(n.Gs)),
		slots:  fastmap.Pool.Get().(*fastmap.Map),
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	bat := proc.Reg.Ax.(*batch.Batch)
	ctr := &n.Ctr
	gvecs := make([]*vector.Vector, len(n.Gs))
	if err := bat.Prefetch(n.Gs, gvecs, proc); err != nil {
		ctr.clean(bat, proc)
		return false, err
	}
	if len(bat.Sels) > 0 {
		if err := ctr.batchGroupSels(bat.Sels, gvecs, proc); err != nil {
			ctr.clean(bat, proc)
			return false, err
		}
	} else {
		if err := ctr.batchGroup(gvecs, proc); err != nil {
			ctr.clean(bat, proc)
			return false, err
		}
	}
	vecs := make([]*vector.Vector, len(n.Es))
	if err := bat.Prefetch(n.Attrs, vecs, proc); err != nil {
		ctr.clean(bat, proc)
		return false, err
	}
	rbat := batch.New(true, n.Rattrs)
	{
		for i := 0; i < len(n.Gs); i++ {
			rbat.Vecs[i+len(n.Es)] = ctr.vecs[i]
		}
	}
	if err := ctr.eval(ctr.vecs[0].Length(), n.Es, vecs, rbat.Vecs, proc); err != nil {
		ctr.clean(bat, proc)
		return false, err
	}
	ctr.vecs = nil
	ctr.clean(bat, proc)
	proc.Reg.Ax = rbat
	register.FreeRegisters(proc)
	return false, nil
}

func (ctr *Container) eval(length int, es []aggregation.Extend, vecs, rvecs []*vector.Vector, proc *process.Process) error {
	for i, e := range es {
		typ := aggregation.ReturnType(e.Op, e.Typ)
		switch typ {
		case types.T_int8:
			data, err := proc.Alloc(int64(length))
			if err != nil {
				return err
			}
			vec := vector.New(types.Type{types.T_int8, 1, 1, 0})
			vs := encoding.DecodeInt8Slice(data[mempool.CountSize:])
			for _, gs := range ctr.groups {
				for _, g := range gs {
					e.Agg.Reset()
					if err := e.Agg.Fill(g.Sels, vecs[i]); err != nil {
						proc.Free(data)
						return err
					}
					v, err := e.Agg.Eval(proc)
					if err != nil {
						proc.Free(data)
						return err
					}
					vs[g.Sel] = v.Col.([]int8)[0]
					if v.Nsp.Contains(0) {
						vec.Nsp.Add(uint64(g.Sel))
					}
					v.Free(proc)
				}
			}
			rvecs[i] = vec
			copy(vec.Data, encoding.EncodeUint64(1+proc.Refer[e.Alias]))
		default:
			return fmt.Errorf("unsupport type %s", typ)
		}
	}
	return nil
}

func (ctr *Container) batchGroup(vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, vecs[0].Length(); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.unitGroup(i, length, nil, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) batchGroupSels(sels []int64, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, len(sels); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.unitGroup(0, length, sels[i:i+length], vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) unitGroup(start int, count int, sels []int64, vecs []*vector.Vector, proc *process.Process) error {
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

func (ctr *Container) clean(bat *batch.Batch, proc *process.Process) {
	bat.Clean(proc)
	fastmap.Pool.Put(ctr.slots)
	for _, vec := range ctr.vecs {
		vec.Free(proc)
	}
	for _, gs := range ctr.groups {
		for _, g := range gs {
			g.Free(proc)
		}
	}
}
