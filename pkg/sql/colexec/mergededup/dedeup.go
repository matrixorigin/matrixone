package mergededup

import (
	"bytes"
	"fmt"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/hash"
	"matrixbase/pkg/intmap/fastmap"
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
		groups: make(map[uint64][]*hash.DedupGroup),
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	for {
		if len(proc.Reg.Ws) == 0 {
			break
		}
		for i := 0; i < len(proc.Reg.Ws); i++ {
			reg := proc.Reg.Ws[i]
			v := <-reg.Ch
			if v == nil {
				reg.Wg.Done()
				proc.Reg.Ws = append(proc.Reg.Ws[:i], proc.Reg.Ws[i+1:]...)
				i--
				continue
			}
			bat := v.(*batch.Batch)
			if bat.Attrs == nil {
				reg.Wg.Done()
				continue
			}
			if ctr.attrs == nil {
				bat.Reorder(n.Attrs)
				ctr.attrs = make([]string, len(bat.Attrs))
				for i, attr := range bat.Attrs {
					ctr.attrs[i] = attr
				}
			} else {
				bat.Reorder(ctr.attrs)
			}
			if err := bat.Prefetch(ctr.attrs, bat.Vecs, proc); err != nil {
				reg.Wg.Done()
				ctr.clean(bat, proc)
				return true, err
			}
			if ctr.bat == nil {
				ctr.bat = batch.New(true, bat.Attrs)
				for i, vec := range bat.Vecs {
					ctr.bat.Vecs[i] = vector.New(vec.Typ)
				}
			}
			if len(bat.Sels) == 0 {
				if err := ctr.batchDedup(bat, bat.Vecs[:len(n.Attrs)], proc); err != nil {
					reg.Wg.Done()
					ctr.clean(bat, proc)
					return true, err
				}
			} else {
				if err := ctr.batchDedupSels(bat.Sels, bat, bat.Vecs[:len(n.Attrs)], proc); err != nil {
					reg.Wg.Done()
					ctr.clean(bat, proc)
					return true, err
				}
			}
			bat.Clean(proc)
			reg.Wg.Done()
		}
	}
	proc.Reg.Ax = ctr.bat
	ctr.bat = nil
	ctr.clean(nil, proc)
	return true, nil
}

func (ctr *Container) batchDedup(bat *batch.Batch, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, vecs[0].Length(); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.unitDedup(i, length, nil, bat, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) batchDedupSels(sels []int64, bat *batch.Batch, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, len(sels); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.unitDedup(0, length, sels[i:i+length], bat, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) unitDedup(start int, count int, sels []int64, bat *batch.Batch, vecs []*vector.Vector, proc *process.Process) error {
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
					if remaining, err = g.Fill(remaining, ctr.matchs, vecs, bat.Vecs, ctr.diffs, proc); err != nil {
						return err
					}
					copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
			} else {
				ctr.groups[h] = make([]*hash.DedupGroup, 0, 8)
			}
			for len(remaining) > 0 {
				g := hash.NewDedupGroup(int64(remaining[0]))
				for i, vec := range bat.Vecs {
					if ctr.bat.Vecs[i].Data == nil {
						if err := ctr.bat.Vecs[i].UnionOne(vec, remaining[0], proc); err != nil {
							return err
						}
						copy(ctr.bat.Vecs[i].Data[:mempool.CountSize], vec.Data[:mempool.CountSize])
					} else {
						if err := ctr.bat.Vecs[i].UnionOne(vec, remaining[0], proc); err != nil {
							return err
						}
					}
				}
				ctr.groups[h] = append(ctr.groups[h], g)
				remaining = remaining[1:]
				if len(remaining) > 0 {
					if remaining, err = g.Fill(remaining, ctr.matchs, vecs, bat.Vecs, ctr.diffs, proc); err != nil {
						return err
					}
					copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
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
	if ctr.bat != nil {
		ctr.bat.Clean(proc)
	}
	fastmap.Pool.Put(ctr.slots)
	register.FreeRegisters(proc)
}
