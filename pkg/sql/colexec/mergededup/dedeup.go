package mergededup

import (
	"bytes"
	"errors"
	"fmt"
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
	buf.WriteString(fmt.Sprintf("δ(%v)", n.Attrs))
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Ctr = Container{
		n:      len(n.Attrs),
		diffs:  make([]bool, UnitLimit),
		matchs: make([]int64, UnitLimit),
		hashs:  make([]uint64, UnitLimit),
		sels:   make([][]int64, UnitLimit),
		slots:  fastmap.Pool.Get().(*fastmap.Map),
		groups: make(map[uint64][]*hash.SetGroup),
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(n, proc); err != nil {
				ctr.clean(proc)
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			ctr.bat.Reduce(n.Attrs, proc)
			proc.Reg.Ax = ctr.bat
			ctr.bat = nil
			ctr.clean(proc)
			ctr.state = End
			return true, nil
		case End:
			proc.Reg.Ax = nil
			return true, nil
		}
	}
}

func (ctr *Container) build(n *Argument, proc *process.Process) error {
	for {
		if len(proc.Reg.Ws) == 0 {
			break
		}
		for i := 0; i < len(proc.Reg.Ws); i++ {
			reg := proc.Reg.Ws[i]
			v := <-reg.Ch
			if v == nil {
				reg.Ch = nil
				reg.Wg.Done()
				proc.Reg.Ws = append(proc.Reg.Ws[:i], proc.Reg.Ws[i+1:]...)
				i--
				continue
			}
			bat := v.(*batch.Batch)
			if bat == nil || bat.Attrs == nil {
				reg.Wg.Done()
				continue
			}
			if ctr.bat == nil {
				bat.Reorder(n.Attrs)
			} else {
				bat.Reorder(ctr.bat.Attrs)
			}
			if err := bat.Prefetch(bat.Attrs, bat.Vecs, proc); err != nil {
				reg.Ch = nil
				reg.Wg.Done()
				bat.Clean(proc)
				return err
			}
			if ctr.bat == nil {
				ctr.bat = batch.New(true, bat.Attrs)
				for i, vec := range bat.Vecs {
					ctr.bat.Vecs[i] = vector.New(vec.Typ)
				}
			}
			if len(bat.Sels) == 0 {
				if err := ctr.buildBatch(bat.Vecs, proc); err != nil {
					reg.Ch = nil
					reg.Wg.Done()
					bat.Clean(proc)
					return err
				}
			} else {
				if err := ctr.buildBatchSels(bat.Sels, bat.Vecs, proc); err != nil {
					reg.Ch = nil
					reg.Wg.Done()
					bat.Clean(proc)
					return err
				}
			}
			reg.Wg.Done()
			bat.Clean(proc)
		}
	}
	return nil
}

func (ctr *Container) buildBatch(vecs []*vector.Vector, proc *process.Process) error {
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

func (ctr *Container) buildBatchSels(sels []int64, vecs []*vector.Vector, proc *process.Process) error {
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

func (ctr *Container) unitDedup(start int, count int, sels []int64,
	vecs []*vector.Vector, proc *process.Process) error {
	var err error

	{
		copy(ctr.hashs[:count], OneUint64s[:count])
		if len(sels) == 0 {
			ctr.fillHash(start, count, vecs[:ctr.n])
		} else {
			ctr.fillHashSels(count, sels, vecs[:ctr.n])
		}
	}
	copy(ctr.diffs[:count], ZeroBools[:count])
	for i, hs := range ctr.slots.Ks {
		for j, h := range hs {
			remaining := ctr.sels[ctr.slots.Vs[i][j]]
			if gs, ok := ctr.groups[h]; ok {
				for _, g := range gs {
					if remaining, err = g.Fill(remaining, ctr.matchs, vecs, ctr.bat.Vecs[:ctr.n], ctr.diffs, proc); err != nil {
						return err
					}
					copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
			} else {
				ctr.groups[h] = make([]*hash.SetGroup, 0, 8)
			}
			for len(remaining) > 0 {

				g := hash.NewSetGroup(ctr.rows)
				{
					for i, vec := range ctr.bat.Vecs {
						if vec.Data == nil {
							if err = vec.UnionOne(vecs[i], remaining[0], proc); err != nil {
								return err
							}
							copy(vec.Data[:mempool.CountSize], vecs[i].Data[:mempool.CountSize])
						} else {
							if err = vec.UnionOne(vecs[i], remaining[0], proc); err != nil {
								return err
							}
						}
					}
				}
				ctr.rows++
				ctr.groups[h] = append(ctr.groups[h], g)
				if remaining, err = g.Fill(remaining, ctr.matchs, vecs, ctr.bat.Vecs[:ctr.n], ctr.diffs, proc); err != nil {
					return err
				}
				copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				if proc.Size() > proc.Lim.Size {
					return errors.New("out of memory")
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
		hash.Rehash(count, ctr.hashs, vec.Window(start, start+count))
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
	for _, sel := range sels {
		h := ctr.hashs[sel]
		slot, ok := ctr.slots.Get(h)
		if !ok {
			slot = nextslot
			ctr.slots.Set(h, slot)
			nextslot++
		}
		ctr.sels[slot] = append(ctr.sels[slot], sel)
	}
}

func (ctr *Container) clean(proc *process.Process) {
	fastmap.Pool.Put(ctr.slots)
	if ctr.bat != nil {
		ctr.bat.Clean(proc)
		ctr.bat = nil
	}
	{
		for _, reg := range proc.Reg.Ws {
			if reg.Ch != nil {
				v := <-reg.Ch
				switch {
				case v == nil:
					reg.Ch = nil
					reg.Wg.Done()
				default:
					bat := v.(*batch.Batch)
					if bat == nil || bat.Attrs == nil {
						reg.Ch = nil
						reg.Wg.Done()
					} else {
						bat.Clean(proc)
						reg.Ch = nil
						reg.Wg.Done()
					}
				}
			}
		}
	}
}
