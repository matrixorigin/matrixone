package mergededup

import (
	"bytes"
	"fmt"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/block"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/hash"
	"matrixone/pkg/intmap/fastmap"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"

	"github.com/google/uuid"
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
		groups: make(map[uint64][]*hash.SetGroup),
	}
	ctr := &n.Ctr
	uuid, err := uuid.NewUUID()
	if err != nil {
		fastmap.Pool.Put(ctr.slots)
		return err
	}
	ctr.spill.id = fmt.Sprintf("%s.%v", proc.Id, uuid)
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	for {
		switch ctr.state {
		case Build:
			ctr.spill.e = n.E
			if err := ctr.build(n, proc); err != nil {
				ctr.clean(proc)
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			if err := ctr.eval(n, proc); err != nil {
				ctr.clean(proc)
				ctr.state = End
				return true, err
			}
			if ctr.bat == nil {
				proc.Reg.Ax = nil
				ctr.clean(proc)
				ctr.state = End
				return true, nil
			}
			proc.Reg.Ax = ctr.bat
			ctr.bat = nil
			return false, nil
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
			if ctr.spill.attrs == nil {
				bat.Reorder(n.Attrs)
				ctr.spill.attrs = make([]string, len(bat.Attrs))
				for i, attr := range bat.Attrs {
					ctr.spill.attrs[i] = attr
				}
				ctr.spill.cs = make([]uint64, len(bat.Attrs))
				ctr.spill.md = make([]metadata.Attribute, len(bat.Attrs))
				for i, attr := range bat.Attrs {
					vec, err := bat.GetVector(attr, proc)
					if err != nil {
						reg.Ch = nil
						reg.Wg.Done()
						bat.Clean(proc)
						return err
					}
					ctr.spill.md[i] = metadata.Attribute{
						Name: attr,
						Type: vec.Typ,
						Alg:  compress.Lz4,
					}
					ctr.spill.cs[i] = encoding.DecodeUint64(vec.Data[:mempool.CountSize])
				}
			} else {
				bat.Reorder(ctr.spill.attrs)
			}
			ctr.bats = append(ctr.bats, &block.Block{
				Bat:   bat,
				R:     ctr.spill.r,
				Cs:    ctr.spill.cs,
				Attrs: ctr.spill.attrs,
			})
			if err := bat.Prefetch(n.Attrs, bat.Vecs[:len(n.Attrs)], proc); err != nil {
				reg.Ch = nil
				reg.Wg.Done()
				bat.Clean(proc)
				return err
			}
			if len(bat.Sels) == 0 {
				if err := ctr.buildBatch(bat, bat.Vecs[:len(n.Attrs)], proc); err != nil {
					reg.Ch = nil
					reg.Wg.Done()
					bat.Clean(proc)
					return err
				}
			} else {
				if err := ctr.buildBatchSels(bat.Sels, bat, bat.Vecs[:len(n.Attrs)], proc); err != nil {
					reg.Ch = nil
					reg.Wg.Done()
					bat.Clean(proc)
					return err
				}
			}
			if len(bat.Sels) == 0 {
				bat.Clean(proc)
				ctr.bats = ctr.bats[:len(ctr.bats)-1]
				reg.Wg.Done()
				continue
			} else {
				blk := ctr.bats[len(ctr.bats)-1]
				blk.Bat.Sels = bat.Sels
				blk.Bat.SelsData = bat.SelsData
			}
			/*
				switch {
				case ctr.spilled:
					blk := ctr.bats[len(ctr.bats)-1]
					if err := blk.Bat.Prefetch(blk.Bat.Attrs, blk.Bat.Vecs, proc); err != nil {
						reg.Ch = nil
						reg.Wg.Done()
						return err
					}
					if err := ctr.spill.r.Write(blk.Bat); err != nil {
						reg.Ch = nil
						reg.Wg.Done()
						return err
					}
					blk.Seg = ctr.spill.r.Segments()[len(ctr.bats)-1]
					blk.Bat.Clean(proc)
					blk.Bat = nil
				case proc.Size() > proc.Lim.Size:
					if err := ctr.newSpill(proc); err != nil {
						reg.Ch = nil
						reg.Wg.Done()
						return err
					}
					for i, blk := range ctr.bats {
						if err := blk.Bat.Prefetch(blk.Bat.Attrs, blk.Bat.Vecs, proc); err != nil {
							reg.Ch = nil
							reg.Wg.Done()
							return err
						}
						if err := ctr.spill.r.Write(blk.Bat); err != nil {
							reg.Ch = nil
							reg.Wg.Done()
							return err
						}
						blk.R = ctr.spill.r
						blk.Seg = ctr.spill.r.Segments()[i]
						blk.Bat.Clean(proc)
						blk.Bat = nil
					}
					ctr.spilled = true
				}
			*/
			reg.Wg.Done()
		}
	}
	return nil
}

func (ctr *Container) eval(n *Argument, proc *process.Process) error {
	if len(ctr.bats) == 0 {
		return nil
	}
	bat, err := ctr.bats[0].GetBatch(proc)
	if err != nil {
		return err
	}
	bat.Reduce(n.Attrs, proc)
	ctr.bat = bat
	ctr.bats = ctr.bats[1:]
	return nil
}

func (ctr *Container) buildBatch(bat *batch.Batch, vecs []*vector.Vector, proc *process.Process) error {
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

func (ctr *Container) buildBatchSels(sels []int64, bat *batch.Batch, vecs []*vector.Vector, proc *process.Process) error {
	{
		data := bat.SelsData
		defer proc.Free(data)
	}
	bat.Sels = nil
	bat.SelsData = nil
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

func (ctr *Container) unitDedup(start int, count int, sels []int64,
	bat *batch.Batch, vecs []*vector.Vector, proc *process.Process) error {
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
					if remaining, err = g.Fill(remaining, ctr.matchs, vecs, ctr.bats, ctr.diffs, proc); err != nil {
						return err
					}
					copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
			} else {
				ctr.groups[h] = make([]*hash.SetGroup, 0, 8)
			}
			for len(remaining) > 0 {
				g := hash.NewSetGroup(int64(len(ctr.bats)-1), int64(remaining[0]))
				{
					if n := cap(bat.Sels); n == 0 {
						data, err := proc.Alloc(int64(8 * 8))
						if err != nil {
							return err
						}
						newsels := encoding.DecodeInt64Slice(data[mempool.CountSize : mempool.CountSize+8*8])
						bat.SelsData = data
						bat.Sels = newsels[:0]
					} else if n == len(bat.Sels) {
						if n < 1024 {
							n *= 2
						} else {
							n += n / 4
						}
						data, err := proc.Alloc(int64(n * 8))
						if err != nil {
							return err
						}
						newsels := encoding.DecodeInt64Slice(data[mempool.CountSize : mempool.CountSize+n*8])
						copy(newsels, bat.Sels)
						bat.Sels = newsels[:n]
						proc.Free(bat.SelsData)
						bat.SelsData = data
					}
				}
				bat.Sels = append(bat.Sels, remaining[0])
				ctr.groups[h] = append(ctr.groups[h], g)
				if remaining = remaining[1:]; len(remaining) > 0 {
					if remaining, err = g.Fill(remaining, ctr.matchs, vecs, ctr.bats, ctr.diffs, proc); err != nil {
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

func (ctr *Container) newSpill(proc *process.Process) error {
	var defs []engine.TableDef

	for _, attr := range ctr.spill.md {
		defs = append(defs, &engine.AttributeDef{attr})
	}
	if err := ctr.spill.e.Create(ctr.spill.id, defs, nil, nil, ""); err != nil {
		return err
	}
	r, err := ctr.spill.e.Relation(ctr.spill.id)
	if err != nil {
		ctr.spill.e.Delete(ctr.spill.id)
		return err
	}
	ctr.spill.r = r
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
	for _, blk := range ctr.bats {
		if blk.Bat != nil {
			blk.Bat.Clean(proc)
			blk.Bat = nil
		}
	}
	if ctr.spill.r != nil {
		ctr.spill.e.Delete(ctr.spill.id)
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
