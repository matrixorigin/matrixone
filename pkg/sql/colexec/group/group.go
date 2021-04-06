package group

import (
	"bytes"
	"fmt"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/hash"
	"matrixbase/pkg/intmap/fastmap"
	"matrixbase/pkg/sql/colexec/aggregation"
	"matrixbase/pkg/sql/colexec/aggregation/aggfunc"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/process"
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
	buf.WriteString("γ([")
	for i, g := range n.Gs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(g)
	}
	buf.WriteString("], [")
	for i, e := range n.Es {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%s(%s) -> %s", aggregation.AggName[e.Op], e.Name, e.Alias))
	}
	buf.WriteString("]")
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	is := make([]int, len(n.Es))
	attrs := make([]string, 0, len(n.Gs)+len(n.Es))
	rattrs := make([]string, len(n.Gs)+len(n.Es))
	{
		mp := make(map[string]int)
		for i, g := range n.Gs {
			rattrs[i] = g
			if _, ok := mp[g]; !ok {
				mp[g] = len(attrs)
				attrs = append(attrs, g)
			}
		}
		for i, e := range n.Es {
			rattrs[i+len(n.Gs)] = e.Alias
			if _, ok := mp[e.Name]; !ok {
				mp[e.Name] = len(attrs)
				attrs = append(attrs, e.Name)
			}
			is[i] = mp[e.Name]
		}
	}
	n.Ctr = Container{
		is:     is,
		attrs:  attrs,
		rattrs: rattrs,
		diffs:  make([]bool, UnitLimit),
		matchs: make([]int64, UnitLimit),
		hashs:  make([]uint64, UnitLimit),
		sels:   make([][]int64, UnitLimit),
		vecs:   make([]*vector.Vector, len(n.Gs)),
		slots:  fastmap.Pool.Get().(*fastmap.Map),
	}
	return nil
}

// slow version
func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	if proc.Reg.Ax == nil {
		ctr.clean(nil, proc)
		return false, nil
	}
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat.Attrs == nil {
		return false, nil
	}
	bat.Reorder(ctr.attrs)
	if err := bat.Prefetch(ctr.attrs, bat.Vecs[:len(ctr.attrs)], proc); err != nil {
		ctr.clean(bat, proc)
		return false, err
	}
	{
		ctr.groups = make(map[uint64][]*hash.Group)
		if ctr.vecs[0] == nil {
			for i := range ctr.vecs {
				ctr.vecs[i] = vector.New(bat.Vecs[i].Typ)
			}
		}
		if n.Es[0].Agg == nil {
			for i, e := range n.Es {
				if e.Agg == nil {
					switch e.Op {
					case aggregation.Avg:
						e.Agg = aggfunc.NewAvg(bat.Vecs[ctr.is[i]].Typ)
					case aggregation.Max:
						e.Agg = aggfunc.NewMax(bat.Vecs[ctr.is[i]].Typ)
					case aggregation.Min:
						e.Agg = aggfunc.NewMin(bat.Vecs[ctr.is[i]].Typ)
					case aggregation.Sum:
						e.Agg = aggfunc.NewSum(bat.Vecs[ctr.is[i]].Typ)
					case aggregation.Count:
						e.Agg = aggfunc.NewCount(bat.Vecs[ctr.is[i]].Typ)
					case aggregation.StarCount:
						e.Agg = aggfunc.NewStarCount(bat.Vecs[ctr.is[i]].Typ)
					case aggregation.SumCount:
						e.Agg = aggfunc.NewSumCount(bat.Vecs[ctr.is[i]].Typ)
					default:
						ctr.clean(bat, proc)
						return false, fmt.Errorf("unsupport aggregation operator '%v'", e.Op)
					}
					if e.Agg == nil {
						ctr.clean(bat, proc)
						return false, fmt.Errorf("unsupport sumcount aggregation operator '%v' for %s", e.Op, bat.Vecs[i+len(n.Gs)].Typ)
					}
					n.Es[i].Agg = e.Agg
				}
			}
		}
	}
	if len(bat.Sels) > 0 {
		if err := ctr.batchGroupSels(bat.Sels, bat.Vecs[:len(ctr.attrs)], n.Es, proc); err != nil {
			ctr.clean(bat, proc)
			return false, err
		}
	} else {
		if err := ctr.batchGroup(bat.Vecs[:len(ctr.attrs)], n.Es, proc); err != nil {
			ctr.clean(bat, proc)
			return false, err
		}
	}
	bat.Clean(proc)
	vecs, err := ctr.eval(ctr.vecs[0].Length(), n.Es, proc)
	if err != nil {
		ctr.clean(nil, proc)
		return false, err
	}
	proc.Reg.Ax = &batch.Batch{
		Ro:    true,
		Attrs: ctr.rattrs,
		Vecs:  append(ctr.vecs, vecs...),
	}
	{
		for i := range ctr.vecs {
			ctr.vecs[i] = nil
		}
	}
	return false, nil
}

func (ctr *Container) eval(length int, es []aggregation.Extend, proc *process.Process) ([]*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(es))
	for i, e := range es {
		typ := e.Agg.Type()
		vecs[i] = vector.New(typ)
		switch typ.Oid {
		case types.T_int8:
			data, err := proc.Alloc(int64(length))
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeInt8Slice(data[mempool.CountSize : mempool.CountSize+length])
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(int8)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_int16:
			data, err := proc.Alloc(int64(length * 2))
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeInt16Slice(data[mempool.CountSize : mempool.CountSize+length*2])
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(int16)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_int32:
			data, err := proc.Alloc(int64(length * 4))
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeInt32Slice(data[mempool.CountSize : mempool.CountSize+length*4])
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(int32)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_int64:
			data, err := proc.Alloc(int64(length * 8))
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeInt64Slice(data[mempool.CountSize : mempool.CountSize+length*8])
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(int64)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_uint8:
			data, err := proc.Alloc(int64(length))
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeUint8Slice(data[mempool.CountSize : mempool.CountSize+length])
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(uint8)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_uint16:
			data, err := proc.Alloc(int64(length * 2))
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeUint16Slice(data[mempool.CountSize : mempool.CountSize+length*2])
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(uint16)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_uint32:
			data, err := proc.Alloc(int64(length * 4))
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeUint32Slice(data[mempool.CountSize : mempool.CountSize+length*4])
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(uint32)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_uint64:
			data, err := proc.Alloc(int64(length * 8))
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeUint64Slice(data[mempool.CountSize : mempool.CountSize+length*8])
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(uint64)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_float32:
			data, err := proc.Alloc(int64(length * 4))
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeFloat32Slice(data[mempool.CountSize : mempool.CountSize+length*4])
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(float32)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_float64:
			data, err := proc.Alloc(int64(length * 8))
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeFloat64Slice(data[mempool.CountSize : mempool.CountSize+length*8])
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(float64)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_varchar:
			size := 0
			vs := make([][]byte, length)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.([]byte)
						size += len(vs[g.Sel])
					}
				}
			}
			data, err := proc.Alloc(int64(size))
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			col := vecs[i].Col.(*types.Bytes)
			{
				o := uint32(0)
				col.Offsets = make([]uint32, 0, length)
				col.Lengths = make([]uint32, 0, length)
				col.Data = data[mempool.CountSize:mempool.CountSize]
				for _, v := range vs {
					col.Offsets = append(col.Offsets, o)
					col.Lengths = append(col.Lengths, uint32(len(v)))
					o += uint32(len(v))
					col.Data = append(col.Data, v...)
				}
			}
			vecs[i].Col = col
			vecs[i].Data = data
		case types.T_tuple:
			vs := make([][]interface{}, length)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.([]interface{})
					}
				}
			}
			vecs[i].Col = vs
		}
	}
	for i, e := range es {
		copy(vecs[i].Data, encoding.EncodeUint64(proc.Refer[e.Alias]))
	}
	return vecs, nil
}

func (ctr *Container) batchGroup(vecs []*vector.Vector, es []aggregation.Extend, proc *process.Process) error {
	for i, j := 0, vecs[0].Length(); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.unitGroup(i, length, nil, vecs, es, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) batchGroupSels(sels []int64, vecs []*vector.Vector, es []aggregation.Extend, proc *process.Process) error {
	for i, j := 0, len(sels); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.unitGroup(0, length, sels[i:i+length], vecs, es, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) unitGroup(start int, count int, sels []int64, vecs []*vector.Vector, es []aggregation.Extend, proc *process.Process) error {
	var err error

	{
		copy(ctr.hashs[:count], OneUint64s[:count])
		if len(sels) == 0 {
			ctr.fillHash(start, count, vecs[:len(ctr.vecs)])
		} else {
			ctr.fillHashSels(count, sels, vecs[:len(ctr.vecs)])
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
				g := hash.NewGroup(int64(ctr.vecs[0].Length()), ctr.is, es)
				for i, vec := range ctr.vecs {
					if vec.Data == nil {
						vec.UnionOne(vecs[i], remaining[0], proc)
						copy(vec.Data[:mempool.CountSize], vecs[i].Data[:mempool.CountSize])
					} else {
						vec.UnionOne(vecs[i], remaining[0], proc)
					}
				}
				ctr.groups[h] = append(ctr.groups[h], g)
				if remaining, err = g.Fill(remaining, ctr.matchs, vecs, ctr.vecs, ctr.diffs, proc); err != nil {
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
	fastmap.Pool.Put(ctr.slots)
	for _, vec := range ctr.vecs {
		if vec != nil {
			vec.Free(proc)
		}
	}
}
