package hash

import (
	"bytes"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vm/process"
)

func NewGroup(sel int64, is []int, es []aggregation.Extend) *Group {
	aggs := make([]aggregation.Aggregation, len(es))
	for i, e := range es {
		aggs[i] = e.Agg.Dup()
	}
	return &Group{
		Is:   is,
		Sel:  sel,
		Aggs: aggs,
	}
}

func (g *Group) Fill(sels, matched []int64, vecs, gvecs []*vector.Vector, diffs []bool, proc *process.Process) []int64 {
	for i, gvec := range gvecs {
		switch gvec.Typ.Oid {
		case types.T_int8:
			vec := vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]int8)
				gv := gvec.Col.([]int8)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]int8)
				gv := gvec.Col.([]int8)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_int16:
			vec := vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]int16)
				gv := gvec.Col.([]int16)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]int16)
				gv := gvec.Col.([]int16)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_int32:
			vec := vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]int32)
				gv := gvec.Col.([]int32)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]int32)
				gv := gvec.Col.([]int32)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_int64:
			vec := vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]int64)
				gv := gvec.Col.([]int64)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]int64)
				gv := gvec.Col.([]int64)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_uint8:
			vec := vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]uint8)
				gv := gvec.Col.([]uint8)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]uint8)
				gv := gvec.Col.([]uint8)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_uint16:
			vec := vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]uint16)
				gv := gvec.Col.([]uint16)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]uint16)
				gv := gvec.Col.([]uint16)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_uint32:
			vec := vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]uint32)
				gv := gvec.Col.([]uint32)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]uint32)
				gv := gvec.Col.([]uint32)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_uint64:
			vec := vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]uint64)
				gv := gvec.Col.([]uint64)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]uint64)
				gv := gvec.Col.([]uint64)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_float32:
			vec := vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]float32)
				gv := gvec.Col.([]float32)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]float32)
				gv := gvec.Col.([]float32)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_float64:
			vec := vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]float64)
				gv := gvec.Col.([]float64)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]float64)
				gv := gvec.Col.([]float64)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_decimal:
		case types.T_date:
		case types.T_datetime:
		case types.T_char, types.T_json, types.T_varchar:
			vec := vecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.(*types.Bytes)
				gvs := gvec.Col.(*types.Bytes)
				gv := gvs.Get(g.Sel)
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(sel)) != 0)
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.(*types.Bytes)
				gvs := gvec.Col.(*types.Bytes)
				gv := gvs.Get(g.Sel)
				for i, sel := range sels {
					diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(sel)) != 0)
				}
			}
		}
	}
	n := len(sels)
	matched = matched[:0]
	remaining := sels[:0]
	for i := 0; i < n; i++ {
		if diffs[i] {
			remaining = append(remaining, sels[i])
		} else {
			matched = append(matched, sels[i])
		}
	}
	if len(matched) > 0 {
		for i, agg := range g.Aggs {
			agg.Fill(matched, vecs[g.Is[i]])
		}
	}
	return remaining
}
