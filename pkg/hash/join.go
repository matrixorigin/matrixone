package hash

import (
	"bytes"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/vm/process"
)

func NewJoin(idx, sel int64) *Join {
	return &Join{
		Idx: idx,
		Sel: sel,
	}
}

func (m *Join) Free(proc *process.Process) {
	if m.Data != nil {
		proc.Free(m.Data)
		m.Data = nil
	}
	if m.Idata != nil {
		proc.Free(m.Idata)
		m.Idata = nil
	}
}

func (m *Join) Fill(distinct bool, idx int64, sels, matched []int64, vecs []*vector.Vector, gvecs [][]*vector.Vector, diffs []bool, proc *process.Process) ([]int64, error) {
	for i, vec := range vecs {
		switch vec.Typ.Oid {
		case types.T_int8:
			gvec := gvecs[m.Idx][i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(m.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]int8)
				gv := gvec.Col.([]int8)[m.Sel]
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
				gv := gvec.Col.([]int8)[m.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_int16:
			gvec := gvecs[m.Idx][i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(m.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]int16)
				gv := gvec.Col.([]int16)[m.Sel]
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
				gv := gvec.Col.([]int16)[m.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_int32:
			gvec := gvecs[m.Idx][i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(m.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]int32)
				gv := gvec.Col.([]int32)[m.Sel]
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
				gv := gvec.Col.([]int32)[m.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_int64:
			gvec := gvecs[m.Idx][i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(m.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]int64)
				gv := gvec.Col.([]int64)[m.Sel]
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
				gv := gvec.Col.([]int64)[m.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_uint8:
			gvec := gvecs[m.Idx][i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(m.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]uint8)
				gv := gvec.Col.([]uint8)[m.Sel]
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
				gv := gvec.Col.([]uint8)[m.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_uint16:
			gvec := gvecs[m.Idx][i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(m.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]uint16)
				gv := gvec.Col.([]uint16)[m.Sel]
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
				gv := gvec.Col.([]uint16)[m.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_uint32:
			gvec := gvecs[m.Idx][i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(m.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]uint32)
				gv := gvec.Col.([]uint32)[m.Sel]
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
				gv := gvec.Col.([]uint32)[m.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_uint64:
			gvec := gvecs[m.Idx][i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(m.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]uint64)
				gv := gvec.Col.([]uint64)[m.Sel]
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
				gv := gvec.Col.([]uint64)[m.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_float32:
			gvec := gvecs[m.Idx][i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(m.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]float32)
				gv := gvec.Col.([]float32)[m.Sel]
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
				gv := gvec.Col.([]float32)[m.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_float64:
			gvec := gvecs[m.Idx][i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(m.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]float64)
				gv := gvec.Col.([]float64)[m.Sel]
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
				gv := gvec.Col.([]float64)[m.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_decimal:
		case types.T_date:
		case types.T_datetime:
		case types.T_char:
		case types.T_varchar:
			gvec := gvecs[m.Idx][i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(m.Sel))
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
				gv := gvs.Get(int(m.Sel))
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.(*types.Bytes)
				gvs := gvec.Col.(*types.Bytes)
				gv := gvs.Get(int(m.Sel))
				for i, sel := range sels {
					diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
				}
			}
		case types.T_json:
			gvec := gvecs[m.Idx][i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(m.Sel))
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
				gv := gvs.Get(int(m.Sel))
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.(*types.Bytes)
				gvs := gvec.Col.(*types.Bytes)
				gv := gvs.Get(int(m.Sel))
				for i, sel := range sels {
					diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
				}
			}
		}
	}
	n := len(sels)
	remaining := sels[:0]
	if !distinct {
		matched = matched[:0]
		for i := 0; i < n; i++ {
			if diffs[i] {
				remaining = append(remaining, sels[i])
			} else {
				matched = append(matched, sels[i])
			}
		}
		if len(matched) > 0 {
			length := len(m.Sels) + len(matched)
			if cap(m.Sels) < length {
				data, err := proc.Alloc(int64(length) * 8)
				if err != nil {
					return nil, err
				}
				idata, err := proc.Alloc(int64(length) * 8)
				if err != nil {
					proc.Free(data)
					return nil, err
				}
				copy(data, m.Data)
				copy(idata, m.Idata)
				proc.Free(m.Data)
				proc.Free(m.Idata)
				m.Is = encoding.DecodeInt64Slice(idata)
				m.Sels = encoding.DecodeInt64Slice(data)
				m.Data = data[:length]
				m.Sels = m.Sels[:length]
				m.Is = m.Is[:length]
				m.Idata = idata[:length]
			}
			for _ = range matched {
				m.Is = append(m.Is, idx)
			}
			m.Sels = append(m.Sels, matched...)
		}
	} else {
		if len(m.Sels) > 0 {
			for i := 0; i < n; i++ {
				if diffs[i] {
					remaining = append(remaining, sels[i])
				}
			}
		} else {
			matched = matched[:0]
			for i := 0; i < n; i++ {
				if diffs[i] {
					remaining = append(remaining, sels[i])
				} else if len(matched) == 0 {
					matched = append(matched, sels[i])
				}
			}
			if len(matched) > 0 && cap(m.Sels) == 0 {
				data, err := proc.Alloc(8)
				if err != nil {
					return nil, err
				}
				idata, err := proc.Alloc(8)
				if err != nil {
					proc.Free(data)
					return nil, err
				}
				m.Data = data
				m.Idata = idata
				m.Is = encoding.DecodeInt64Slice(idata)
				m.Sels = encoding.DecodeInt64Slice(data)
				m.Is = append(m.Is, idx)
				m.Sels = append(m.Sels, matched[0])
			}
		}
	}
	return remaining, nil
}
