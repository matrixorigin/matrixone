package overload

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/and"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func init() {
	BinOps[And] = []*BinOp{
		&BinOp{
			LeftType:   types.T_sel,
			RightType:  types.T_sel,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				if len(lvs) >= len(rvs) && (lv.Ref == 1 || lv.Ref == 0) {
					lv.Ref = 0
					lvs = lvs[:and.SelAnd(lvs, rvs, lvs)]
					lv.Nsp = lv.Nsp.Or(rv.Nsp).Filter(lvs)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				}
				if len(rvs) >= len(lvs) && (rv.Ref == 1 || rv.Ref == 0) {
					rv.Ref = 0
					rvs = rvs[:and.SelAnd(lvs, rvs, rvs)]
					rv.Nsp = rv.Nsp.Or(lv.Nsp).Filter(rvs)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				n := len(lvs)
				if n < len(rvs) {
					n = len(rvs)
				}
				vec, err := register.Get(proc, int64(n)*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:and.SelAnd(lvs, rvs, rs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.Nsp.Filter(rs)
				return vec, nil
			},
		},
	}
}
