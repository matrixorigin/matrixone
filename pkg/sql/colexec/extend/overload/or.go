package overload

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/or"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func init() {
	BinOps[Or] = []*BinOp{
		&BinOp{
			LeftType:   types.T_sel,
			RightType:  types.T_sel,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs)+len(rvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:or.SelOr(lvs, rvs, rs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
				}
				vec.Col = rs
				return vec, nil
			},
		},
	}
}
