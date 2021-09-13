// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
