// Copyright 2022 Matrix Origin
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

package binary

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/power"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// append cast rules for built-in function power
func init() {
	targetType := []types.Type{{Oid: types.T_float64, Size: 8}, {Oid: types.T_float64, Size: 8}}
	sourceTypes := []types.T{types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_float32, types.T_float64}

	for _, left := range sourceTypes {
		for _, right := range sourceTypes {
			overload.AppendCastRules(builtin.Power, 2, []types.T{left, right}, targetType)
			overload.AppendCastRules(builtin.Power, 2, []types.T{right, left}, targetType)
		}
	}
}

func init() {
	extend.FunctionRegistry["power"] = builtin.Power
	extend.BinaryReturnTypes[builtin.Power] = func(e extend.Extend, e2 extend.Extend) types.T {
		return types.T_float64
	}

	extend.BinaryStrings[builtin.Power] = func(e extend.Extend, e2 extend.Extend) string {
		return fmt.Sprintf("power(%s, %s)", e, e2)
	}

	overload.OpTypes[builtin.Power] = overload.Binary

	overload.BinOps[builtin.Power] = []*overload.BinOp{
		{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						power.PowerScalarLeftConst(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := process.Get(proc, 8*int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data)
					rs = rs[:len(rvs)]
					nulls.Set(vec.Nsp, rv.Nsp)
					vector.SetCol(vec, power.PowerScalarLeftConst(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						power.PowerScalarRightConst(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := process.Get(proc, 8*int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data)
					rs = rs[:len(rvs)]
					nulls.Set(vec.Nsp, rv.Nsp)
					vector.SetCol(vec, power.PowerScalarRightConst(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					power.Power(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						process.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					power.Power(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						process.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := process.Get(proc, 8*int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vector.SetCol(vec, power.Power(lvs, rvs, rs))
				if lv.Ref == 0 {
					process.Put(proc, lv)
				}
				if rv.Ref == 0 {
					process.Put(proc, rv)
				}
				return vec, nil
			},
		},
	}
}
