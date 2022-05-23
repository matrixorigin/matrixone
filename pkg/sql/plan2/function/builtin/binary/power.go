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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/power"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Power function's evaluation for arguments: [float64, float]
func FdsPowerFloat64Float64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, rvs := vs[0].Col.([]float64), vs[1].Col.([]float64)
	switch {
	case vs[0].IsConstant() && !vs[1].IsConstant():
		//if rv.Ref == 1 || rv.Ref == 0 {
		//	rv.Ref = 0
		//	power.PowerScalarLeftConst(lvs[0], rvs, rvs)
		//	return rv, nil
		//}
		vec, err := process.Get(proc, 8*int64(len(rvs)), vs[0].Typ)
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeFloat64Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Set(vec.Nsp, vs[1].Nsp)
		vector.SetCol(vec, power.PowerScalarLeftConst(lvs[0], rvs, rs))
		return vec, nil
	case !vs[0].IsConstant() && vs[1].IsConstant():
		//if lv.Ref == 1 || lv.Ref == 0 {
		//	lv.Ref = 0
		//	power.PowerScalarRightConst(rvs[0], lvs, lvs)
		//	return lv, nil
		//}
		vec, err := process.Get(proc, 8*int64(len(lvs)), vs[0].Typ)
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeFloat64Slice(vec.Data)
		rs = rs[:len(rvs)]
		nulls.Set(vec.Nsp, vs[1].Nsp)
		vector.SetCol(vec, power.PowerScalarRightConst(rvs[0], lvs, rs))
		return vec, nil
	}
	vec, err := process.Get(proc, 8*int64(len(lvs)), vs[0].Typ)
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFloat64Slice(vec.Data)
	rs = rs[:len(rvs)]
	nulls.Or(vs[0].Nsp, vs[1].Nsp, vec.Nsp)
	vector.SetCol(vec, power.Power(lvs, rvs, rs))
	return vec, nil
}
