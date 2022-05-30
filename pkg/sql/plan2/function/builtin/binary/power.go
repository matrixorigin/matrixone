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

func Power(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vs[0], vs[1]
	lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
	switch {
	case lv.IsScalar() && rv.IsScalar():
		if lv.IsScalarNull() || rv.IsScalarNull() {
			return proc.AllocScalarNullVector(lv.Typ), nil
		}
		vec := proc.AllocScalarVector(lv.Typ)
		rs := make([]float64, 1)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, power.Power(lvs, rvs, rs))
		return vec, nil
	case lv.IsScalar() && !rv.IsScalar():
		if lv.IsScalarNull() {
			return proc.AllocScalarNullVector(lv.Typ), nil
		}
		vec, err := proc.AllocVector(lv.Typ, 8*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeFloat64Slice(vec.Data)
		rs = rs[:len(rvs)]
		// If one of the parameters of power is null, the return value is null
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, power.PowerScalarLeftConst(lvs[0], rvs, rs))
		return vec, nil
	case !lv.IsScalar() && rv.IsScalar():
		if rv.IsScalarNull() {
			return proc.AllocScalarNullVector(rv.Typ), nil
		}
		vec, err := proc.AllocVector(lv.Typ, 8*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeFloat64Slice(vec.Data)
		rs = rs[:len(rvs)]
		// If one of the parameters of power is null, the return value is null
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		vector.SetCol(vec, power.PowerScalarRightConst(rvs[0], lvs, rs))
		return vec, nil
	}
	vec, err := proc.AllocVector(lv.Typ, 8*int64(len(lvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFloat64Slice(vec.Data)
	rs = rs[:len(rvs)]
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, power.Power(lvs, rvs, rs))
	return vec, nil
}
