// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func convTypeCheck(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) != 3 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	index := 0
	if inputs[0].Oid == types.T_any {
		for i, ov := range overloads {
			if len(ov.args) == 3 && ov.args[0] == types.T_any {
				index = i
				break
			}
		}
	}
	casts := []types.Type{inputs[0], inputs[1], inputs[2]}
	changed := false
	for i := 1; i < 3; i++ {
		switch {
		case inputs[i].Oid == types.T_uint64, inputs[i].Oid == types.T_int64:
			continue
		case inputs[i].Oid.IsInteger(), inputs[i].Oid == types.T_any, inputs[i].Oid.IsMySQLString():
			casts[i] = types.T_int64.ToType()
			changed = true
		default:
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
	}
	if changed {
		return newCheckResultWithCast(index, casts)
	}
	return newCheckResultWithSuccess(index)
}

// Binding normalizes bases without narrowing UINT64 through a signed cast.
type convBase struct {
	signed   vector.FunctionParameterWrapper[int64]
	unsigned vector.FunctionParameterWrapper[uint64]
}

func newConvBase(v *vector.Vector) (convBase, error) {
	switch v.GetType().Oid {
	case types.T_int64:
		return convBase{signed: vector.GenerateFunctionFixedTypeParameter[int64](v)}, nil
	case types.T_uint64:
		return convBase{unsigned: vector.GenerateFunctionFixedTypeParameter[uint64](v)}, nil
	case types.T_any:
		return convBase{}, nil
	default:
		return convBase{}, moerr.NewInvalidArgNoCtx("conv base type", v.GetType().Oid)
	}
}

func (b convBase) at(row uint64) (int64, bool) {
	if b.unsigned != nil {
		v, null := b.unsigned.GetValue(row)
		if null || v < 2 || v > 36 {
			return 0, false
		}
		return int64(v), true
	}
	if b.signed != nil {
		v, null := b.signed.GetValue(row)
		return v, !null && ((v >= 2 && v <= 36) || (v >= -36 && v <= -2))
	}
	return 0, false
}

type convBases struct {
	from, to           convBase
	constant           bool
	fromValue, toValue int64
	valid              bool
}

func newConvBases(from, to *vector.Vector) (convBases, error) {
	f, err := newConvBase(from)
	if err != nil {
		return convBases{}, err
	}
	t, err := newConvBase(to)
	if err != nil {
		return convBases{}, err
	}
	b := convBases{from: f, to: t}
	if from.IsConst() && to.IsConst() {
		b.fromValue, b.toValue, b.valid = b.at(0)
		b.constant = true
	}
	return b, nil
}

func (b convBases) at(row uint64) (int64, int64, bool) {
	if b.constant {
		return b.fromValue, b.toValue, b.valid
	}
	from, okFrom := b.from.at(row)
	to, okTo := b.to.at(row)
	return from, to, okFrom && okTo
}
