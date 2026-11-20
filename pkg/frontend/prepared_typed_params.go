// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// installTypedPrepareParams gives the process each bound parameter as one row
// of its source SQL type, so a ParamRef bound to that type is evaluated without
// a TEXT round trip. The values come from the TEXT transport, which holds the
// canonical rendering of a typed source (protocol value or user variable).
//
// A position is left without a typed value (nil column) when its binding is a
// string or unsupported type, or when its text does not parse exactly; the
// executor then keeps the TEXT transport for that position.
func installTypedPrepareParams(proc *process.Process, bindings []plan2.PreparedSourceBinding) error {
	params := proc.GetPrepareParams()
	if params == nil || params.Length() == 0 || len(bindings) == 0 {
		return nil
	}
	var typed *batch.Batch
	for _, binding := range bindings {
		pos := int(binding.Position)
		if pos < 0 || pos >= params.Length() || params.IsNull(uint64(pos)) {
			continue
		}
		raw, err := proc.GetPrepareParamsAt(pos)
		if err != nil {
			return err
		}
		vec, ok, err := typedPrepareParam(proc, binding.Type, string(raw))
		if err != nil {
			if typed != nil {
				typed.Clean(proc.Mp())
			}
			return err
		}
		if !ok {
			continue
		}
		if typed == nil {
			typed = batch.NewWithSize(params.Length())
		}
		typed.Vecs[pos] = vec
	}
	if typed != nil {
		typed.SetRowCount(1)
		proc.SetTypedPrepareParams(typed)
	}
	return nil
}

// typedPrepareParam parses the canonical text of a typed parameter into a
// constant one-row vector. ok is false when typ is not handled or the text is
// not an exact rendering of a typ value.
func typedPrepareParam(proc *process.Process, typ types.Type, text string) (*vector.Vector, bool, error) {
	mp := proc.Mp()
	constFixed := func(v any) (*vector.Vector, bool, error) {
		var vec *vector.Vector
		var err error
		switch x := v.(type) {
		case int8:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case int16:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case int32:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case int64:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case uint8:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case uint16:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case uint32:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case uint64:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case float32:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case float64:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case bool:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case types.Decimal64:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case types.Decimal128:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		case types.Decimal256:
			vec, err = vector.NewConstFixed(typ, x, 1, mp)
		}
		if err != nil {
			return nil, false, err
		}
		return vec, vec != nil, nil
	}
	signed := func(bits int) (int64, bool) {
		v, err := strconv.ParseInt(text, 10, bits)
		return v, err == nil
	}
	unsigned := func(bits int) (uint64, bool) {
		v, err := strconv.ParseUint(text, 10, bits)
		return v, err == nil
	}

	// typeswitch:partial a type without a case keeps the TEXT transport, which is always correct
	switch typ.Oid {
	case types.T_int8:
		if v, ok := signed(8); ok {
			return constFixed(int8(v))
		}
	case types.T_int16:
		if v, ok := signed(16); ok {
			return constFixed(int16(v))
		}
	case types.T_int32:
		if v, ok := signed(32); ok {
			return constFixed(int32(v))
		}
	case types.T_int64:
		if v, ok := signed(64); ok {
			return constFixed(v)
		}
	case types.T_uint8:
		if v, ok := unsigned(8); ok {
			return constFixed(uint8(v))
		}
	case types.T_uint16:
		if v, ok := unsigned(16); ok {
			return constFixed(uint16(v))
		}
	case types.T_uint32:
		if v, ok := unsigned(32); ok {
			return constFixed(uint32(v))
		}
	case types.T_uint64:
		if v, ok := unsigned(64); ok {
			return constFixed(v)
		}
	case types.T_float32:
		if v, err := strconv.ParseFloat(text, 32); err == nil {
			return constFixed(float32(v))
		}
	case types.T_float64:
		if v, err := strconv.ParseFloat(text, 64); err == nil {
			return constFixed(v)
		}
	case types.T_bool:
		if v, err := strconv.ParseBool(text); err == nil {
			return constFixed(v)
		}
	case types.T_decimal64:
		if v, err := types.ParseDecimal64(text, typ.Width, typ.Scale); err == nil {
			return constFixed(v)
		}
	case types.T_decimal128:
		if v, err := types.ParseDecimal128(text, typ.Width, typ.Scale); err == nil {
			return constFixed(v)
		}
	case types.T_decimal256:
		if v, err := types.ParseDecimal256(text, typ.Width, typ.Scale); err == nil {
			return constFixed(v)
		}
	}
	return nil, false, nil
}
