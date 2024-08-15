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

package function

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/assertx"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type fEvalFn func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error

type FunctionTestCase struct {
	proc       *process.Process
	parameters []*vector.Vector
	result     vector.FunctionResultWrapper
	expected   FunctionTestResult
	fn         fEvalFn
	fnLength   int
}

// FunctionTestInput
// the values should fit to typ.
// for example:
// if typ is int64, the values should be []int64
// if your typ is string type (varchar or others), the values should be []string.
type FunctionTestInput struct {
	typ      types.Type
	values   any
	nullList []bool
	isConst  bool
}

// FunctionTestResult
// the wanted should fit to typ.
// for example:
// if typ is int64, the wanted should be []int64
// if your typ is string type (varchar or others), the wanted should be []string.
type FunctionTestResult struct {
	typ      types.Type
	wantErr  bool
	wanted   any
	nullList []bool
}

func NewFunctionTestInput(typ types.Type, values any, nullList []bool) FunctionTestInput {
	return FunctionTestInput{
		typ:      typ,
		values:   values,
		nullList: nullList,
	}
}

func NewFunctionTestConstInput(typ types.Type, values any, nullList []bool) FunctionTestInput {
	return FunctionTestInput{
		typ:      typ,
		values:   values,
		nullList: nullList,
		isConst:  true,
	}
}

func NewFunctionTestResult(typ types.Type, wantErr bool, wanted any, nullList []bool) FunctionTestResult {
	return FunctionTestResult{
		typ:      typ,
		wantErr:  wantErr,
		wanted:   wanted,
		nullList: nullList,
	}
}

// NewFunctionTestCase generate a testcase for built-in function F.
// fn is the evaluate method of F.
func NewFunctionTestCase(
	proc *process.Process,
	inputs []FunctionTestInput,
	wanted FunctionTestResult,
	fn fEvalFn) FunctionTestCase {
	f := FunctionTestCase{proc: proc}
	mp := proc.Mp()
	// allocate vector for function parameters
	f.parameters = make([]*vector.Vector, len(inputs))
	for i := range f.parameters {
		typ := inputs[i].typ
		// generate the nulls.
		var nsp *nulls.Nulls = nil
		if len(inputs[i].nullList) != 0 {
			nsp = nulls.NewWithSize(len(inputs[i].nullList))
			for j, b := range inputs[i].nullList {
				if b {
					nsp.Set(uint64(j))
				}
			}
		}
		// new the vector.
		f.parameters[i] = newVectorByType(proc.Mp(), typ, inputs[i].values, nsp)
		if inputs[i].isConst {
			f.parameters[i].SetClass(vector.CONSTANT)
		}
	}
	// new the result
	f.result = vector.NewFunctionResultWrapper(proc.GetVector, proc.PutVector, wanted.typ, mp)
	if len(f.parameters) == 0 {
		f.fnLength = 1
	} else {
		f.fnLength = f.parameters[0].Length()
	}
	f.expected = wanted
	f.fn = fn
	return f
}

func (fc *FunctionTestCase) GetResultVectorDirectly() *vector.Vector {
	return fc.result.GetResultVector()
}

// Run will run the function case and do the correctness check for result.
func (fc *FunctionTestCase) Run() (succeed bool, errInfo string) {
	err := fc.result.PreExtendAndReset(fc.fnLength)
	if err != nil {
		panic(err)
	}

	err = fc.fn(fc.parameters, fc.result, fc.proc, fc.fnLength, nil)
	if err != nil {
		if fc.expected.wantErr {
			return true, ""
		}
		return false, fmt.Sprintf("expected to run success, but get an error that '%s'",
			err.Error())
	}
	if fc.expected.wantErr {
		return false, "expected to run failed, but run succeed with no error"
	}
	v := fc.result.GetResultVector()
	// check the length
	if fc.fnLength != v.Length() {
		return false, fmt.Sprintf("expected %d rows but get %d rows", fc.fnLength, v.Length())
	}
	// check type (it's stupid, haha
	if v.GetType().Oid != fc.expected.typ.Oid {
		return false, fmt.Sprintf("expected result type %s but get type %s", fc.expected.typ,
			v.GetType())
	}
	// generate the expected nsp
	var expectedNsp *nulls.Nulls = nil
	if fc.expected.nullList != nil {
		expectedNsp = nulls.NewWithSize(len(fc.expected.nullList))
		for i, b := range fc.expected.nullList {
			if b {
				expectedNsp.Add(uint64(i))
			}
		}
	}
	// check the value
	col := fc.expected.wanted
	vExpected := newVectorByType(fc.proc.Mp(), fc.expected.typ, col, expectedNsp)
	var i uint64
	switch v.GetType().Oid {
	case types.T_bool:
		r := vector.GenerateFunctionFixedTypeParameter[bool](v)
		s := vector.GenerateFunctionFixedTypeParameter[bool](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_bit:
		r := vector.GenerateFunctionFixedTypeParameter[uint64](v)
		s := vector.GenerateFunctionFixedTypeParameter[uint64](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_int8:
		r := vector.GenerateFunctionFixedTypeParameter[int8](v)
		s := vector.GenerateFunctionFixedTypeParameter[int8](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_int16:
		r := vector.GenerateFunctionFixedTypeParameter[int16](v)
		s := vector.GenerateFunctionFixedTypeParameter[int16](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_int32:
		r := vector.GenerateFunctionFixedTypeParameter[int32](v)
		s := vector.GenerateFunctionFixedTypeParameter[int32](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_int64:
		r := vector.GenerateFunctionFixedTypeParameter[int64](v)
		s := vector.GenerateFunctionFixedTypeParameter[int64](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_uint8:
		r := vector.GenerateFunctionFixedTypeParameter[uint8](v)
		s := vector.GenerateFunctionFixedTypeParameter[uint8](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_uint16:
		r := vector.GenerateFunctionFixedTypeParameter[uint16](v)
		s := vector.GenerateFunctionFixedTypeParameter[uint16](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_uint32:
		r := vector.GenerateFunctionFixedTypeParameter[uint32](v)
		s := vector.GenerateFunctionFixedTypeParameter[uint32](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_uint64:
		r := vector.GenerateFunctionFixedTypeParameter[uint64](v)
		s := vector.GenerateFunctionFixedTypeParameter[uint64](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_float32:
		r := vector.GenerateFunctionFixedTypeParameter[float32](v)
		s := vector.GenerateFunctionFixedTypeParameter[float32](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_float64:
		r := vector.GenerateFunctionFixedTypeParameter[float64](v)
		s := vector.GenerateFunctionFixedTypeParameter[float64](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if !assertx.InEpsilonF64(want, get) {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_decimal64:
		r := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](v)
		s := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_decimal128:
		r := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](v)
		s := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_date:
		r := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
		s := vector.GenerateFunctionFixedTypeParameter[types.Date](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_datetime:
		r := vector.GenerateFunctionFixedTypeParameter[types.Datetime](v)
		s := vector.GenerateFunctionFixedTypeParameter[types.Datetime](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_time:
		r := vector.GenerateFunctionFixedTypeParameter[types.Time](v)
		s := vector.GenerateFunctionFixedTypeParameter[types.Time](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_timestamp:
		r := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](v)
		s := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_enum:
		r := vector.GenerateFunctionFixedTypeParameter[types.Enum](v)
		s := vector.GenerateFunctionFixedTypeParameter[types.Enum](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		r := vector.GenerateFunctionStrParameter(v)
		s := vector.GenerateFunctionStrParameter(vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetStrValue(i)
			get, null2 := r.GetStrValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %s, but get NULL", i+1, string(want))
			}
			if string(want) != string(get) {
				return false, fmt.Sprintf("the %dth row expected %s, but get %s",
					i+1, string(want), string(get))
			}
		}

	case types.T_array_float32:
		r := vector.GenerateFunctionStrParameter(v)
		s := vector.GenerateFunctionStrParameter(vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetStrValue(i)
			get, null2 := r.GetStrValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %s, but get NULL", i+1, string(want))
			}
			if moarray.Compare[float32](types.BytesToArray[float32](want), types.BytesToArray[float32](get)) != 0 {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, types.BytesToArray[float32](want), types.BytesToArray[float32](get))
			}
		}
	case types.T_array_float64:
		r := vector.GenerateFunctionStrParameter(v)
		s := vector.GenerateFunctionStrParameter(vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetStrValue(i)
			get, null2 := r.GetStrValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %s, but get NULL", i+1, string(want))
			}
			if !assertx.InEpsilonF64Slice(types.BytesToArray[float64](want), types.BytesToArray[float64](get)) {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, types.BytesToArray[float64](want), types.BytesToArray[float64](get))
			}
		}
	case types.T_uuid:
		r := vector.GenerateFunctionFixedTypeParameter[types.Uuid](v)
		s := vector.GenerateFunctionFixedTypeParameter[types.Uuid](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_TS:
		r := vector.GenerateFunctionFixedTypeParameter[types.TS](v)
		s := vector.GenerateFunctionFixedTypeParameter[types.TS](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_Rowid:
		r := vector.GenerateFunctionFixedTypeParameter[types.Rowid](v)
		s := vector.GenerateFunctionFixedTypeParameter[types.Rowid](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_Blockid:
		r := vector.GenerateFunctionFixedTypeParameter[types.Blockid](v)
		s := vector.GenerateFunctionFixedTypeParameter[types.Blockid](vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetValue(i)
			get, null2 := r.GetValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if want != get {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v",
					i+1, want, get)
			}
		}
	case types.T_json:
		r := vector.GenerateFunctionStrParameter(v)
		s := vector.GenerateFunctionStrParameter(vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetStrValue(i)
			get, null2 := r.GetStrValue(i)
			if null1 {
				if null2 {
					continue
				} else {
					return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
				}
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if string(want) != string(get) {
				return false, fmt.Sprintf("the %dth row expected %s, but get %s",
					i+1, string(want), string(get))
			}
		}
	default:
		panic(fmt.Sprintf("unsupported result type %s for function ut framework", v.GetType()))
	}
	return true, ""
}

// DebugRun will not run the compare logic for function result but return the result vector directly.
func (fc *FunctionTestCase) DebugRun() (*vector.Vector, error) {
	err := fc.fn(fc.parameters, fc.result, fc.proc, fc.fnLength, nil)
	return fc.result.GetResultVector(), err
}

// BenchMarkRun will run the function case N times without correctness check for result.
func (fc *FunctionTestCase) BenchMarkRun() error {
	num := 100000
	for num > 0 {
		num--
		err := fc.fn(fc.parameters, fc.result, fc.proc, fc.fnLength, nil)
		// XXX maybe free is unnecessary.
		typ := fc.result.GetResultVector().GetType()
		fc.result.GetResultVector().Reset(*typ)
		if err != nil {
			return err
		}
	}
	return nil
}

func newVectorByType(mp *mpool.MPool, typ types.Type, val any, nsp *nulls.Nulls) *vector.Vector {
	vec := vector.NewVec(typ)
	switch typ.Oid {
	case types.T_bool:
		values := val.([]bool)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_bit:
		values := val.([]uint64)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_int8:
		values := val.([]int8)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_int16:
		values := val.([]int16)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_int32:
		values := val.([]int32)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_int64:
		values := val.([]int64)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_uint8:
		values := val.([]uint8)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_uint16:
		values := val.([]uint16)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_uint32:
		values := val.([]uint32)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_uint64:
		values := val.([]uint64)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_float32:
		values := val.([]float32)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_float64:
		values := val.([]float64)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_decimal64:
		values := val.([]types.Decimal64)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_decimal128:
		values := val.([]types.Decimal128)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_date:
		values := val.([]types.Date)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_datetime:
		values := val.([]types.Datetime)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_time:
		values := val.([]types.Time)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_timestamp:
		values := val.([]types.Timestamp)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		values := val.([]string)
		vector.AppendStringList(vec, values, nil, mp)
	case types.T_array_float32:
		values := val.([][]float32)
		vector.AppendArrayList[float32](vec, values, nil, mp)
	case types.T_array_float64:
		values := val.([][]float64)
		vector.AppendArrayList[float64](vec, values, nil, mp)
	case types.T_uuid:
		values := val.([]types.Uuid)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_TS:
		values := val.([]types.TS)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_Rowid:
		values := val.([]types.Rowid)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_Blockid:
		values := val.([]types.Blockid)
		vector.AppendFixedList(vec, values, nil, mp)
	case types.T_json:
		values := val.([]string)
		vector.AppendStringList(vec, values, nil, mp)
	case types.T_enum:
		values := val.([]types.Enum)
		vector.AppendFixedList(vec, values, nil, mp)
	default:
		panic(fmt.Sprintf("function test framework do not support typ %s", typ))
	}
	vec.SetNulls(nsp)
	return vec
}
