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
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/assertx"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// newMemoryFunctionTestProcess binds memory-backed dependencies to the test lifetime.
func newMemoryFunctionTestProcess(t testing.TB) *process.Process {
	t.Helper()
	fs := testutil.NewFS(nil)
	t.Cleanup(func() { fs.Close(context.Background()) })
	return testutil.NewProcess(t, testutil.WithFileService(fs))
}

// geometryComparisonWKT normalizes a geometry payload (WKB or legacy WKT/EWKT
// text) to canonical WKT so geometry test expectations written as WKT compare
// equal to WKB-encoded results.
func geometryComparisonWKT(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	var (
		g   geo.Geometry
		err error
	)
	if payloadIsWKB(b) {
		g, err = geo.ReadWKB(b)
		if err != nil {
			// float32-coordinate WKB (GEOMETRY32)
			g, err = geo.ReadWKBFloat32(b)
		}
	} else {
		s, _, _ := stripEWKTSRID(strings.TrimSpace(string(b)))
		g, err = geo.ParseWKT(s)
	}
	if err != nil {
		return string(b)
	}
	return geo.WriteWKT(g)
}

type FunctionTestCase struct {
	proc       *process.Process
	parameters []*vector.Vector
	result     vector.FunctionResultWrapper
	expected   FunctionTestResult
	fn         executeLogicOfOverload
	fnLength   int
	selectList *FunctionSelectList
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
	fn executeLogicOfOverload) FunctionTestCase {
	f := FunctionTestCase{proc: proc}
	owned := &f
	defer func() {
		if owned != nil {
			owned.Free()
		}
	}()
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
	f.result = vector.NewFunctionResultWrapper(wanted.typ, mp)
	if len(f.parameters) == 0 {
		f.fnLength = 1
	} else {
		f.fnLength = f.parameters[0].Length()
	}
	f.expected = wanted
	f.fn = fn
	owned = nil
	return f
}

// WithSelectList sets the FunctionSelectList handed to the evaluated function. The
// expression framework supplies one whenever short-circuit evaluation has masked rows off;
// without it a test can only ever exercise the evaluate-every-row path, which is exactly
// where a batched fast path can wrongly evaluate (and raise errors from) a masked row.
func (fc FunctionTestCase) WithSelectList(selectList *FunctionSelectList) FunctionTestCase {
	fc.selectList = selectList
	return fc
}

// Free releases the case's input and result vectors. The process and evaluator
// remain caller-owned. A borrowed result is valid only until reset or Free.
func (fc *FunctionTestCase) Free() {
	if fc == nil {
		return
	}
	if fc.result != nil {
		fc.result.Free()
	}
	if fc.proc != nil {
		for _, parameter := range fc.parameters {
			if parameter != nil {
				parameter.Free(fc.proc.Mp())
			}
		}
	}
	*fc = FunctionTestCase{}
}

// RunAndFree consumes a terminal case while preserving Run's comparison path.
// Callers that inspect or reuse the result must retain Run and free afterward.
func (fc *FunctionTestCase) RunAndFree() (bool, string) {
	defer fc.Free()
	return fc.Run()
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

	err = fc.fn(fc.parameters, fc.result, fc.proc, fc.fnLength, fc.selectList)
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
	// Check complete metadata before decoding the result values.
	if *v.GetType() != fc.expected.typ {
		return false, fmt.Sprintf("expected result type %#v but get type %#v", fc.expected.typ,
			v.GetType())
	}
	switch v.GetType().Oid {
	case types.T_bool:
		return compareFunctionFixedResult[bool](v, fc.expected, fc.fnLength)
	case types.T_bit:
		return compareFunctionFixedResult[uint64](v, fc.expected, fc.fnLength)
	case types.T_int8:
		return compareFunctionFixedResult[int8](v, fc.expected, fc.fnLength)
	case types.T_int16:
		return compareFunctionFixedResult[int16](v, fc.expected, fc.fnLength)
	case types.T_int32:
		return compareFunctionFixedResult[int32](v, fc.expected, fc.fnLength)
	case types.T_int64:
		return compareFunctionFixedResult[int64](v, fc.expected, fc.fnLength)
	case types.T_uint8:
		return compareFunctionFixedResult[uint8](v, fc.expected, fc.fnLength)
	case types.T_uint16:
		return compareFunctionFixedResult[uint16](v, fc.expected, fc.fnLength)
	case types.T_uint32:
		return compareFunctionFixedResult[uint32](v, fc.expected, fc.fnLength)
	case types.T_uint64:
		return compareFunctionFixedResult[uint64](v, fc.expected, fc.fnLength)
	case types.T_float32:
		return compareFunctionFixedResult[float32](v, fc.expected, fc.fnLength)
	case types.T_float64:
		wanted := fc.expected.wanted.([]float64)
		r := vector.GenerateFunctionFixedTypeParameter[float64](v)
		for i := uint64(0); i < uint64(fc.fnLength); i++ {
			null1 := i < uint64(len(fc.expected.nullList)) && fc.expected.nullList[i]
			var want float64
			if !null1 {
				want = wanted[i]
			}
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
		return true, ""
	case types.T_decimal64:
		return compareFunctionFixedResult[types.Decimal64](v, fc.expected, fc.fnLength)
	case types.T_decimal128:
		return compareFunctionFixedResult[types.Decimal128](v, fc.expected, fc.fnLength)
	case types.T_decimal256:
		return compareFunctionFixedResult[types.Decimal256](v, fc.expected, fc.fnLength)
	case types.T_date:
		return compareFunctionFixedResult[types.Date](v, fc.expected, fc.fnLength)
	case types.T_datetime:
		return compareFunctionFixedResult[types.Datetime](v, fc.expected, fc.fnLength)
	case types.T_time:
		return compareFunctionFixedResult[types.Time](v, fc.expected, fc.fnLength)
	case types.T_timestamp:
		return compareFunctionFixedResult[types.Timestamp](v, fc.expected, fc.fnLength)
	case types.T_enum:
		return compareFunctionFixedResult[types.Enum](v, fc.expected, fc.fnLength)
	case types.T_uuid:
		return compareFunctionFixedResult[types.Uuid](v, fc.expected, fc.fnLength)
	case types.T_TS:
		return compareFunctionFixedResult[types.TS](v, fc.expected, fc.fnLength)
	case types.T_Rowid:
		return compareFunctionFixedResult[types.Rowid](v, fc.expected, fc.fnLength)
	case types.T_Blockid:
		return compareFunctionFixedResult[types.Blockid](v, fc.expected, fc.fnLength)
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
	defer vExpected.Free(fc.proc.Mp())
	var i uint64
	switch v.GetType().Oid {
	case types.T_bf16:
		if ok, info := compareFixedResult[types.BF16](fc, v, vExpected); !ok {
			return ok, info
		}
	case types.T_float16:
		if ok, info := compareFixedResult[types.Float16](fc, v, vExpected); !ok {
			return ok, info
		}
	case types.T_float8:
		if ok, info := compareFixedResult[types.Float8](fc, v, vExpected); !ok {
			return ok, info
		}
	case types.T_float4:
		if ok, info := compareFixedResult[types.Float4](fc, v, vExpected); !ok {
			return ok, info
		}
	case types.T_geometry, types.T_geometry32:
		// Geometry values are stored as WKB; expectations are written as WKT.
		// Canonicalize both sides to WKT before comparing.
		r := vector.GenerateFunctionStrParameter(v)
		s := vector.GenerateFunctionStrParameter(vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetStrValue(i)
			get, null2 := r.GetStrValue(i)
			if null1 {
				if null2 {
					continue
				}
				return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %s, but get NULL", i+1, geometryComparisonWKT(want))
			}
			wantWKT := geometryComparisonWKT(want)
			getWKT := geometryComparisonWKT(get)
			if wantWKT != getWKT {
				return false, fmt.Sprintf("the %dth row expected %s, but get %s", i+1, wantWKT, getWKT)
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
			wantArray, gotArray := types.BytesToArray[float32](want), types.BytesToArray[float32](get)
			if len(wantArray) != len(gotArray) {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v", i+1, wantArray, gotArray)
			}
			for j, value := range wantArray {
				if value != gotArray[j] && !(math.IsNaN(float64(value)) && math.IsNaN(float64(gotArray[j]))) {
					return false, fmt.Sprintf("the %dth row expected %v, but get %v", i+1, wantArray, gotArray)
				}
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
	case types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8:
		// Narrow vector types compare byte-exact (their stored representation is
		// the comparison ground truth; ArrayCompare only covers float32/float64).
		r := vector.GenerateFunctionStrParameter(v)
		s := vector.GenerateFunctionStrParameter(vExpected)
		for i = 0; i < uint64(fc.fnLength); i++ {
			want, null1 := s.GetStrValue(i)
			get, null2 := r.GetStrValue(i)
			if null1 {
				if null2 {
					continue
				}
				return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
			}
			if null2 {
				return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
			}
			if !bytes.Equal(want, get) {
				return false, fmt.Sprintf("the %dth row expected %v, but get %v", i+1, want, get)
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

func compareFunctionFixedResult[T types.FixedSizeTExceptStrType](actual *vector.Vector, expected FunctionTestResult, rows int) (bool, string) {
	wanted := expected.wanted.([]T)
	r := vector.GenerateFunctionFixedTypeParameter[T](actual)
	for i := uint64(0); i < uint64(rows); i++ {
		null1 := i < uint64(len(expected.nullList)) && expected.nullList[i]
		var want T
		if !null1 {
			want = wanted[i]
		}
		get, null2 := r.GetValue(i)
		if null1 {
			if null2 {
				continue
			}
			return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
		}
		if null2 {
			return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
		}
		if want != get {
			return false, fmt.Sprintf("the %dth row expected %v, but get %v", i+1, want, get)
		}
	}
	return true, ""
}

// DebugRun will not run the compare logic for function result but return the result vector directly.
func (fc *FunctionTestCase) DebugRun() (*vector.Vector, error) {
	if err := fc.result.PreExtendAndReset(fc.fnLength); err != nil {
		return nil, err
	}
	err := fc.fn(fc.parameters, fc.result, fc.proc, fc.fnLength, fc.selectList)
	return fc.result.GetResultVector(), err
}

// Benchmark checks the case once before timing and measures one evaluation per iteration.
func (fc *FunctionTestCase) Benchmark(b *testing.B) {
	b.Helper()
	if ok, info := fc.Run(); !ok {
		b.Fatal(info)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := fc.result.PreExtendAndReset(fc.fnLength); err != nil {
			b.Fatal(err)
		}
		err := fc.fn(fc.parameters, fc.result, fc.proc, fc.fnLength, fc.selectList)
		if err != nil && !fc.expected.wantErr {
			b.Fatal(err)
		}
		if err == nil && fc.expected.wantErr {
			b.Fatal("expected to run failed, but run succeed with no error")
		}
	}
}

// compareFixedResult compares a fixed-width result vector with the expected one row by
// row: NULL slots and values.
func compareFixedResult[T types.FixedSizeTExceptStrType](fc *FunctionTestCase, v, vExpected *vector.Vector) (bool, string) {
	r := vector.GenerateFunctionFixedTypeParameter[T](v)
	s := vector.GenerateFunctionFixedTypeParameter[T](vExpected)
	for i := uint64(0); i < uint64(fc.fnLength); i++ {
		want, null1 := s.GetValue(i)
		get, null2 := r.GetValue(i)
		if null1 {
			if null2 {
				continue
			}
			return false, fmt.Sprintf("the %dth row expected NULL, but get not null", i+1)
		}
		if null2 {
			return false, fmt.Sprintf("the %dth row expected %v, but get NULL", i+1, want)
		}
		if want != get {
			return false, fmt.Sprintf("the %dth row expected %v, but get %v", i+1, want, get)
		}
	}
	return true, ""
}

func newVectorByType(mp *mpool.MPool, typ types.Type, val any, nsp *nulls.Nulls) *vector.Vector {
	vec := vector.NewVec(typ)
	owned := vec
	defer func() {
		if owned != nil {
			owned.Free(mp)
		}
	}()
	var err error
	switch typ.Oid {
	case types.T_bool:
		values := val.([]bool)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_bit:
		values := val.([]uint64)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_int8:
		values := val.([]int8)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_int16:
		values := val.([]int16)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_int32:
		values := val.([]int32)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_int64:
		values := val.([]int64)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_uint8:
		values := val.([]uint8)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_uint16:
		values := val.([]uint16)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_uint32:
		values := val.([]uint32)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_uint64:
		values := val.([]uint64)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_float32:
		values := val.([]float32)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_float64:
		values := val.([]float64)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_decimal64:
		values := val.([]types.Decimal64)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_decimal128:
		values := val.([]types.Decimal128)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_decimal256:
		values := val.([]types.Decimal256)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_date:
		values := val.([]types.Date)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_datetime:
		values := val.([]types.Datetime)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_time:
		values := val.([]types.Time)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_timestamp:
		values := val.([]types.Timestamp)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink, types.T_geometry, types.T_geometry32:
		values := val.([]string)
		err = vector.AppendStringList(vec, values, nil, mp)
	case types.T_array_float32:
		values := val.([][]float32)
		err = vector.AppendArrayList[float32](vec, values, nil, mp)
	case types.T_array_float64:
		values := val.([][]float64)
		err = vector.AppendArrayList[float64](vec, values, nil, mp)
	case types.T_array_bf16:
		values := val.([][]types.BF16)
		err = vector.AppendArrayList[types.BF16](vec, values, nil, mp)
	case types.T_array_float16:
		values := val.([][]types.Float16)
		err = vector.AppendArrayList[types.Float16](vec, values, nil, mp)
	case types.T_array_int8:
		values := val.([][]int8)
		err = vector.AppendArrayList[int8](vec, values, nil, mp)
	case types.T_array_uint8:
		values := val.([][]uint8)
		err = vector.AppendArrayList[uint8](vec, values, nil, mp)
	case types.T_uuid:
		values := val.([]types.Uuid)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_TS:
		values := val.([]types.TS)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_Rowid:
		values := val.([]types.Rowid)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_Blockid:
		values := val.([]types.Blockid)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_json:
		values := val.([]string)
		for i, value := range values {
			// Null payload bytes are unspecified; install the null at admission,
			// rather than appending an invalid empty JSON value and marking it later.
			isNull := nsp != nil && nsp.Contains(uint64(i))
			if err := vector.AppendBytes(vec, []byte(value), isNull, mp); err != nil {
				panic(err)
			}
		}
	case types.T_enum:
		values := val.([]types.Enum)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_year:
		values := val.([]types.MoYear)
		err = vector.AppendFixedList(vec, values, nil, mp)
	case types.T_bf16:
		err = vector.AppendFixedList(vec, val.([]types.BF16), nil, mp)
	case types.T_float16:
		err = vector.AppendFixedList(vec, val.([]types.Float16), nil, mp)
	case types.T_float8:
		err = vector.AppendFixedList(vec, val.([]types.Float8), nil, mp)
	case types.T_float4:
		err = vector.AppendFixedList(vec, val.([]types.Float4), nil, mp)
	default:
		panic(fmt.Sprintf("function test framework do not support typ %s", typ))
	}
	if err != nil {
		panic(err)
	}
	vec.SetNulls(nsp)
	owned = nil
	return vec
}

func TestFunctionTestCaseOwnership(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	mp := proc.Mp()
	nativeBefore, heapBefore := mp.CurrNB(), mp.OnHeapCurrNB()
	assertReleased := func(t *testing.T) {
		t.Helper()
		require.Equal(t, nativeBefore, mp.CurrNB())
		require.Equal(t, heapBefore, mp.OnHeapCurrNB())
	}
	t.Run("constructor rollback", func(t *testing.T) {
		require.PanicsWithValue(t, "function test framework do not support typ ANY", func() {
			_ = NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{7}, nil),
				NewFunctionTestInput(types.T_any.ToType(), []int64{0}, nil),
			}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{7}, nil), AbsInt64)
		})
		assertReleased(t)
	})
	t.Run("current vector rollback", func(t *testing.T) {
		value, err := types.ParseStringToByteJson(`{"v":"01234567890123456789012345678901"}`)
		require.NoError(t, err)
		encoded, err := types.EncodeJson(value)
		require.NoError(t, err)
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_ = NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{7}, nil),
				NewFunctionTestInput(types.T_json.ToType(), []string{string(encoded), ""}, nil),
			}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{7}, nil), AbsInt64)
		}()
		err, ok := recovered.(error)
		require.True(t, ok)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Equal(t, "invalid input: invalid JSON vector payload", err.Error())
		assertReleased(t)
	})
	t.Run("borrow and reuse", func(t *testing.T) {
		fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_int64.ToType(), []int64{-7, -8}, nil)}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{7, 8}, nil), AbsInt64)
		defer fc.Free()
		ok, info := fc.Run()
		require.True(t, ok, info)
		fc.expected.wanted = []int64{7, 9}
		ok, info = fc.Run()
		require.False(t, ok)
		require.Equal(t, "the 2th row expected 9, but get 8", info)
		fc.expected.wanted = []int64{7, 8}
		fc.expected.nullList = []bool{true, false}
		ok, info = fc.Run()
		require.False(t, ok)
		require.Equal(t, "the 1th row expected NULL, but get not null", info)
		fc.parameters[0].GetNulls().Add(0)
		fc.expected.nullList = nil
		ok, info = fc.Run()
		require.False(t, ok)
		require.Equal(t, "the 1th row expected 7, but get NULL", info)
		fc.expected.nullList = []bool{true, false}
		fc.expected.wanted = []int64{999, 9}
		ok, info = fc.Run()
		require.False(t, ok)
		require.Equal(t, "the 2th row expected 9, but get 8", info)
		fc.expected.wanted = []int64{999, 8}
		ok, info = fc.Run()
		require.True(t, ok, info)
		fc.expected.nullList = nil
		fc.parameters[0].GetNulls().Reset()
		vector.MustFixedColNoTypeCheck[int64](fc.parameters[0])[0] = -9
		fc.expected.wanted = []int64{9, 8}
		ok, info = fc.Run()
		require.True(t, ok, info)
		require.Equal(t, []int64{9, 8}, vector.MustFixedColNoTypeCheck[int64](fc.GetResultVectorDirectly()))
		fc.Free()
		fc.Free()
		assertReleased(t)

		f64 := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_float64.ToType(), []float64{1.0000000005, math.NaN()}, nil)}, NewFunctionTestResult(types.T_float64.ToType(), false, []float64{1, math.NaN()}, nil), AbsFloat64)
		defer f64.Free()
		ok, info = f64.Run()
		require.True(t, ok, info)
		f64.expected.wanted = []float64{1.000000002, math.NaN()}
		ok, info = f64.Run()
		require.False(t, ok)
		require.Equal(t, "the 1th row expected 1.000000002, but get 1.0000000005", info)
		f64.Free()
		assertReleased(t)
		f32 := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_float32.ToType(), []float32{1e-10}, nil), NewFunctionTestInput(types.T_float32.ToType(), []float32{}, nil)}, NewFunctionTestResult(types.T_float32.ToType(), false, []float32{0}, nil), NewCast)
		defer f32.Free()
		ok, info = f32.Run()
		require.False(t, ok)
		require.Equal(t, "the 1th row expected 0, but get 1e-10", info)
		f32.Free()
		assertReleased(t)

		mask := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
		debug := NewFunctionTestCase(proc, []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-7, math.MinInt64}, nil),
		}, NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil), AbsInt64).WithSelectList(mask)
		defer debug.Free()
		result, err := debug.DebugRun()
		require.NoError(t, err)
		require.Equal(t, types.T_int64.ToType(), *result.GetType())
		require.Equal(t, 2, result.Length())
		values := vector.GenerateFunctionFixedTypeParameter[int64](result)
		value, isNull := values.GetValue(0)
		require.False(t, isNull)
		require.Equal(t, int64(7), value)
		_, isNull = values.GetValue(1)
		require.True(t, isNull)
		require.Equal(t, []int64{-7, math.MinInt64}, vector.MustFixedColNoTypeCheck[int64](debug.parameters[0]))
		require.Equal(t, []bool{true, false}, mask.SelectList)
		debug.selectList = nil
		copy(vector.MustFixedColNoTypeCheck[int64](debug.parameters[0]), []int64{-9, -8})
		result, err = debug.DebugRun()
		require.NoError(t, err)
		require.Equal(t, []int64{9, 8}, vector.MustFixedColNoTypeCheck[int64](result))
		require.False(t, result.IsNull(0))
		require.False(t, result.IsNull(1))
		debug.Free()
		assertReleased(t)

	})
	t.Run("literal expectation extent", func(t *testing.T) {
		for _, tc := range []struct {
			name           string
			values, wanted []int64
			nulls          []bool
			wantPanic      bool
		}{
			{"empty", []int64{}, []int64{}, nil, false},
			{"short NULL mask", []int64{-7, -8}, []int64{7, 8}, []bool{false}, false},
			{"NULL beyond payload", []int64{-7, 0}, []int64{7}, []bool{false, true}, false},
			{"all NULL without payload", []int64{0, 0}, []int64{}, []bool{true, true}, false},
			{"non NULL missing payload", []int64{-7, -8}, []int64{7}, nil, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				fc := NewFunctionTestCase(proc, []FunctionTestInput{
					NewFunctionTestInput(types.T_int64.ToType(), tc.values, tc.nulls),
				}, NewFunctionTestResult(types.T_int64.ToType(), false, tc.wanted, tc.nulls), AbsInt64)
				defer fc.Free()
				var recovered any
				func() {
					defer func() { recovered = recover() }()
					ok, info := fc.RunAndFree()
					require.True(t, ok, info)
				}()
				if tc.wantPanic {
					require.Implements(t, (*runtime.Error)(nil), recovered)
					require.Contains(t, recovered.(error).Error(), "index out of range")
				} else {
					require.Nil(t, recovered)
				}
				assertReleased(t)
			})
		}
	})
	t.Run("terminal comparison panic", func(t *testing.T) {
		fc := NewFunctionTestCase(proc, []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-7}, nil),
		}, NewFunctionTestResult(types.T_int64.ToType(), false, []string{"7"}, nil), AbsInt64)
		defer fc.Free()
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_, _ = fc.RunAndFree()
		}()
		require.IsType(t, &runtime.TypeAssertionError{}, recovered)
		assertReleased(t)
	})
	t.Run("JSON borrowed subtest lifetime", func(t *testing.T) {
		t.Run("evaluate", func(t *testing.T) {
			vec := runJsonFunctionWithSelectList(t, proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":1}`}, nil),
			}, types.T_json.ToType(), JsonKeys, nil)
			require.Equal(t, `["a"]`, jsonVectorRowString(t, vec, 0))
		})
		assertReleased(t)
	})
	t.Run("result admission denial", func(t *testing.T) {
		registry, err := mpool.NewAllocationAccountRegistry(1, 4)
		require.NoError(t, err)
		account, err := registry.Open(1)
		require.NoError(t, err)
		defer func() {
			snapshot := account.Seal()
			_, err := registry.Finalize(account)
			require.Zero(t, snapshot.Used)
			require.NoError(t, err)
		}()
		previousContext := proc.Ctx
		probe := &admissionProbeContext{Context: previousContext, doneCalled: make(chan struct{})}
		proc.Ctx = probe
		defer func() { proc.Ctx = previousContext }()
		fc := NewFunctionTestCase(proc, []FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{0, 0}, nil),
		}, NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0, 0}, nil), Sleep[uint64])
		defer fc.Free()
		selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
		require.NoError(t, err)
		result, err := vector.NewFunctionResultWrapperWithAllocation(types.T_uint8.ToType(), mp, selection)
		require.NoError(t, err)
		fc.result.Free()
		fc.result = result
		value, debugErr := fc.DebugRun()
		require.Nil(t, value)
		require.ErrorIs(t, debugErr, mpool.ErrAllocationAccountCapacity)
		require.Zero(t, account.Snapshot().Used)
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_, _ = fc.RunAndFree()
		}()
		err, ok := recovered.(error)
		require.True(t, ok)
		require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
		select {
		case <-probe.doneCalled:
			t.Fatal("result admission denial reached the evaluator")
		default:
		}
		require.Zero(t, account.Snapshot().Used)
		assertReleased(t)
	})
}

func TestFunctionResultMetadataContract(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	t.Run("float32 array values", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			want, actual []float32
			match        bool
		}{
			{"finite equal", []float32{1}, []float32{1}, true},
			{"finite different", []float32{1}, []float32{2}, false},
			{"unexpected NaN", []float32{1}, []float32{float32(math.NaN())}, false},
			{"missing NaN", []float32{float32(math.NaN())}, []float32{1}, false},
			{"matching NaN", []float32{float32(math.NaN())}, []float32{float32(math.NaN())}, true},
			{"array length", []float32{1}, []float32{1, 2}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1}}, nil)},
					NewFunctionTestResult(types.T_array_float32.ToType(), false, [][]float32{tc.want}, nil),
					func(_ []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, _ int, _ *FunctionSelectList) error {
						return vector.MustFunctionResult[types.Varlena](result).AppendBytes(types.ArrayToBytes(tc.actual), false)
					})
				t.Cleanup(fc.Free)
				ok, info := fc.RunAndFree()
				require.Equal(t, tc.match, ok, info)
				require.Zero(t, proc.Mp().CurrNB())
			})
		}
	})

	for _, tc := range []struct {
		name   string
		length int
		null   bool
		mutate func(*types.Type)
	}{
		{name: "correct", length: 1},
		{name: "oid", length: 1, mutate: func(t *types.Type) { t.Oid = types.T_float64 }},
		{name: "size", length: 1, mutate: func(t *types.Type) { t.Size++ }},
		{name: "width", length: 1, mutate: func(t *types.Type) { t.Width-- }},
		{name: "scale", length: 1, mutate: func(t *types.Type) { t.Scale++ }},
		{name: "charset", length: 1, mutate: func(t *types.Type) { t.Charset++ }},
		{name: "not null", length: 1, mutate: func(t *types.Type) { t.SetNotNull(true) }},
		{name: "empty width", length: 0, mutate: func(t *types.Type) { t.Width-- }},
		{name: "null scale", length: 1, null: true, mutate: func(t *types.Type) { t.Scale++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := types.New(types.T_decimal64, 18, 2)
			values := make([]types.Decimal64, tc.length)
			mask := make([]bool, tc.length)
			if tc.length > 0 {
				values[0] = 123
				mask[0] = tc.null
			}
			fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(typ, values, mask)}, NewFunctionTestResult(typ, false, values, mask),
				func(_ []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, _ *FunctionSelectList) error {
					v := result.GetResultVector()
					if length > 0 {
						if err := vector.SetFixedAtWithTypeCheck(v, 0, types.Decimal64(123)); err != nil {
							return err
						}
						if tc.null {
							v.GetNulls().Add(0)
						}
					}
					if tc.mutate != nil {
						tc.mutate(v.GetType())
					}
					return nil
				})
			ok, info := fc.RunAndFree()
			require.Equal(t, tc.mutate == nil, ok, info)
			if tc.mutate != nil {
				require.Contains(t, info, "expected result type")
				require.Contains(t, info, "types.Type")
			}
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}
