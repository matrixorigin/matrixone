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

package operator

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

var boolType = types.T_bool.ToType()

type CompEq[T constraints.Integer | constraints.Float] struct{}
type CompGe[T constraints.Integer | constraints.Float] struct{}
type CompGt[T constraints.Integer | constraints.Float] struct{}
type CompLe[T constraints.Integer | constraints.Float] struct{}
type CompLt[T constraints.Integer | constraints.Float] struct{}
type CompNe[T constraints.Integer | constraints.Float] struct{}

type CompOp[T constraints.Integer | constraints.Float] interface {
	CompEq[T] | CompGe[T] | CompGt[T] | CompLe[T] | CompLt[T] | CompNe[T]
	Eval(a, b T) bool
}

func (c CompEq[T]) Eval(a, b T) bool {
	return a == b
}

func (c CompGe[T]) Eval(a, b T) bool {
	return a >= b
}

func (c CompGt[T]) Eval(a, b T) bool {
	return a > b
}

func (c CompLe[T]) Eval(a, b T) bool {
	return a <= b
}

func (c CompLt[T]) Eval(a, b T) bool {
	return a < b
}

func (c CompNe[T]) Eval(a, b T) bool {
	return a != b
}

func handleScalarNull(v1, v2 *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if v1.IsScalarNull() {
		return proc.AllocConstNullVector(boolType, vector.Length(v2)), nil
	} else if v2.IsScalarNull() {
		return proc.AllocConstNullVector(boolType, vector.Length(v1)), nil
	}
	panic(moerr.NewPanicError("handleScalarNull failed."))
}

func allocateBoolVector(length int, proc *process.Process) *vector.Vector {
	vec, err := proc.AllocVector(boolType, int64(length))
	if err != nil {
		panic(moerr.NewPanicError("allocBoolVec OOM"))
	}
	vec.Col = encoding.DecodeBoolSlice(vec.Data)
	vec.Col = vec.Col.([]bool)[:length]
	return vec
}

func CompareOrdered[T constraints.Integer | constraints.Float, C CompOp[T]](c C, vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]
	col1, col2 := vector.MustTCols[T](v1), vector.MustTCols[T](v2)
	if v1.IsScalarNull() || v2.IsScalarNull() {
		return handleScalarNull(v1, v2, proc)
	}

	if v1.IsScalar() && v2.IsScalar() {
		vec := proc.AllocScalarVector(boolType)
		vec.Col = make([]bool, 1)
		vec.Col.([]bool)[0] = c.Eval(col1[0], col2[0])
		return vec, nil
	}

	if v1.IsScalar() {
		val1 := col1[0]
		length := vector.Length(v2)
		vec := allocateBoolVector(length, proc)
		veccol := vec.Col.([]bool)
		for i := range veccol {
			veccol[i] = c.Eval(val1, col2[i])
		}
		nulls.Or(v2.Nsp, nil, vec.Nsp)
		return vec, nil
	}

	if v2.IsScalar() {
		val2 := col2[0]
		length := vector.Length(v1)
		vec := allocateBoolVector(length, proc)
		veccol := vec.Col.([]bool)
		for i := range veccol {
			veccol[i] = c.Eval(col1[i], val2)
		}
		nulls.Or(v1.Nsp, nil, vec.Nsp)
		return vec, nil
	}

	// Vec Vec
	length := vector.Length(v1)
	vec := allocateBoolVector(length, proc)
	veccol := vec.Col.([]bool)
	for i := range veccol {
		veccol[i] = c.Eval(col1[i], col2[i])
	}
	nulls.Or(v1.Nsp, v2.Nsp, vec.Nsp)
	return vec, nil
}

type compFnType interface {
	bool | types.Decimal64 | types.Decimal128
}

type compFn[T compFnType] func(v1, v2 T, s1, s2 int32) bool

func CompareWithFn[T compFnType](vs []*vector.Vector, fn compFn[T], proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]

	col1, col2 := vector.MustTCols[T](v1), vector.MustTCols[T](v2)
	if v1.IsScalarNull() || v2.IsScalarNull() {
		return handleScalarNull(v1, v2, proc)
	}

	if v1.IsScalar() && v2.IsScalar() {
		vec := proc.AllocScalarVector(boolType)
		vec.Col = make([]bool, 1)
		vec.Col.([]bool)[0] = fn(col1[0], col2[0], v1.Typ.Scale, v2.Typ.Scale)
		return vec, nil
	}

	if v1.IsScalar() {
		val1 := col1[0]
		length := vector.Length(v2)
		vec := allocateBoolVector(length, proc)
		veccol := vec.Col.([]bool)
		for i := range veccol {
			veccol[i] = fn(val1, col2[i], v1.Typ.Scale, v2.Typ.Scale)
		}
		nulls.Or(v2.Nsp, nil, vec.Nsp)
		return vec, nil
	}

	if v2.IsScalar() {
		val2 := col2[0]
		length := vector.Length(v1)
		vec := allocateBoolVector(length, proc)
		veccol := vec.Col.([]bool)
		for i := range veccol {
			veccol[i] = fn(col1[i], val2, v1.Typ.Scale, v2.Typ.Scale)
		}
		nulls.Or(v1.Nsp, nil, vec.Nsp)
		return vec, nil
	}

	// Vec Vec
	length := vector.Length(v1)
	vec := allocateBoolVector(length, proc)
	veccol := vec.Col.([]bool)
	for i := range veccol {
		veccol[i] = fn(col1[i], col2[i], v1.Typ.Scale, v2.Typ.Scale)
	}
	nulls.Or(v1.Nsp, v2.Nsp, vec.Nsp)
	return vec, nil
}

func CompareDecimal64Eq(v1, v2 types.Decimal64, s1, s2 int32) bool {
	return types.CompareDecimal64Decimal64(v1, v2, s1, s2) == 0
}
func CompareDecimal64Le(v1, v2 types.Decimal64, s1, s2 int32) bool {
	return types.CompareDecimal64Decimal64(v1, v2, s1, s2) <= 0
}
func CompareDecimal64Lt(v1, v2 types.Decimal64, s1, s2 int32) bool {
	return types.CompareDecimal64Decimal64(v1, v2, s1, s2) < 0
}
func CompareDecimal64Ge(v1, v2 types.Decimal64, s1, s2 int32) bool {
	return types.CompareDecimal64Decimal64(v1, v2, s1, s2) >= 0
}
func CompareDecimal64Gt(v1, v2 types.Decimal64, s1, s2 int32) bool {
	return types.CompareDecimal64Decimal64(v1, v2, s1, s2) > 0
}
func CompareDecimal64Ne(v1, v2 types.Decimal64, s1, s2 int32) bool {
	return types.CompareDecimal64Decimal64(v1, v2, s1, s2) != 0
}

func CompareDecimal128Eq(v1, v2 types.Decimal128, s1, s2 int32) bool {
	return types.CompareDecimal128Decimal128(v1, v2, s1, s2) == 0
}
func CompareDecimal128Le(v1, v2 types.Decimal128, s1, s2 int32) bool {
	return types.CompareDecimal128Decimal128(v1, v2, s1, s2) <= 0
}
func CompareDecimal128Lt(v1, v2 types.Decimal128, s1, s2 int32) bool {
	return types.CompareDecimal128Decimal128(v1, v2, s1, s2) < 0
}
func CompareDecimal128Ge(v1, v2 types.Decimal128, s1, s2 int32) bool {
	return types.CompareDecimal128Decimal128(v1, v2, s1, s2) >= 0
}
func CompareDecimal128Gt(v1, v2 types.Decimal128, s1, s2 int32) bool {
	return types.CompareDecimal128Decimal128(v1, v2, s1, s2) > 0
}
func CompareDecimal128Ne(v1, v2 types.Decimal128, s1, s2 int32) bool {
	return types.CompareDecimal128Decimal128(v1, v2, s1, s2) != 0
}

func CompareBoolEq(v1, v2 bool, s1, s2 int32) bool {
	return v1 == v2
}
func CompareBoolLe(v1, v2 bool, s1, s2 int32) bool {
	return (!v1) || v2
}
func CompareBoolLt(v1, v2 bool, s1, s2 int32) bool {
	return (!v1) && v2
}
func CompareBoolGe(v1, v2 bool, s1, s2 int32) bool {
	return v1 || (!v2)
}
func CompareBoolGt(v1, v2 bool, s1, s2 int32) bool {
	return v1 && (!v2)
}
func CompareBoolNe(v1, v2 bool, s1, s2 int32) bool {
	return v1 != v2
}

type compStringFn func(v1, v2 []byte, s1, s2 int32) bool

func CompareBytesEq(v1, v2 []byte, s1, s2 int32) bool {
	return bytes.Equal(v1, v2)
}
func CompareBytesLe(v1, v2 []byte, s1, s2 int32) bool {
	return bytes.Compare(v1, v2) <= 0
}
func CompareBytesLt(v1, v2 []byte, s1, s2 int32) bool {
	return bytes.Compare(v1, v2) < 0
}
func CompareBytesGe(v1, v2 []byte, s1, s2 int32) bool {
	return bytes.Compare(v1, v2) >= 0
}
func CompareBytesGt(v1, v2 []byte, s1, s2 int32) bool {
	return bytes.Compare(v1, v2) > 0
}
func CompareBytesNe(v1, v2 []byte, s1, s2 int32) bool {
	return !bytes.Equal(v1, v2)
}

func CompareString(vs []*vector.Vector, fn compStringFn, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]
	col1, col2 := vector.MustBytesCols(v1), vector.MustBytesCols(v2)
	if v1.IsScalarNull() || v2.IsScalarNull() {
		return handleScalarNull(v1, v2, proc)
	}

	if v1.IsScalar() && v2.IsScalar() {
		vec := proc.AllocScalarVector(boolType)
		vec.Col = make([]bool, 1)
		vec.Col.([]bool)[0] = fn(col1.Get(0), col2.Get(0), v1.Typ.Scale, v2.Typ.Scale)
		return vec, nil
	}

	if v1.IsScalar() {
		length := vector.Length(v2)
		vec := allocateBoolVector(length, proc)
		veccol := vec.Col.([]bool)
		for i := range veccol {
			veccol[i] = fn(col1.Get(0), col2.Get(int64(i)), v1.Typ.Scale, v2.Typ.Scale)
		}
		nulls.Or(v2.Nsp, nil, vec.Nsp)
		return vec, nil
	}

	if v2.IsScalar() {
		length := vector.Length(v1)
		vec := allocateBoolVector(length, proc)
		veccol := vec.Col.([]bool)
		for i := range veccol {
			veccol[i] = fn(col1.Get(int64(i)), col2.Get(0), v1.Typ.Scale, v2.Typ.Scale)
		}
		nulls.Or(v1.Nsp, nil, vec.Nsp)
		return vec, nil
	}

	// Vec Vec
	length := vector.Length(v1)
	vec := allocateBoolVector(length, proc)
	veccol := vec.Col.([]bool)
	for i := range veccol {
		veccol[i] = fn(col1.Get(int64(i)), col2.Get(int64(i)), v1.Typ.Scale, v2.Typ.Scale)
	}
	nulls.Or(v1.Nsp, v2.Nsp, vec.Nsp)
	return vec, nil
}

func EqGeneral[T constraints.Integer | constraints.Float](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var c CompEq[T]
	return CompareOrdered[T](c, vs, proc)
}

func EqDecimal64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareDecimal64Eq, proc)
}

func EqDecimal128(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareDecimal128Eq, proc)
}

func EqBool(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareBoolEq, proc)
}

func EqString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareString(vs, CompareBytesEq, proc)
}

func LeGeneral[T constraints.Integer | constraints.Float](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var c CompLe[T]
	return CompareOrdered[T](c, vs, proc)
}

func LeDecimal64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareDecimal64Le, proc)
}

func LeDecimal128(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareDecimal128Le, proc)
}

func LeBool(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareBoolLe, proc)
}

func LeString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareString(vs, CompareBytesLe, proc)
}

func LtGeneral[T constraints.Integer | constraints.Float](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var c CompLt[T]
	return CompareOrdered[T](c, vs, proc)
}

func LtDecimal64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareDecimal64Lt, proc)
}

func LtDecimal128(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareDecimal128Lt, proc)
}

func LtBool(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareBoolLt, proc)
}

func LtString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareString(vs, CompareBytesLt, proc)
}

func GeGeneral[T constraints.Integer | constraints.Float](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var c CompGe[T]
	return CompareOrdered[T](c, vs, proc)
}

func GeDecimal64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareDecimal64Ge, proc)
}

func GeDecimal128(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareDecimal128Ge, proc)
}

func GeBool(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareBoolGe, proc)
}

func GeString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareString(vs, CompareBytesGe, proc)
}

func GtGeneral[T constraints.Integer | constraints.Float](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var c CompGt[T]
	return CompareOrdered[T](c, vs, proc)
}

func GtDecimal64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareDecimal64Gt, proc)
}

func GtDecimal128(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareDecimal128Gt, proc)
}

func GtBool(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareBoolGt, proc)
}

func GtString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareString(vs, CompareBytesGt, proc)
}

func NeGeneral[T constraints.Integer | constraints.Float](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var c CompNe[T]
	return CompareOrdered[T](c, vs, proc)
}

func NeDecimal64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareDecimal64Ne, proc)
}

func NeDecimal128(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareDecimal128Ne, proc)
}

func NeBool(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareWithFn(vs, CompareBoolNe, proc)
}

func NeString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareString(vs, CompareBytesNe, proc)
}
