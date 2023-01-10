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
	"github.com/matrixorigin/matrixone/pkg/vectorize/compare"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type compareT interface {
	constraints.Integer | constraints.Float | bool |
		types.Date | types.Time | types.Datetime | types.Timestamp
}

var boolType = types.T_bool.ToType()

func handleScalarNull(v1, v2 *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if v1.IsConstNull() {
		return proc.AllocConstNullVector(boolType), nil
	} else if v2.IsConstNull() {
		return proc.AllocConstNullVector(boolType), nil
	}
	panic(moerr.NewInternalError(proc.Ctx, "handleScalarNull failed."))
}

func allocateBoolVector(length int, proc *process.Process) *vector.Vector {
	vec, err := proc.AllocVectorOfRows(boolType, int64(length), nil)
	if err != nil {
		panic(moerr.NewOOM(proc.Ctx))
	}
	return vec
}

type compareFn func(v1, v2, r *vector.Vector) error

func CompareOrdered(vs []*vector.Vector, proc *process.Process, cfn compareFn) (*vector.Vector, error) {
	left, right := vs[0], vs[1]

	if left.IsConstNull() || right.IsConstNull() {
		return handleScalarNull(left, right, proc)
	}

	if left.IsConst() && right.IsConst() {
		resultVector := proc.AllocScalarVector(boolType)
		if err := cfn(left, right, resultVector); err != nil {
			return nil, err
		}
		return resultVector, nil
	}

	length := left.Length()
	if left.IsConst() {
		length = right.Length()
	}
	resultVector := allocateBoolVector(length, proc)
	nulls.Or(left.GetNulls(), right.GetNulls(), resultVector.GetNulls())

	if err := cfn(left, right, resultVector); err != nil {
		return nil, err
	}
	return resultVector, nil
}

// Equal compare operator
func EqGeneral[T compareT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.NumericEqual[T])
}

func EqDecimal64(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.Decimal64VecEq)
}

func EqDecimal128(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.Decimal128VecEq)
}

// Not Equal compare operator
func NeGeneral[T compareT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.NumericNotEqual[T])
}

func NeDecimal64(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.Decimal64VecNe)
}

func NeDecimal128(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.Decimal128VecNe)
}

// IN operator
func INGeneral[T compareT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	leftVec, rightVec := args[0], args[1]
	left, right := vector.MustTCols[T](leftVec), vector.MustTCols[T](rightVec)
	lenLeft := len(left)
	lenRight := len(right)
	if leftVec.IsConst() {
		lenLeft = 1
	}
	inMap := make(map[T]bool, lenRight)
	for i := 0; i < lenRight; i++ {
		if !rightVec.GetNulls().Contains(uint64(i)) {
			inMap[right[i]] = true
		}
	}
	retVec := allocateBoolVector(lenLeft, proc)
	ret := vector.MustTCols[bool](retVec)
	for i := 0; i < lenLeft; i++ {
		if _, ok := inMap[left[i]]; ok {
			ret[i] = true
		}
	}
	nulls.Or(leftVec.GetNulls(), nil, retVec.GetNulls())
	return retVec, nil
}

func INString(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	leftVec, rightVec := args[0], args[1]
	left, area1 := vector.MustVarlenaRawData(leftVec)
	right, area2 := vector.MustVarlenaRawData(rightVec)

	lenLeft := len(left)
	lenRight := len(right)
	if leftVec.IsConst() {
		lenLeft = 1
	}
	inMap := make(map[string]bool, lenRight)
	for i := 0; i < lenRight; i++ {
		if !rightVec.GetNulls().Contains(uint64(i)) {
			inMap[right[i].GetString(area2)] = true
		}
	}
	retVec := allocateBoolVector(lenLeft, proc)
	ret := vector.MustTCols[bool](retVec)
	for i := 0; i < lenLeft; i++ {
		if _, ok := inMap[left[i].GetString(area1)]; ok {
			ret[i] = true
		}
	}
	nulls.Or(leftVec.GetNulls(), nil, retVec.GetNulls())
	return retVec, nil
}

// NOT IN operator
func NotINGeneral[T compareT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	leftVec, rightVec := args[0], args[1]
	left, right := vector.MustTCols[T](leftVec), vector.MustTCols[T](rightVec)
	lenLeft := len(left)
	lenRight := len(right)
	if leftVec.IsConst() {
		lenLeft = 1
	}
	notInMap := make(map[T]bool, lenRight)
	for i := 0; i < lenRight; i++ {
		if !rightVec.GetNulls().Contains(uint64(i)) {
			notInMap[right[i]] = true
		} else {
			//not in null, return false
			vec := vector.New(vector.CONSTANT, boolType)
			vector.Append(vec, false, false, proc.Mp())
			return vec, nil
		}
	}
	retVec := allocateBoolVector(lenLeft, proc)
	ret := vector.MustTCols[bool](retVec)
	for i := 0; i < lenLeft; i++ {
		if _, ok := notInMap[left[i]]; !ok {
			ret[i] = true
		}
	}
	nulls.Or(leftVec.GetNulls(), nil, retVec.GetNulls())
	return retVec, nil
}

func NotINString(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	leftVec, rightVec := args[0], args[1]
	left, area1 := vector.MustVarlenaRawData(leftVec)
	right, area2 := vector.MustVarlenaRawData(rightVec)

	lenLeft := len(left)
	lenRight := len(right)
	if leftVec.IsConst() {
		lenLeft = 1
	}
	inMap := make(map[string]bool, lenRight)
	for i := 0; i < lenRight; i++ {
		if !rightVec.GetNulls().Contains(uint64(i)) {
			inMap[right[i].GetString(area2)] = true
		} else {
			//not in null, return false
			vec := vector.New(vector.CONSTANT, boolType)
			vector.Append(vec, false, false, proc.Mp())
			return vec, nil
		}
	}
	retVec := allocateBoolVector(lenLeft, proc)
	ret := vector.MustTCols[bool](retVec)
	for i := 0; i < lenLeft; i++ {
		if _, ok := inMap[left[i].GetString(area1)]; !ok {
			ret[i] = true
		}
	}
	nulls.Or(leftVec.GetNulls(), nil, retVec.GetNulls())
	return retVec, nil
}

// Great than operator
func GtGeneral[T compareT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.NumericGreatThan[T])
}

func GtDecimal64(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.Decimal64VecGt)
}

func GtDecimal128(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.Decimal128VecGt)
}

// Great equal operator
func GeGeneral[T compareT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.NumericGreatEqual[T])
}

func GeDecimal64(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.Decimal64VecGe)
}

func GeDecimal128(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.Decimal128VecGe)
}

// less than operator
func LtGeneral[T compareT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.NumericLessThan[T])
}

func LtDecimal64(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.Decimal64VecLt)
}

func LtDecimal128(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.Decimal128VecLt)
}

// less equal operator
func LeGeneral[T compareT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.NumericLessEqual[T])
}

func LeDecimal64(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.Decimal64VecLe)
}

func LeDecimal128(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareOrdered(args, proc, compare.Decimal128VecLe)
}

// string compare
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

func CompareValenaInline(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]
	if v1.IsConstNull() || v2.IsConstNull() {
		return handleScalarNull(v1, v2, proc)
	}
	col1, _ := vector.MustVarlenaRawData(v1)
	col2, _ := vector.MustVarlenaRawData(v2)

	if v1.IsConst() && v2.IsConst() {
		p1 := col1[0].UnsafePtr()
		p2 := col2[0].UnsafePtr()
		ret := *(*[3]int64)(p1) == *(*[3]int64)(p2)
		vec := vector.New(vector.CONSTANT, boolType)
		vector.Append(vec, ret, false, proc.Mp())
		return vec, nil
	}

	length := v1.Length()
	if length < v2.Length() {
		length = v2.Length()
	}
	vec := allocateBoolVector(length, proc)
	veccol := vector.MustTCols[bool](vec)

	if !v1.IsConst() && !v2.IsConst() {
		for i := 0; i < length; i++ {
			p1 := col1[i].UnsafePtr()
			p2 := col2[i].UnsafePtr()
			veccol[i] = *(*[3]int64)(p1) == *(*[3]int64)(p2)
		}
		nulls.Or(v1.GetNulls(), v2.GetNulls(), vec.GetNulls())
	} else if v1.IsConst() {
		p1 := col1[0].UnsafePtr()
		for i := 0; i < length; i++ {
			p2 := col2[i].UnsafePtr()
			veccol[i] = *(*[3]int64)(p1) == *(*[3]int64)(p2)
		}
		nulls.Or(nil, v2.GetNulls(), vec.GetNulls())
	} else {
		p2 := col2[0].UnsafePtr()
		for i := 0; i < length; i++ {
			p1 := col1[i].UnsafePtr()
			veccol[i] = *(*[3]int64)(p1) == *(*[3]int64)(p2)
		}
		nulls.Or(v1.GetNulls(), nil, vec.GetNulls())
	}
	return vec, nil
}

func CompareString(vs []*vector.Vector, fn compStringFn, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]

	if v1.IsConstNull() || v2.IsConstNull() {
		return handleScalarNull(v1, v2, proc)
	}

	if v1.IsConst() && v2.IsConst() {
		col1, col2 := vector.MustBytesCols(v1), vector.MustBytesCols(v2)
		vec := vector.New(vector.CONSTANT, boolType)
		vector.Append(vec, fn(col1[0], col2[0], v1.GetType().Width, v2.GetType().Width), false, proc.Mp())
		return vec, nil
	}

	if v1.IsConst() {
		col1 := vector.MustBytesCols(v1)
		col2, area := vector.MustVarlenaRawData(v2)
		length := v2.Length()
		vec := allocateBoolVector(length, proc)
		veccol := vector.MustTCols[bool](vec)
		if v2.GetArea() == nil {
			for i := range veccol {
				veccol[i] = fn(col1[0], (&col2[i]).ByteSlice(), v1.GetType().Width, v2.GetType().Width)
			}
		} else {
			for i := range veccol {
				veccol[i] = fn(col1[0], (&col2[i]).GetByteSlice(area), v1.GetType().Width, v2.GetType().Width)
			}
		}
		nulls.Or(v2.GetNulls(), nil, vec.GetNulls())
		return vec, nil
	}

	if v2.IsConst() {
		col1, area := vector.MustVarlenaRawData(v1)
		col2 := vector.MustBytesCols(v2)
		length := v1.Length()
		vec := allocateBoolVector(length, proc)
		veccol := vector.MustTCols[bool](vec)
		if v1.GetArea() == nil {
			for i := range veccol {
				veccol[i] = fn((&col1[i]).ByteSlice(), col2[0], v1.GetType().Width, v2.GetType().Width)
			}
		} else {
			for i := range veccol {
				veccol[i] = fn((&col1[i]).GetByteSlice(area), col2[0], v1.GetType().Width, v2.GetType().Width)
			}
		}
		nulls.Or(v1.GetNulls(), nil, vec.GetNulls())
		return vec, nil
	}

	// Vec Vec
	col1, area1 := vector.MustVarlenaRawData(v1)
	col2, area2 := vector.MustVarlenaRawData(v2)
	length := v1.Length()
	vec := allocateBoolVector(length, proc)
	veccol := vector.MustTCols[bool](vec)
	if v1.GetArea() == nil && v2.GetArea() == nil {
		for i := range veccol {
			veccol[i] = fn((&col1[i]).ByteSlice(), (&col2[i]).ByteSlice(), v1.GetType().Width, v2.GetType().Width)
		}
	} else if v1.GetArea() == nil {
		for i := range veccol {
			veccol[i] = fn((&col1[i]).ByteSlice(), (&col2[i]).GetByteSlice(area2), v1.GetType().Width, v2.GetType().Width)
		}
	} else if v2.GetArea() == nil {
		for i := range veccol {
			veccol[i] = fn((&col1[i]).GetByteSlice(area1), (&col2[i]).ByteSlice(), v1.GetType().Width, v2.GetType().Width)
		}
	} else {
		for i := range veccol {
			veccol[i] = fn((&col1[i]).GetByteSlice(area1), (&col2[i]).GetByteSlice(area2), v1.GetType().Width, v2.GetType().Width)
		}
	}
	nulls.Or(v1.GetNulls(), v2.GetNulls(), vec.GetNulls())
	return vec, nil
}

func EqString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if vs[0].GetArea() == nil && vs[1].GetArea() == nil {
		return CompareValenaInline(vs, proc)
	}
	return CompareString(vs, CompareBytesEq, proc)
}

func LeString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareString(vs, CompareBytesLe, proc)
}

func LtString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareString(vs, CompareBytesLt, proc)
}

func GeString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareString(vs, CompareBytesGe, proc)
}

func GtString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareString(vs, CompareBytesGt, proc)
}

func NeString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareString(vs, CompareBytesNe, proc)
}

// uuid compare
type compUuidFn func(v1, v2 [16]byte) bool

func CompareUuidEq(v1, v2 [16]byte) bool {
	return types.EqualUuid(v1, v2)
}
func CompareUuidLe(v1, v2 [16]byte) bool {
	return types.CompareUuid(v1, v2) <= 0
}
func CompareUuidLt(v1, v2 [16]byte) bool {
	return types.CompareUuid(v1, v2) < 0
}
func CompareUuidGe(v1, v2 [16]byte) bool {
	return types.CompareUuid(v1, v2) >= 0
}
func CompareUuidGt(v1, v2 [16]byte) bool {
	return types.CompareUuid(v1, v2) > 0
}
func CompareUuidNe(v1, v2 [16]byte) bool {
	return !types.EqualUuid(v1, v2)
}

func CompareUuid(vs []*vector.Vector, fn compUuidFn, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]
	//col1, col2 := vector.MustBytesCols(v1), vector.MustBytesCols(v2)
	col1, col2 := vector.MustTCols[types.Uuid](v1), vector.MustTCols[types.Uuid](v2)
	if v1.IsConstNull() || v2.IsConstNull() {
		return handleScalarNull(v1, v2, proc)
	}

	if v1.IsConst() && v2.IsConst() {
		vec := vector.New(vector.CONSTANT, boolType)
		vector.Append(vec, fn(col1[0], col2[0]), false, proc.Mp())
		return vec, nil
	}

	if v1.IsConst() {
		length := v2.Length()
		vec := allocateBoolVector(length, proc)
		veccol := vector.MustTCols[bool](vec)
		for i := range veccol {
			veccol[i] = fn(col1[0], col2[i])
		}
		nulls.Or(v2.GetNulls(), nil, vec.GetNulls())
		return vec, nil
	}

	if v2.IsConst() {
		length := v1.Length()
		vec := allocateBoolVector(length, proc)
		veccol := vector.MustTCols[bool](vec)
		for i := range veccol {
			veccol[i] = fn(col1[i], col2[0])
		}
		nulls.Or(v1.GetNulls(), nil, vec.GetNulls())
		return vec, nil
	}

	// Vec Vec
	length := v1.Length()
	vec := allocateBoolVector(length, proc)
	veccol := vector.MustTCols[bool](vec)
	for i := range veccol {
		veccol[i] = fn(col1[i], col2[i])
	}
	nulls.Or(v1.GetNulls(), v2.GetNulls(), vec.GetNulls())
	return vec, nil
}

func EqUuid(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareUuid(vs, CompareUuidEq, proc)
}

func LeUuid(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareUuid(vs, CompareUuidLe, proc)
}

func LtUuid(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareUuid(vs, CompareUuidLt, proc)
}

func GeUuid(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareUuid(vs, CompareUuidGe, proc)
}

func GtUuid(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareUuid(vs, CompareUuidGt, proc)
}

func NeUuid(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return CompareUuid(vs, CompareUuidNe, proc)
}
