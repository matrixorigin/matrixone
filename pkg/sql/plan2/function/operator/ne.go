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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var retType = types.T_bool.ToType()

/*func nequal[T OrderedValue](d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(T), d2.(T)
	return l != v
}

func nequal_B(d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(bool), d2.(bool)
	return l != v
}

func nequal_D(d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(types.Decimal128), d2.(types.Decimal128)
	return types.CompareDecimal128Decimal128(l, v, aScale, bScale) != 0
}

type NeOpFunc = func(d1, d2 interface{}, aScale, bScale int32) bool

var NeOpFuncMap = map[int]NeOpFunc{}

var NeOpFuncVec = []NeOpFunc{
	nequal[int8], nequal[int16], nequal[int32], nequal[int64], nequal[uint8], nequal[uint16], nequal[uint32],
	nequal[uint64], nequal[float32], nequal[float64], nequal[string], nequal_B, nequal[types.Date],
	nequal[types.Datetime], nequal[types.Decimal64], nequal_D,
}

func InitNeOpFuncMap() {
	for i := 0; i < len(NeOpFuncVec); i++ {
		NeOpFuncMap[i] = NeOpFuncVec[i]
	}
}

var StrNeOpFuncMap = map[int]StrCompOpFunc{}

var StrNeOpFuncVec = []StrCompOpFunc{
	nequalCol_Col, nequalCol_Const, nequalConst_Col, nequalConst_Const,
}

func nequalCol_Col(d1, d2 *vector.Vector) ([]bool, error) {
	lvs, ok := d1.Col.(*types.Bytes)
	if !ok {
		return nil, errors.New("the left col type is not *types.Bytes")
	}
	rvs, ok := d2.Col.(*types.Bytes)
	if !ok {
		return nil, errors.New("the right col type is not *types.Bytes")
	}
	rs := make([]int64, len(lvs.Lengths))
	rs = ne.StrNe(lvs, rvs, rs)
	col := make([]bool, len(lvs.Lengths))
	rsi := 0
	for i := 0; i < len(col); i++ {
		if rsi >= len(rs) {
			break
		}
		if int64(i) == rs[rsi] {
			col[i] = true
			rsi++
		}
		if nulls.Contains(d1.Nsp, uint64(i)) || nulls.Contains(d2.Nsp, uint64(i)) {
			col[i] = false
		}
	}
	return col, nil
}

func nequalCol_Const(d1, d2 *vector.Vector) ([]bool, error) {
	lvs, ok := d1.Col.(*types.Bytes)
	if !ok {
		return nil, errors.New("the left col type is not *types.Bytes")
	}
	rvs, ok := d2.Col.(*types.Bytes)
	if !ok {
		return nil, errors.New("the right col type is not *types.Bytes")
	}
	rs := make([]int64, len(lvs.Lengths))
	rs = ne.StrNeScalar(rvs.Data, lvs, rs)
	col := make([]bool, len(lvs.Lengths))
	rsi := 0
	for i := 0; i < len(col); i++ {
		if rsi >= len(rs) {
			break
		}
		if int64(i) == rs[rsi] {
			col[i] = true
			rsi++
		}
		if nulls.Contains(d1.Nsp, uint64(i)) {
			col[i] = false
		}
	}
	return col, nil
}

func nequalConst_Col(d1, d2 *vector.Vector) ([]bool, error) {
	lvs, ok := d1.Col.(*types.Bytes)
	if !ok {
		return nil, errors.New("the left col type is not *types.Bytes")
	}
	rvs, ok := d2.Col.(*types.Bytes)
	if !ok {
		return nil, errors.New("the right col type is not *types.Bytes")
	}
	rs := make([]int64, len(rvs.Lengths))
	rs = ne.StrNeScalar(lvs.Data, rvs, rs)
	col := make([]bool, len(rvs.Lengths))
	rsi := 0
	for i := 0; i < len(col); i++ {
		if rsi >= len(rs) {
			break
		}
		if int64(i) == rs[rsi] {
			col[i] = true
			rsi++
		}
		if nulls.Contains(d2.Nsp, uint64(i)) {
			col[i] = false
		}
	}
	return col, nil
}

func nequalConst_Const(d1, d2 *vector.Vector) ([]bool, error) {
	lvs, ok := d1.Col.(*types.Bytes)
	if !ok {
		return nil, errors.New("the left col type is not *types.Bytes")
	}
	rvs, ok := d2.Col.(*types.Bytes)
	if !ok {
		return nil, errors.New("the right col type is not *types.Bytes")
	}
	return []bool{string(lvs.Data) != string(rvs.Data)}, nil
}

func InitStrNeOpFuncMap() {
	for i := 0; i < len(StrEqOpFuncVec); i++ {
		StrNeOpFuncMap[i] = StrNeOpFuncVec[i]
	}
}

func ColNeCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	col, err := GetRetCol[T](lv, rv, col_col, NeOpFuncMap, StrNeOpFuncMap)
	if err != nil {
		return nil, err
	}
	vector.SetCol(vec, col)
	return vec, nil
}

func ColNeConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	col, err := GetRetCol[T](lv, rv, col_const, NeOpFuncMap, StrNeOpFuncMap)
	if err != nil {
		return nil, err
	}
	vector.SetCol(vec, col)
	return vec, nil
}

func ColNeNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func ConstNeCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ColNeConst[T](rv, lv, proc)
}

func ConstNeConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec := proc.AllocScalarVector(proc.GetBoolTyp(lv.Typ))
	col, err := GetRetCol[T](lv, rv, const_const, NeOpFuncMap, StrNeOpFuncMap)
	if err != nil {
		return nil, err
	}
	vector.SetCol(vec, col)
	return vec, nil
}

func ConstNeNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullNeCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullNeConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullNeNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

type NeFunc = func(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error)

var NeFuncMap = map[int]NeFunc{}

var NeFuncVec = []NeFunc{
	ColNeCol[int8], ColNeCol[int16], ColNeCol[int32], ColNeCol[int64], ColNeCol[uint8], ColNeCol[uint16],
	ColNeCol[uint32], ColNeCol[uint64], ColNeCol[float32], ColNeCol[float64], ColNeCol[string], ColNeCol[bool],
	ColNeCol[types.Date], ColNeCol[types.Datetime], ColNeCol[types.Decimal64], ColNeCol[types.Decimal128],

	ColNeConst[int8], ColNeConst[int16], ColNeConst[int32], ColNeConst[int64], ColNeConst[uint8], ColNeConst[uint16],
	ColNeConst[uint32], ColNeConst[uint64], ColNeConst[float32], ColNeConst[float64], ColNeConst[string], ColNeConst[bool],
	ColNeConst[types.Date], ColNeConst[types.Datetime], ColNeConst[types.Decimal64], ColNeConst[types.Decimal128],

	ColNeNull[int8], ColNeNull[int16], ColNeNull[int32], ColNeNull[int64], ColNeNull[uint8], ColNeNull[uint16],
	ColNeNull[uint32], ColNeNull[uint64], ColNeNull[float32], ColNeNull[float64], ColNeNull[string], ColNeNull[bool],
	ColNeNull[types.Date], ColNeNull[types.Datetime], ColNeNull[types.Decimal64], ColNeNull[types.Decimal128],

	ConstNeCol[int8], ConstNeCol[int16], ConstNeCol[int32], ConstNeCol[int64], ConstNeCol[uint8], ConstNeCol[uint16],
	ConstNeCol[uint32], ConstNeCol[uint64], ConstNeCol[float32], ConstNeCol[float64], ConstNeCol[string], ConstNeCol[bool],
	ConstNeCol[types.Date], ConstNeCol[types.Datetime], ConstNeCol[types.Decimal64], ConstNeCol[types.Decimal128],

	ConstNeConst[int8], ConstNeConst[int16], ConstNeConst[int32], ConstNeConst[int64], ConstNeConst[uint8], ConstNeConst[uint16],
	ConstNeConst[uint32], ConstNeConst[uint64], ConstNeConst[float32], ConstNeConst[float64], ConstNeConst[string], ConstNeConst[bool],
	ConstNeConst[types.Date], ConstNeConst[types.Datetime], ConstNeConst[types.Decimal64], ConstNeConst[types.Decimal128],

	ConstNeNull[int8], ConstNeNull[int16], ConstNeNull[int32], ConstNeNull[int64], ConstNeNull[uint8], ConstNeNull[uint16],
	ConstNeNull[uint32], ConstNeNull[uint64], ConstNeNull[float32], ConstNeNull[float64], ConstNeNull[string], ConstNeNull[bool],
	ConstNeNull[types.Date], ConstNeNull[types.Datetime], ConstNeNull[types.Decimal64], ConstNeNull[types.Decimal128],

	NullNeCol[int8], NullNeCol[int16], NullNeCol[int32], NullNeCol[int64], NullNeCol[uint8], NullNeCol[uint16],
	NullNeCol[uint32], NullNeCol[uint64], NullNeCol[float32], NullNeCol[float64], NullNeCol[string], NullNeCol[bool],
	NullNeCol[types.Date], NullNeCol[types.Datetime], NullNeCol[types.Decimal64], NullNeCol[types.Decimal128],

	NullNeConst[int8], NullNeConst[int16], NullNeConst[int32], NullNeConst[int64], NullNeConst[uint8], NullNeConst[uint16],
	NullNeConst[uint32], NullNeConst[uint64], NullNeConst[float32], NullNeConst[float64], NullNeConst[string], NullNeConst[bool],
	NullNeConst[types.Date], NullNeConst[types.Datetime], NullNeConst[types.Decimal64], NullNeConst[types.Decimal128],

	NullNeNull[int8], NullNeNull[int16], NullNeNull[int32], NullNeNull[int64], NullNeNull[uint8], NullNeNull[uint16],
	NullNeNull[uint32], NullNeNull[uint64], NullNeNull[float32], NullNeNull[float64], NullNeNull[string], NullNeNull[bool],
	NullNeNull[types.Date], NullNeNull[types.Datetime], NullNeNull[types.Decimal64], NullNeNull[types.Decimal128],
}

func InitNeFuncMap() {
	InitNeOpFuncMap()
	InitStrNeOpFuncMap()
	for i := 0; i < len(NeFuncVec); i++ {
		NeFuncMap[i] = NeFuncVec[i]
	}
}

func NeDataValue[T DataValue](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	lt, rt := GetTypeID(lv), GetTypeID(rv)
	dataID := GetDatatypeID[T]()
	vec, err := NeFuncMap[(lt*3+rt)*dataTypeNum+dataID](lv, rv, proc)
	if err != nil {
		return nil, errors.New("Ne function: " + err.Error())
	}
	return vec, nil
}*/


func allocateBoolVector(length int64, proc *process.Process) (*vector.Vector, error) {
	vec, err := proc.AllocVector(retType, length)
	if err != nil {
		return nil, err
	}
	vec.Col = encoding.DecodeBoolSlice(vec.Data)
	vec.Col = vec.Col.([]bool)[:length]
	return vec, nil
}

func FillNullPos(vec *vector.Vector) {
	if nulls.Any(vec.Nsp) {
		rows := vec.Nsp.Np.ToArray()
		cols := vec.Col.([]bool)
		for _, row := range rows {
			cols[row] = false
		}
	}
}

func ScalarNeNotScalar[T NormalType](sv, nsv *vector.Vector, col1, col2 []T, proc *process.Process) (*vector.Vector, error) {
	length := int64(vector.Length(sv))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	value := col1[0]
	for i := range vcols {
		vcols[i] = value != col2[i]
	}
	vec.Nsp.Or(nsv.Nsp)
	FillNullPos(vec)
	return vec, nil
}

func NeGeneral[T NormalType](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]
	col1, col2 := vector.MustTCols[T](v1), vector.MustTCols[T](v2)
	if v1.IsScalarNull() || v2.IsScalarNull() {
		return proc.AllocScalarNullVector(retType), nil
	}
	c1, c2 := v1.IsScalar(), v2.IsScalar()
	switch {
	case c1 && c2:
		vec := proc.AllocScalarVector(retType)
		vec.Col = make([]bool, 1)
		vec.Col.([]bool)[0] = col1[0] != col2[0]
		return vec, nil
	case c1 && !c2:
		return ScalarNeNotScalar[T](v1, v2, col1, col2, proc)
	case !c1 && c2:
		return ScalarNeNotScalar[T](v2, v1, col2, col1, proc)
	}
	// case !c1 && !c2
	length := int64(vector.Length(v1))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	for i := range vcols {
		vcols[i] = col1[i] != col2[i]
	}
	nulls.Or(v1.Nsp, v2.Nsp, vec.Nsp)
	FillNullPos(vec)
	fmt.Println("wangjian sql0c is", vec.Col, vec.Nsp)
	return vec, nil
}

func isBytesNe(b1, b2 []byte) bool {
	if len(b1) != len(b2) {
		return false
	}
	return !bytes.Equal(b1, b2)
}

func ScalarStringEqNotScalar(sv, nsv *vector.Vector, str []byte, col *types.Bytes, proc *process.Process) (*vector.Vector, error) {
	var i int64
	length := int64(vector.Length(sv))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	for i = 0; i < length; i++ {
		vcols[i] = isBytesNe(str, col.Get(i))
	}
	vec.Nsp.Or(nsv.Nsp)
	FillNullPos(vec)
	return vec, nil
}

func NeString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]
	col1, col2 := vector.MustBytesCols(v1), vector.MustBytesCols(v2)

	if v1.IsScalarNull() || v2.IsScalarNull() {
		return proc.AllocScalarNullVector(retType), nil
	}

	c1, c2 := v1.IsScalar(), v2.IsScalar()
	switch {
	case c1 && c2:
		vec := proc.AllocScalarVector(retType)
		vec.Col = make([]bool, 1)
		vec.Col.([]bool)[0] = isBytesNe(v1.Data, v2.Data)
		return vec, nil
	case c1 && !c2:
		return ScalarStringEqNotScalar(v1, v2, col1.Get(0), col2, proc)
	case !c1 && c2:
		return ScalarStringEqNotScalar(v2, v1, col2.Get(0), col1, proc)
	}
	length := int64(vector.Length(v1))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	for i := range vcols {
		j := int64(i)
		vcols[i] = isBytesNe(col1.Get(j), col2.Get(j))
	}
	nulls.Or(v1.Nsp, v2.Nsp, vec.Nsp)
	FillNullPos(vec)
	return vec, nil
}


