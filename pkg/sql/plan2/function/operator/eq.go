package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/eq"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var dataTypeNum = 16

type OrderedValue interface {
	int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64 | string |
		types.Date | types.Datetime | types.Decimal64
}

type DataValue interface {
	OrderedValue | bool | types.Decimal128
}

func equal[T OrderedValue](d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(T), d2.(T)
	return l == v
}

func equal_B(d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(bool), d2.(bool)
	return l == v
}

func equal_D(d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(types.Decimal128), d2.(types.Decimal128)
	return types.CompareDecimal128Decimal128(l, v, aScale, bScale) == 0
}

type CompOpFunc = func(d1, d2 interface{}, aScale, bScale int32) bool

var EqOpFuncMap = map[int]CompOpFunc{}

var EqOpFuncVec = []CompOpFunc{
	equal[int8], equal[int16], equal[int32], equal[int64], equal[uint8], equal[uint16], equal[uint32],
	equal[uint64], equal[float32], equal[float64], equal[string], equal_B, equal[types.Date],
	equal[types.Datetime], equal[types.Decimal64], equal_D,
}

func InitEqOpFuncMap() {
	for i := 0; i < len(EqOpFuncVec); i++ {
		EqOpFuncMap[i] = EqOpFuncVec[i]
	}
}

type StrCompOpFunc = func(d1, d2 interface{}) []bool

var StrEqOpFuncMap = map[int]StrCompOpFunc{}

var StrEqOpFuncVec = []StrCompOpFunc{
	equalCol_Col, equalCol_Const, equalConst_Col, equalConst_Const,
}

func equalCol_Col(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(lvs.Lengths))
	rs = eq.StrEq(lvs, rvs, rs)
	col := make([]bool, len(lvs.Lengths))
	rsi := 0
	for i := 0; i < len(col); i++ {
		if rsi >= len(rs) {
			break
		}
		if int64(i) == rs[rsi] {
			col[i] = true
			rsi++
		} else {
			col[i] = false
		}
	}
	return col
}

func equalCol_Const(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(lvs.Lengths))
	rs = eq.StrEqScalar(rvs.Data, lvs, rs)
	col := make([]bool, len(lvs.Lengths))
	rsi := 0
	for i := 0; i < len(col); i++ {
		if rsi >= len(rs) {
			break
		}
		if int64(i) == rs[rsi] {
			col[i] = true
			rsi++
		} else {
			col[i] = false
		}
	}
	return col
}

func equalConst_Col(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(rvs.Lengths))
	rs = eq.StrEqScalar(lvs.Data, rvs, rs)
	col := make([]bool, len(rvs.Lengths))
	rsi := 0
	for i := 0; i < len(col); i++ {
		if rsi >= len(rs) {
			break
		}
		if int64(i) == rs[rsi] {
			col[i] = true
			rsi++
		} else {
			col[i] = false
		}
	}
	return col
}

func equalConst_Const(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	return []bool{string(lvs.Data) == string(rvs.Data)}
}

func InitStrEqOpFuncMap() {
	for i := 0; i < len(StrEqOpFuncVec); i++ {
		StrEqOpFuncMap[i] = StrEqOpFuncVec[i]
	}
}

func GetRetColLen[T DataValue](lv *vector.Vector) int {
	dataID := GetDatatypeID[T]()
	if dataID != 10 {
		return GetRetColLen_1[T](lv)
	} else {
		return GetRetColLen_2(lv)
	}
}

func GetRetColLen_1[T DataValue](lv *vector.Vector) int {
	lvs := lv.Col.([]T)
	return len(lvs)
}

func GetRetColLen_2(lv *vector.Vector) int {
	lvs := lv.Col.(*types.Bytes)
	return len(lvs.Offsets)
}

const (
	col_col = iota
	col_const
	const_col
	const_const
)

func GetRetCol[T DataValue](lv, rv *vector.Vector, colType int, FuncMap map[int]CompOpFunc, StrFuncMap map[int]StrCompOpFunc) []bool {
	dataID := GetDatatypeID[T]()
	if dataID != 10 {
		return GetRetCol_1[T](lv, rv, colType, FuncMap)
	} else {
		return GetRetCol_2(lv, rv, colType, StrFuncMap)
	}
}

func GetRetCol_1[T DataValue](lv, rv *vector.Vector, colType int, FuncMap map[int]CompOpFunc) []bool {
	lvs, rvs := lv.Col.([]T), rv.Col.([]T)
	var col []bool
	switch colType {
	case col_col:
		col = make([]bool, len(lvs))
		dataID := GetDatatypeID[T]()
		for i := 0; i < len(lvs); i++ {
			if FuncMap[dataID](lvs[i], rvs[i], lv.Typ.Scale, rv.Typ.Scale) {
				col[i] = true
			} else {
				col[i] = false
			}
		}
	case col_const:
		r := rvs[0]
		col = make([]bool, len(lvs))
		dataID := GetDatatypeID[T]()
		for i := 0; i < len(lvs); i++ {
			if FuncMap[dataID](lvs[i], r, lv.Typ.Scale, rv.Typ.Scale) {
				col[i] = true
			} else {
				col[i] = false
			}
		}
	case const_col:
		l := lvs[0]
		col = make([]bool, len(rvs))
		dataID := GetDatatypeID[T]()
		for i := 0; i < len(rvs); i++ {
			if FuncMap[dataID](l, rvs[i], lv.Typ.Scale, rv.Typ.Scale) {
				col[i] = true
			} else {
				col[i] = false
			}
		}
	case const_const:
		dataID := GetDatatypeID[T]()
		col = []bool{GeOpFuncMap[dataID](lvs[0], rvs[0], lv.Typ.Scale, rv.Typ.Scale)}
	}
	return col
}

func GetRetCol_2(lv, rv *vector.Vector, colType int, FuncMap map[int]StrCompOpFunc) []bool {
	var col []bool
	switch colType {
	case col_col:
		col = FuncMap[col_col](lv.Col, rv.Col)
	case col_const:
		col = FuncMap[col_const](lv.Col, rv.Col)
	case const_col:
		col = FuncMap[const_col](lv.Col, rv.Col)
	case const_const:
		col = FuncMap[const_const](lv.Col, rv.Col)
	}
	return col
}

func ColEqCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, col_col, EqOpFuncMap, StrEqOpFuncMap))
	return vec, nil
}

func ColEqConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, col_const, EqOpFuncMap, StrEqOpFuncMap))
	return vec, nil
}

func ColEqNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func ConstEqCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ColEqConst[T](rv, lv, proc)
}

func ConstEqConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec := proc.AllocScalarVector(proc.GetBoolTyp(lv.Typ))
	vector.SetCol(vec, GetRetCol[T](lv, rv, const_const, EqOpFuncMap, StrEqOpFuncMap))
	return vec, nil
}

func ConstEqNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullEqCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullEqConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullEqNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

type EqFunc = func(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error)

var EqFuncMap = map[int]EqFunc{}

var EqFuncVec = []EqFunc{
	ColEqCol[int8], ColEqCol[int16], ColEqCol[int32], ColEqCol[int64], ColEqCol[uint8], ColEqCol[uint16],
	ColEqCol[uint32], ColEqCol[uint64], ColEqCol[float32], ColEqCol[float64], ColEqCol[string], ColEqCol[bool],
	ColEqCol[types.Date], ColEqCol[types.Datetime], ColEqCol[types.Decimal64], ColEqCol[types.Decimal128],

	ColEqConst[int8], ColEqConst[int16], ColEqConst[int32], ColEqConst[int64], ColEqConst[uint8], ColEqConst[uint16],
	ColEqConst[uint32], ColEqConst[uint64], ColEqConst[float32], ColEqConst[float64], ColEqConst[string], ColEqConst[bool],
	ColEqConst[types.Date], ColEqConst[types.Datetime], ColEqConst[types.Decimal64], ColEqConst[types.Decimal128],

	ColEqNull[int8], ColEqNull[int16], ColEqNull[int32], ColEqNull[int64], ColEqNull[uint8], ColEqNull[uint16],
	ColEqNull[uint32], ColEqNull[uint64], ColEqNull[float32], ColEqNull[float64], ColEqNull[string], ColEqNull[bool],
	ColEqNull[types.Date], ColEqNull[types.Datetime], ColEqNull[types.Decimal64], ColEqNull[types.Decimal128],

	ConstEqCol[int8], ConstEqCol[int16], ConstEqCol[int32], ConstEqCol[int64], ConstEqCol[uint8], ConstEqCol[uint16],
	ConstEqCol[uint32], ConstEqCol[uint64], ConstEqCol[float32], ConstEqCol[float64], ConstEqCol[string], ConstEqCol[bool],
	ConstEqCol[types.Date], ConstEqCol[types.Datetime], ConstEqCol[types.Decimal64], ConstEqCol[types.Decimal128],

	ConstEqConst[int8], ConstEqConst[int16], ConstEqConst[int32], ConstEqConst[int64], ConstEqConst[uint8], ConstEqConst[uint16],
	ConstEqConst[uint32], ConstEqConst[uint64], ConstEqConst[float32], ConstEqConst[float64], ConstEqConst[string], ConstEqConst[bool],
	ConstEqConst[types.Date], ConstEqConst[types.Datetime], ConstEqConst[types.Decimal64], ConstEqConst[types.Decimal128],

	ConstEqNull[int8], ConstEqNull[int16], ConstEqNull[int32], ConstEqNull[int64], ConstEqNull[uint8], ConstEqNull[uint16],
	ConstEqNull[uint32], ConstEqNull[uint64], ConstEqNull[float32], ConstEqNull[float64], ConstEqNull[string], ConstEqNull[bool],
	ConstEqNull[types.Date], ConstEqNull[types.Datetime], ConstEqNull[types.Decimal64], ConstEqNull[types.Decimal128],

	NullEqCol[int8], NullEqCol[int16], NullEqCol[int32], NullEqCol[int64], NullEqCol[uint8], NullEqCol[uint16],
	NullEqCol[uint32], NullEqCol[uint64], NullEqCol[float32], NullEqCol[float64], NullEqCol[string], NullEqCol[bool],
	NullEqCol[types.Date], NullEqCol[types.Datetime], NullEqCol[types.Decimal64], NullEqCol[types.Decimal128],

	NullEqConst[int8], NullEqConst[int16], NullEqConst[int32], NullEqConst[int64], NullEqConst[uint8], NullEqConst[uint16],
	NullEqConst[uint32], NullEqConst[uint64], NullEqConst[float32], NullEqConst[float64], NullEqConst[string], NullEqConst[bool],
	NullEqConst[types.Date], NullEqConst[types.Datetime], NullEqConst[types.Decimal64], NullEqConst[types.Decimal128],

	NullEqNull[int8], NullEqNull[int16], NullEqNull[int32], NullEqNull[int64], NullEqNull[uint8], NullEqNull[uint16],
	NullEqNull[uint32], NullEqNull[uint64], NullEqNull[float32], NullEqNull[float64], NullEqNull[string], NullEqNull[bool],
	NullEqNull[types.Date], NullEqNull[types.Datetime], NullEqNull[types.Decimal64], NullEqNull[types.Decimal128],
}

func InitEqFuncMap() {
	InitEqOpFuncMap()
	InitStrEqOpFuncMap()
	for i := 0; i < len(EqFuncVec); i++ {
		EqFuncMap[i] = EqFuncVec[i]
	}
}

func GetDatatypeID[T DataValue]() int {
	var t T
	var ti interface{} = &t
	switch ti.(type) {
	case *int8:
		return 0
	case *int16:
		return 1
	case *int32:
		return 2
	case *int64:
		return 3
	case *uint8:
		return 4
	case *uint16:
		return 5
	case *uint32:
		return 6
	case *uint64:
		return 7
	case *float32:
		return 8
	case *float64:
		return 9
	case *string:
		return 10
	case *bool:
		return 11
	case *types.Date:
		return 12
	case *types.Datetime:
		return 13
	case *types.Decimal64:
		return 14
	case *types.Decimal128:
		return 15
	default:
		return -1
	}
}

func EqDataValue[T DataValue](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	lt, rt := GetTypeID(lv), GetTypeID(rv)
	dataID := GetDatatypeID[T]()
	vec, err := EqFuncMap[(lt*3+rt)*dataTypeNum+dataID](lv, rv, proc)
	if err != nil {
		return nil, err
	}
	return vec, nil
}
