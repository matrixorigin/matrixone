package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/ne"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func nequal[T OrderedValue](d1, d2 interface{}, aScale, bScale int32) bool {
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

func nequalCol_Col(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
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
		} else {
			col[i] = false
		}
	}
	return col
}

func nequalCol_Const(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
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
		} else {
			col[i] = false
		}
	}
	return col
}

func nequalConst_Col(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
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
		} else {
			col[i] = false
		}
	}
	return col
}

func nequalConst_Const(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	return []bool{string(lvs.Data) != string(rvs.Data)}
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
	vector.SetCol(vec, GetRetCol[T](lv, rv, col_col, NeOpFuncMap, StrNeOpFuncMap))
	return vec, nil
}

func ColNeConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, col_const, NeOpFuncMap, StrNeOpFuncMap))
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
	vector.SetCol(vec, GetRetCol[T](lv, rv, const_const, NeOpFuncMap, StrNeOpFuncMap))
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
		return nil, err
	}
	return vec, nil
}
