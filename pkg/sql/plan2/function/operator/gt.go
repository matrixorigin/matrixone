package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/gt"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lt"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func great[T OrderedValue](d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(T), d2.(T)
	return l > v
}

func great_B(d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(bool), d2.(bool)
	return l && !v
}

func great_D(d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(types.Decimal128), d2.(types.Decimal128)
	return types.CompareDecimal128Decimal128(l, v, aScale, bScale) > 0
}

type GtOpFunc = func(d1, d2 interface{}, aScale, bScale int32) bool

var GtOpFuncMap = map[int]GtOpFunc{}

var GtOpFuncVec = []GtOpFunc{
	great[int8], great[int16], great[int32], great[int64], great[uint8], great[uint16], great[uint32],
	great[uint64], great[float32], great[float64], great[string], great_B, great[types.Date],
	great[types.Datetime], great[types.Decimal64], great_D,
}

func InitGtOpFuncMap() {
	for i := 0; i < len(GtOpFuncVec); i++ {
		GtOpFuncMap[i] = GtOpFuncVec[i]
	}
}

var StrGtOpFuncMap = map[int]StrCompOpFunc{}

var StrGtOpFuncVec = []StrCompOpFunc{
	greatCol_Col, greatCol_Const, greatConst_Col, greatConst_Const,
}

func greatCol_Col(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(lvs.Lengths))
	rs = gt.StrGt(lvs, rvs, rs)
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

func greatCol_Const(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(lvs.Lengths))
	rs = lt.StrLtScalar(rvs.Data, lvs, rs)
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

func greatConst_Col(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(rvs.Lengths))
	rs = gt.StrGtScalar(lvs.Data, rvs, rs)
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

func greatConst_Const(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	return []bool{string(lvs.Data) > string(rvs.Data)}
}

func InitStrGtOpFuncMap() {
	for i := 0; i < len(StrGtOpFuncVec); i++ {
		StrGtOpFuncMap[i] = StrGtOpFuncVec[i]
	}
}

func ColGtCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, col_col, GtOpFuncMap, StrGtOpFuncMap))
	return vec, nil
}

func ColGtConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, col_const, GtOpFuncMap, StrGtOpFuncMap))
	return vec, nil
}

func ColGtNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func ConstGtCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, const_col, GtOpFuncMap, StrGtOpFuncMap))
	return vec, nil
}

func ConstGtConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec := proc.AllocScalarVector(proc.GetBoolTyp(lv.Typ))
	vector.SetCol(vec, GetRetCol[T](lv, rv, const_const, GtOpFuncMap, StrGtOpFuncMap))
	return vec, nil
}

func ConstGtNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullGtCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullGtConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullGtNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

type GtFunc = func(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error)

var GtFuncMap = map[int]GtFunc{}

var GtFuncVec = []GtFunc{
	ColGtCol[int8], ColGtCol[int16], ColGtCol[int32], ColGtCol[int64], ColGtCol[uint8], ColGtCol[uint16],
	ColGtCol[uint32], ColGtCol[uint64], ColGtCol[float32], ColGtCol[float64], ColGtCol[string], ColGtCol[bool],
	ColGtCol[types.Date], ColGtCol[types.Datetime], ColGtCol[types.Decimal64], ColGtCol[types.Decimal128],

	ColGtConst[int8], ColGtConst[int16], ColGtConst[int32], ColGtConst[int64], ColGtConst[uint8], ColGtConst[uint16],
	ColGtConst[uint32], ColGtConst[uint64], ColGtConst[float32], ColGtConst[float64], ColGtConst[string], ColGtConst[bool],
	ColGtConst[types.Date], ColGtConst[types.Datetime], ColGtConst[types.Decimal64], ColGtConst[types.Decimal128],

	ColGtNull[int8], ColGtNull[int16], ColGtNull[int32], ColGtNull[int64], ColGtNull[uint8], ColGtNull[uint16],
	ColGtNull[uint32], ColGtNull[uint64], ColGtNull[float32], ColGtNull[float64], ColGtNull[string], ColGtNull[bool],
	ColGtNull[types.Date], ColGtNull[types.Datetime], ColGtNull[types.Decimal64], ColGtNull[types.Decimal128],

	ConstGtCol[int8], ConstGtCol[int16], ConstGtCol[int32], ConstGtCol[int64], ConstGtCol[uint8], ConstGtCol[uint16],
	ConstGtCol[uint32], ConstGtCol[uint64], ConstGtCol[float32], ConstGtCol[float64], ConstGtCol[string], ConstGtCol[bool],
	ConstGtCol[types.Date], ConstGtCol[types.Datetime], ConstGtCol[types.Decimal64], ConstGtCol[types.Decimal128],

	ConstGtConst[int8], ConstGtConst[int16], ConstGtConst[int32], ConstGtConst[int64], ConstGtConst[uint8], ConstGtConst[uint16],
	ConstGtConst[uint32], ConstGtConst[uint64], ConstGtConst[float32], ConstGtConst[float64], ConstGtConst[string], ConstGtConst[bool],
	ConstGtConst[types.Date], ConstGtConst[types.Datetime], ConstGtConst[types.Decimal64], ConstGtConst[types.Decimal128],

	ConstGtNull[int8], ConstGtNull[int16], ConstGtNull[int32], ConstGtNull[int64], ConstGtNull[uint8], ConstGtNull[uint16],
	ConstGtNull[uint32], ConstGtNull[uint64], ConstGtNull[float32], ConstGtNull[float64], ConstGtNull[string], ConstGtNull[bool],
	ConstGtNull[types.Date], ConstGtNull[types.Datetime], ConstGtNull[types.Decimal64], ConstGtNull[types.Decimal128],

	NullGtCol[int8], NullGtCol[int16], NullGtCol[int32], NullGtCol[int64], NullGtCol[uint8], NullGtCol[uint16],
	NullGtCol[uint32], NullGtCol[uint64], NullGtCol[float32], NullGtCol[float64], NullGtCol[string], NullGtCol[bool],
	NullGtCol[types.Date], NullGtCol[types.Datetime], NullGtCol[types.Decimal64], NullGtCol[types.Decimal128],

	NullGtConst[int8], NullGtConst[int16], NullGtConst[int32], NullGtConst[int64], NullGtConst[uint8], NullGtConst[uint16],
	NullGtConst[uint32], NullGtConst[uint64], NullGtConst[float32], NullGtConst[float64], NullGtConst[string], NullGtConst[bool],
	NullGtConst[types.Date], NullGtConst[types.Datetime], NullGtConst[types.Decimal64], NullGtConst[types.Decimal128],

	NullGtNull[int8], NullGtNull[int16], NullGtNull[int32], NullGtNull[int64], NullGtNull[uint8], NullGtNull[uint16],
	NullGtNull[uint32], NullGtNull[uint64], NullGtNull[float32], NullGtNull[float64], NullGtNull[string], NullGtNull[bool],
	NullGtNull[types.Date], NullGtNull[types.Datetime], NullGtNull[types.Decimal64], NullGtNull[types.Decimal128],
}

func InitGtFuncMap() {
	InitGtOpFuncMap()
	InitStrGtOpFuncMap()
	for i := 0; i < len(GtFuncVec); i++ {
		GtFuncMap[i] = GtFuncVec[i]
	}
}

func GtDataValue[T DataValue](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	lt, rt := GetTypeID(lv), GetTypeID(rv)
	dataID := GetDatatypeID[T]()
	vec, err := GtFuncMap[(lt*3+rt)*dataTypeNum+dataID](lv, rv, proc)
	if err != nil {
		return nil, err
	}
	return vec, nil
}
