package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/gt"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lt"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func less[T OrderedValue](d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(T), d2.(T)
	return l < v
}

func less_B(d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(bool), d2.(bool)
	return !l && v
}

func less_D(d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(types.Decimal128), d2.(types.Decimal128)
	return types.CompareDecimal128Decimal128(l, v, aScale, bScale) < 0
}

type LtOpFunc = func(d1, d2 interface{}, aScale, bScale int32) bool

var LtOpFuncMap = map[int]LtOpFunc{}

var LtOpFuncVec = []LtOpFunc{
	less[int8], less[int16], less[int32], less[int64], less[uint8], less[uint16], less[uint32],
	less[uint64], less[float32], less[float64], less[string], less_B, less[types.Date],
	less[types.Datetime], less[types.Decimal64], less_D,
}

func InitLtOpFuncMap() {
	for i := 0; i < len(LtOpFuncVec); i++ {
		LtOpFuncMap[i] = LtOpFuncVec[i]
	}
}

var StrLtOpFuncMap = map[int]StrCompOpFunc{}

var StrLtOpFuncVec = []StrCompOpFunc{
	lessCol_Col, lessCol_Const, lessConst_Col, lessConst_Const,
}

func lessCol_Col(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(lvs.Lengths))
	rs = lt.StrLt(lvs, rvs, rs)
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

func lessCol_Const(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(lvs.Lengths))
	rs = gt.StrGtScalar(rvs.Data, lvs, rs)
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

func lessConst_Col(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(rvs.Lengths))
	rs = lt.StrLtScalar(lvs.Data, rvs, rs)
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

func lessConst_Const(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	return []bool{string(lvs.Data) < string(rvs.Data)}
}

func InitStrLtOpFuncMap() {
	for i := 0; i < len(StrLeOpFuncVec); i++ {
		StrLtOpFuncMap[i] = StrLtOpFuncVec[i]
	}
}

func ColLtCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, col_col, LtOpFuncMap, StrLtOpFuncMap))
	return vec, nil
}

func ColLtConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, col_const, LtOpFuncMap, StrLtOpFuncMap))
	return vec, nil
}

func ColLtNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func ConstLtCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, const_col, LtOpFuncMap, StrLtOpFuncMap))
	return vec, nil
}

func ConstLtConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec := proc.AllocScalarVector(proc.GetBoolTyp(lv.Typ))
	vector.SetCol(vec, GetRetCol[T](lv, rv, const_const, LtOpFuncMap, StrLtOpFuncMap))
	return vec, nil
}

func ConstLtNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullLtCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullLtConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullLtNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

type LtFunc = func(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error)

var LtFuncMap = map[int]LtFunc{}

var LtFuncVec = []LtFunc{
	ColLtCol[int8], ColLtCol[int16], ColLtCol[int32], ColLtCol[int64], ColLtCol[uint8], ColLtCol[uint16],
	ColLtCol[uint32], ColLtCol[uint64], ColLtCol[float32], ColLtCol[float64], ColLtCol[string], ColLtCol[bool],
	ColLtCol[types.Date], ColLtCol[types.Datetime], ColLtCol[types.Decimal64], ColLtCol[types.Decimal128],

	ColLtConst[int8], ColLtConst[int16], ColLtConst[int32], ColLtConst[int64], ColLtConst[uint8], ColLtConst[uint16],
	ColLtConst[uint32], ColLtConst[uint64], ColLtConst[float32], ColLtConst[float64], ColLtConst[string], ColLtConst[bool],
	ColLtConst[types.Date], ColLtConst[types.Datetime], ColLtConst[types.Decimal64], ColLtConst[types.Decimal128],

	ColLtNull[int8], ColLtNull[int16], ColLtNull[int32], ColLtNull[int64], ColLtNull[uint8], ColLtNull[uint16],
	ColLtNull[uint32], ColLtNull[uint64], ColLtNull[float32], ColLtNull[float64], ColLtNull[string], ColLtNull[bool],
	ColLtNull[types.Date], ColLtNull[types.Datetime], ColLtNull[types.Decimal64], ColLtNull[types.Decimal128],

	ConstLtCol[int8], ConstLtCol[int16], ConstLtCol[int32], ConstLtCol[int64], ConstLtCol[uint8], ConstLtCol[uint16],
	ConstLtCol[uint32], ConstLtCol[uint64], ConstLtCol[float32], ConstLtCol[float64], ConstLtCol[string], ConstLtCol[bool],
	ConstLtCol[types.Date], ConstLtCol[types.Datetime], ConstLtCol[types.Decimal64], ConstLtCol[types.Decimal128],

	ConstLtConst[int8], ConstLtConst[int16], ConstLtConst[int32], ConstLtConst[int64], ConstLtConst[uint8], ConstLtConst[uint16],
	ConstLtConst[uint32], ConstLtConst[uint64], ConstLtConst[float32], ConstLtConst[float64], ConstLtConst[string], ConstLtConst[bool],
	ConstLtConst[types.Date], ConstLtConst[types.Datetime], ConstLtConst[types.Decimal64], ConstLtConst[types.Decimal128],

	ConstLtNull[int8], ConstLtNull[int16], ConstLtNull[int32], ConstLtNull[int64], ConstLtNull[uint8], ConstLtNull[uint16],
	ConstLtNull[uint32], ConstLtNull[uint64], ConstLtNull[float32], ConstLtNull[float64], ConstLtNull[string], ConstLtNull[bool],
	ConstLtNull[types.Date], ConstLtNull[types.Datetime], ConstLtNull[types.Decimal64], ConstLtNull[types.Decimal128],

	NullLtCol[int8], NullLtCol[int16], NullLtCol[int32], NullLtCol[int64], NullLtCol[uint8], NullLtCol[uint16],
	NullLtCol[uint32], NullLtCol[uint64], NullLtCol[float32], NullLtCol[float64], NullLtCol[string], NullLtCol[bool],
	NullLtCol[types.Date], NullLtCol[types.Datetime], NullLtCol[types.Decimal64], NullLtCol[types.Decimal128],

	NullLtConst[int8], NullLtConst[int16], NullLtConst[int32], NullLtConst[int64], NullLtConst[uint8], NullLtConst[uint16],
	NullLtConst[uint32], NullLtConst[uint64], NullLtConst[float32], NullLtConst[float64], NullLtConst[string], NullLtConst[bool],
	NullLtConst[types.Date], NullLtConst[types.Datetime], NullLtConst[types.Decimal64], NullLtConst[types.Decimal128],

	NullLtNull[int8], NullLtNull[int16], NullLtNull[int32], NullLtNull[int64], NullLtNull[uint8], NullLtNull[uint16],
	NullLtNull[uint32], NullLtNull[uint64], NullLtNull[float32], NullLtNull[float64], NullLtNull[string], NullLtNull[bool],
	NullLtNull[types.Date], NullLtNull[types.Datetime], NullLtNull[types.Decimal64], NullLtNull[types.Decimal128],
}

func InitLtFuncMap() {
	InitLtOpFuncMap()
	InitStrLtOpFuncMap()
	for i := 0; i < len(LtFuncVec); i++ {
		LtFuncMap[i] = LtFuncVec[i]
	}
}

func LtDataValue[T DataValue](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	lt, rt := GetTypeID(lv), GetTypeID(rv)
	dataID := GetDatatypeID[T]()
	vec, err := LtFuncMap[(lt*3+rt)*dataTypeNum+dataID](lv, rv, proc)
	if err != nil {
		return nil, err
	}
	return vec, nil
}
