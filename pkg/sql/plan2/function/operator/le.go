package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/ge"
	"github.com/matrixorigin/matrixone/pkg/vectorize/le"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func lequal[T OrderedValue](d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(T), d2.(T)
	return l <= v
}

func lequal_B(d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(bool), d2.(bool)
	return !l || v
}

func lequal_D(d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(types.Decimal128), d2.(types.Decimal128)
	return types.CompareDecimal128Decimal128(l, v, aScale, bScale) <= 0
}

type LeOpFunc = func(d1, d2 interface{}, aScale, bScale int32) bool

var LeOpFuncMap = map[int]LeOpFunc{}

var LeOpFuncVec = []LeOpFunc{
	lequal[int8], lequal[int16], lequal[int32], lequal[int64], lequal[uint8], lequal[uint16], lequal[uint32],
	lequal[uint64], lequal[float32], lequal[float64], lequal[string], lequal_B, lequal[types.Date],
	lequal[types.Datetime], lequal[types.Decimal64], lequal_D,
}

func InitLeOpFuncMap() {
	for i := 0; i < len(LeOpFuncVec); i++ {
		LeOpFuncMap[i] = LeOpFuncVec[i]
	}
}

var StrLeOpFuncMap = map[int]StrCompOpFunc{}

var StrLeOpFuncVec = []StrCompOpFunc{
	lequalCol_Col, lequalCol_Const, lequalConst_Col, lequalConst_Const,
}

func lequalCol_Col(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(lvs.Lengths))
	rs = le.StrLe(lvs, rvs, rs)
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

func lequalCol_Const(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(lvs.Lengths))
	rs = ge.StrGeScalar(rvs.Data, lvs, rs)
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

func lequalConst_Col(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(rvs.Lengths))
	rs = le.StrLeScalar(lvs.Data, rvs, rs)
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

func lequalConst_Const(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	return []bool{string(lvs.Data) <= string(rvs.Data)}
}

func InitStrLeOpFuncMap() {
	for i := 0; i < len(StrLeOpFuncVec); i++ {
		StrLeOpFuncMap[i] = StrLeOpFuncVec[i]
	}
}

func ColLeCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, col_col, LeOpFuncMap, StrLeOpFuncMap))
	return vec, nil
}

func ColLeConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, col_const, LeOpFuncMap, StrLeOpFuncMap))
	return vec, nil
}

func ColLeNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func ConstLeCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, const_col, LeOpFuncMap, StrLeOpFuncMap))
	return vec, nil
}

func ConstLeConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec := proc.AllocScalarVector(proc.GetBoolTyp(lv.Typ))
	vector.SetCol(vec, GetRetCol[T](lv, rv, const_const, LeOpFuncMap, StrLeOpFuncMap))
	return vec, nil
}

func ConstLeNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullLeCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullLeConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullLeNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

type LeFunc = func(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error)

var LeFuncMap = map[int]LeFunc{}

var LeFuncVec = []LeFunc{
	ColLeCol[int8], ColLeCol[int16], ColLeCol[int32], ColLeCol[int64], ColLeCol[uint8], ColLeCol[uint16],
	ColLeCol[uint32], ColLeCol[uint64], ColLeCol[float32], ColLeCol[float64], ColLeCol[string], ColLeCol[bool],
	ColLeCol[types.Date], ColLeCol[types.Datetime], ColLeCol[types.Decimal64], ColLeCol[types.Decimal128],

	ColLeConst[int8], ColLeConst[int16], ColLeConst[int32], ColLeConst[int64], ColLeConst[uint8], ColLeConst[uint16],
	ColLeConst[uint32], ColLeConst[uint64], ColLeConst[float32], ColLeConst[float64], ColLeConst[string], ColLeConst[bool],
	ColLeConst[types.Date], ColLeConst[types.Datetime], ColLeConst[types.Decimal64], ColLeConst[types.Decimal128],

	ColLeNull[int8], ColLeNull[int16], ColLeNull[int32], ColLeNull[int64], ColLeNull[uint8], ColLeNull[uint16],
	ColLeNull[uint32], ColLeNull[uint64], ColLeNull[float32], ColLeNull[float64], ColLeNull[string], ColLeNull[bool],
	ColLeNull[types.Date], ColLeNull[types.Datetime], ColLeNull[types.Decimal64], ColLeNull[types.Decimal128],

	ConstLeCol[int8], ConstLeCol[int16], ConstLeCol[int32], ConstLeCol[int64], ConstLeCol[uint8], ConstLeCol[uint16],
	ConstLeCol[uint32], ConstLeCol[uint64], ConstLeCol[float32], ConstLeCol[float64], ConstLeCol[string], ConstLeCol[bool],
	ConstLeCol[types.Date], ConstLeCol[types.Datetime], ConstLeCol[types.Decimal64], ConstLeCol[types.Decimal128],

	ConstLeConst[int8], ConstLeConst[int16], ConstLeConst[int32], ConstLeConst[int64], ConstLeConst[uint8], ConstLeConst[uint16],
	ConstLeConst[uint32], ConstLeConst[uint64], ConstLeConst[float32], ConstLeConst[float64], ConstLeConst[string], ConstLeConst[bool],
	ConstLeConst[types.Date], ConstLeConst[types.Datetime], ConstLeConst[types.Decimal64], ConstLeConst[types.Decimal128],

	ConstLeNull[int8], ConstLeNull[int16], ConstLeNull[int32], ConstLeNull[int64], ConstLeNull[uint8], ConstLeNull[uint16],
	ConstLeNull[uint32], ConstLeNull[uint64], ConstLeNull[float32], ConstLeNull[float64], ConstLeNull[string], ConstLeNull[bool],
	ConstLeNull[types.Date], ConstLeNull[types.Datetime], ConstLeNull[types.Decimal64], ConstLeNull[types.Decimal128],

	NullLeCol[int8], NullLeCol[int16], NullLeCol[int32], NullLeCol[int64], NullLeCol[uint8], NullLeCol[uint16],
	NullLeCol[uint32], NullLeCol[uint64], NullLeCol[float32], NullLeCol[float64], NullLeCol[string], NullLeCol[bool],
	NullLeCol[types.Date], NullLeCol[types.Datetime], NullLeCol[types.Decimal64], NullLeCol[types.Decimal128],

	NullLeConst[int8], NullLeConst[int16], NullLeConst[int32], NullLeConst[int64], NullLeConst[uint8], NullLeConst[uint16],
	NullLeConst[uint32], NullLeConst[uint64], NullLeConst[float32], NullLeConst[float64], NullLeConst[string], NullLeConst[bool],
	NullLeConst[types.Date], NullLeConst[types.Datetime], NullLeConst[types.Decimal64], NullLeConst[types.Decimal128],

	NullLeNull[int8], NullLeNull[int16], NullLeNull[int32], NullLeNull[int64], NullLeNull[uint8], NullLeNull[uint16],
	NullLeNull[uint32], NullLeNull[uint64], NullLeNull[float32], NullLeNull[float64], NullLeNull[string], NullLeNull[bool],
	NullLeNull[types.Date], NullLeNull[types.Datetime], NullLeNull[types.Decimal64], NullLeNull[types.Decimal128],
}

func InitLeFuncMap() {
	InitLeOpFuncMap()
	InitStrLeOpFuncMap()
	for i := 0; i < len(LeFuncVec); i++ {
		LeFuncMap[i] = LeFuncVec[i]
	}
}

func LeDataValue[T DataValue](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	lt, rt := GetTypeID(lv), GetTypeID(rv)
	dataID := GetDatatypeID[T]()
	vec, err := LeFuncMap[(lt*3+rt)*dataTypeNum+dataID](lv, rv, proc)
	if err != nil {
		return nil, err
	}
	return vec, nil
}
