package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/ge"
	"github.com/matrixorigin/matrixone/pkg/vectorize/le"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func gequal[T OrderedValue](d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(T), d2.(T)
	return l >= v
}

func gequal_B(d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(bool), d2.(bool)
	return l || !v
}

func gequal_D(d1, d2 interface{}, aScale, bScale int32) bool {
	l, v := d1.(types.Decimal128), d2.(types.Decimal128)
	return types.CompareDecimal128Decimal128(l, v, aScale, bScale) >= 0
}

var GeOpFuncMap = map[int]CompOpFunc{}

var GeOpFuncVec = []CompOpFunc{
	gequal[int8], gequal[int16], gequal[int32], gequal[int64], gequal[uint8], gequal[uint16], gequal[uint32],
	gequal[uint64], gequal[float32], gequal[float64], gequal[string], gequal_B, gequal[types.Date],
	gequal[types.Datetime], gequal[types.Decimal64], gequal_D,
}

func InitGeOpFuncMap() {
	for i := 0; i < len(GeOpFuncVec); i++ {
		GeOpFuncMap[i] = GeOpFuncVec[i]
	}
}

var StrGeOpFuncMap = map[int]StrCompOpFunc{}

var StrGeOpFuncVec = []StrCompOpFunc{
	gequalCol_Col, gequalCol_Const, gequalConst_Col, gequalConst_Const,
}

func gequalCol_Col(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(lvs.Lengths))
	rs = ge.StrGe(lvs, rvs, rs)
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

func gequalCol_Const(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(lvs.Lengths))
	rs = le.StrLeScalar(rvs.Data, lvs, rs)
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

func gequalConst_Col(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	rs := make([]int64, len(rvs.Lengths))
	rs = ge.StrGeScalar(lvs.Data, rvs, rs)
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

func gequalConst_Const(d1, d2 interface{}) []bool {
	lvs, rvs := d1.(*types.Bytes), d2.(*types.Bytes)
	return []bool{string(lvs.Data) >= string(rvs.Data)}
}

func InitStrGeOpFuncMap() {
	for i := 0; i < len(StrGeOpFuncVec); i++ {
		StrGeOpFuncMap[i] = StrGeOpFuncVec[i]
	}
}

func ColGeCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, col_col, GeOpFuncMap, StrGeOpFuncMap))
	return vec, nil
}

func ColGeConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, col_const, GeOpFuncMap, StrGeOpFuncMap))
	return vec, nil
}

func ColGeNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func ConstGeCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	n := GetRetColLen[T](lv)
	vec, err := proc.AllocVector(proc.GetBoolTyp(lv.Typ), int64(n)*1)
	if err != nil {
		return nil, err
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, GetRetCol[T](lv, rv, const_col, GeOpFuncMap, StrGeOpFuncMap))
	return vec, nil
}

func ConstGeConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec := proc.AllocScalarVector(proc.GetBoolTyp(lv.Typ))
	vector.SetCol(vec, GetRetCol[T](lv, rv, const_const, GeOpFuncMap, StrGeOpFuncMap))
	return vec, nil
}

func ConstGeNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullGeCol[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullGeConst[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

func NullGeNull[T DataValue](lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(proc.GetBoolTyp(lv.Typ)), nil
}

type GeFunc = func(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error)

var GeFuncMap = map[int]GeFunc{}

var GeFuncVec = []GeFunc{
	ColGeCol[int8], ColGeCol[int16], ColGeCol[int32], ColGeCol[int64], ColGeCol[uint8], ColGeCol[uint16],
	ColGeCol[uint32], ColGeCol[uint64], ColGeCol[float32], ColGeCol[float64], ColGeCol[string], ColGeCol[bool],
	ColGeCol[types.Date], ColGeCol[types.Datetime], ColGeCol[types.Decimal64], ColGeCol[types.Decimal128],

	ColGeConst[int8], ColGeConst[int16], ColGeConst[int32], ColGeConst[int64], ColGeConst[uint8], ColGeConst[uint16],
	ColGeConst[uint32], ColGeConst[uint64], ColGeConst[float32], ColGeConst[float64], ColGeConst[string], ColGeConst[bool],
	ColGeConst[types.Date], ColGeConst[types.Datetime], ColGeConst[types.Decimal64], ColGeConst[types.Decimal128],

	ColGeNull[int8], ColGeNull[int16], ColGeNull[int32], ColGeNull[int64], ColGeNull[uint8], ColGeNull[uint16],
	ColGeNull[uint32], ColGeNull[uint64], ColGeNull[float32], ColGeNull[float64], ColGeNull[string], ColGeNull[bool],
	ColGeNull[types.Date], ColGeNull[types.Datetime], ColGeNull[types.Decimal64], ColGeNull[types.Decimal128],

	ConstGeCol[int8], ConstGeCol[int16], ConstGeCol[int32], ConstGeCol[int64], ConstGeCol[uint8], ConstGeCol[uint16],
	ConstGeCol[uint32], ConstGeCol[uint64], ConstGeCol[float32], ConstGeCol[float64], ConstGeCol[string], ConstGeCol[bool],
	ConstGeCol[types.Date], ConstGeCol[types.Datetime], ConstGeCol[types.Decimal64], ConstGeCol[types.Decimal128],

	ConstGeConst[int8], ConstGeConst[int16], ConstGeConst[int32], ConstGeConst[int64], ConstGeConst[uint8], ConstGeConst[uint16],
	ConstGeConst[uint32], ConstGeConst[uint64], ConstGeConst[float32], ConstGeConst[float64], ConstGeConst[string], ConstGeConst[bool],
	ConstGeConst[types.Date], ConstGeConst[types.Datetime], ConstGeConst[types.Decimal64], ConstGeConst[types.Decimal128],

	ConstGeNull[int8], ConstGeNull[int16], ConstGeNull[int32], ConstGeNull[int64], ConstGeNull[uint8], ConstGeNull[uint16],
	ConstGeNull[uint32], ConstGeNull[uint64], ConstGeNull[float32], ConstGeNull[float64], ConstGeNull[string], ConstGeNull[bool],
	ConstGeNull[types.Date], ConstGeNull[types.Datetime], ConstGeNull[types.Decimal64], ConstGeNull[types.Decimal128],

	NullGeCol[int8], NullGeCol[int16], NullGeCol[int32], NullGeCol[int64], NullGeCol[uint8], NullGeCol[uint16],
	NullGeCol[uint32], NullGeCol[uint64], NullGeCol[float32], NullGeCol[float64], NullGeCol[string], NullGeCol[bool],
	NullGeCol[types.Date], NullGeCol[types.Datetime], NullGeCol[types.Decimal64], NullGeCol[types.Decimal128],

	NullGeConst[int8], NullGeConst[int16], NullGeConst[int32], NullGeConst[int64], NullGeConst[uint8], NullGeConst[uint16],
	NullGeConst[uint32], NullGeConst[uint64], NullGeConst[float32], NullGeConst[float64], NullGeConst[string], NullGeConst[bool],
	NullGeConst[types.Date], NullGeConst[types.Datetime], NullGeConst[types.Decimal64], NullGeConst[types.Decimal128],

	NullGeNull[int8], NullGeNull[int16], NullGeNull[int32], NullGeNull[int64], NullGeNull[uint8], NullGeNull[uint16],
	NullGeNull[uint32], NullGeNull[uint64], NullGeNull[float32], NullGeNull[float64], NullGeNull[string], NullGeNull[bool],
	NullGeNull[types.Date], NullGeNull[types.Datetime], NullGeNull[types.Decimal64], NullGeNull[types.Decimal128],
}

func InitGeFuncMap() {
	InitGeOpFuncMap()
	InitStrGeOpFuncMap()
	for i := 0; i < len(GeFuncVec); i++ {
		GeFuncMap[i] = GeFuncVec[i]
	}
}

func GeDataValue[T DataValue](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	lt, rt := GetTypeID(lv), GetTypeID(rv)
	dataID := GetDatatypeID[T]()
	vec, err := GeFuncMap[(lt*3+rt)*dataTypeNum+dataID](lv, rv, proc)
	if err != nil {
		return nil, err
	}
	return vec, nil
}
