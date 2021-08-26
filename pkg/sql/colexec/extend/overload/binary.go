package overload

import (
	"errors"
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/add"
	"matrixone/pkg/vectorize/and"
	"matrixone/pkg/vectorize/div"
	"matrixone/pkg/vectorize/eq"
	"matrixone/pkg/vectorize/ge"
	"matrixone/pkg/vectorize/gt"
	"matrixone/pkg/vectorize/le"
	"matrixone/pkg/vectorize/lt"
	"matrixone/pkg/vectorize/mod"
	"matrixone/pkg/vectorize/mul"
	"matrixone/pkg/vectorize/ne"
	"matrixone/pkg/vectorize/or"
	"matrixone/pkg/vectorize/sub"
	"matrixone/pkg/vectorize/typecast"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
	"sync"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var pool = sync.Pool{
	New: func() interface{} {
		return make([]int64, 0, 16)
	},
}

var (
	// ErrDivByZero is reported on a division by zero.
	ErrDivByZero = errors.New("division by zero")
	// ErrZeroModulus is reported when computing the rest of a division by zero.
	ErrModByZero = errors.New("zero modulus")
)

func BinaryEval(op int, ltyp, rtyp types.T, lc, rc bool, lv, rv *vector.Vector, p *process.Process) (*vector.Vector, error) {
	if os, ok := BinOps[op]; ok {
		for _, o := range os {
			if binaryCheck(op, o.LeftType, o.RightType, ltyp, rtyp) {
				return o.Fn(lv, rv, p, lc, rc)
			}
		}
	}
	return nil, fmt.Errorf("%s not yet implemented for %s, %s", OpName[op], ltyp, rtyp)
}

func binaryCheck(op int, arg0, arg1 types.T, val0, val1 types.T) bool {
	return arg0 == val0 && arg1 == val1
}

// BinOps contains the binary operations indexed by operation type.
var BinOps = map[int][]*BinOp{
	Or: {
		&BinOp{
			LeftType:   types.T_sel,
			RightType:  types.T_sel,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs)+len(rvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:or.SelOr(lvs, rvs, rs)]
				vec.SetCol(rs)
				return vec, nil
			},
		},
	},
	And: {
		&BinOp{
			LeftType:   types.T_sel,
			RightType:  types.T_sel,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				n := len(lvs)
				if n < len(rvs) {
					n = len(rvs)
				}
				vec, err := register.Get(proc, int64(n)*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:and.SelAnd(lvs, rvs, rs)]
				vec.SetCol(rs)
				return vec, nil
			},
		},
	},
	Plus: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Int8AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Int8AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Int8Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Int16AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Int16AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Int16Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Int32AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Int32AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Int32Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Int64AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Int64AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Int64Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Uint8AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Uint8AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Uint8Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Uint16AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Uint16AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Uint16Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Uint32AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Uint32AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Uint32Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Uint64AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Uint64AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Uint64Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				switch {
				case lc && !rc:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Float32AddScalar(lvs[0], rvs, rs))
				case !lc && rc:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Float32AddScalar(rvs[0], lvs, rs))
				default:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(add.Float32Add(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Float64AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Float64AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Float64Add(lvs, rvs, rs))
				return vec, nil
			},
		},
	},
	Minus: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Int8SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Int8SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Int8Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Int16SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Int16SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Int16Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Int32SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Int32SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Int32Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Int64SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Int64SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Int64Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Uint8SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Uint8SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Uint8Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Uint16SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Uint16SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Uint16Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Uint32SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Uint32SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Uint32Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Uint64SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Uint64SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Uint64Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				switch {
				case lc && !rc:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Float32SubScalar(lvs[0], rvs, rs))
				case !lc && rc:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Float32SubScalar(rvs[0], lvs, rs))
				default:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(sub.Float32Sub(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Float64SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Float64SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Float64Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
	},
	Mult: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Int8MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Int8MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Int8Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Int16MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Int16MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Int16Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Int32MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Int32MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Int32Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Int64MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Int64MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Int64Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Uint8MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Uint8MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Uint8Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Uint16MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Uint16MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Uint16Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Uint32MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Uint32MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Uint32Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Uint64MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Uint64MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Uint64Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				switch {
				case lc && !rc:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Float32MulScalar(lvs[0], rvs, rs))
				case !lc && rc:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Float32MulScalar(rvs[0], lvs, rs))
				default:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mul.Float32Mul(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Float64MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Float64MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Float64Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
	},
	Div: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int8DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int8DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Int8DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int8Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int8DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int16DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int16DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Int16DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int16Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int16DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int32DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int32DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Int32DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int32Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int32DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int64DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int64DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Int64DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int64Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int64DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint8DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint8DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Uint8DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint8Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint8DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint16DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint16DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Uint16DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint16Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint16DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil

			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint32DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint32DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Uint32DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint32Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint32DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil

			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint64DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint64DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Uint64DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint64Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint64DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Float32DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Float32DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Float32DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Float32Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Float32DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Float64DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Float64DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Float64DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Float64Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Float64DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
	},
	Mod: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int8ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int8ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Int8ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int8Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int8ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int16ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int16ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Int16ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int16Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int16ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int32ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int32ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Int32ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int32Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int32ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int64ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int64ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Int64ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int64Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int64ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint8ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint8ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Uint8ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint8Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint8ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint16ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint16ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Uint16ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint16Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint16ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil

			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint32ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint32ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Uint32ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint32Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint32ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil

			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint64ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint64ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Uint64ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint64Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint64ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
	},
	EQ: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(eq.Int8EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int8EqScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(eq.Int8EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int8EqScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Int8EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Int8EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Int8EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Int8Eq(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(eq.Int16EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int16EqScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(eq.Int16EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int16EqScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Int16EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Int16EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Int16EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Int16Eq(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(eq.Int32EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int32EqScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(eq.Int32EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int32EqScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Int32EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Int32EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Int32EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Int32Eq(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(eq.Int64EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int64EqScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(eq.Int64EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int64EqScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Int64EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Int64EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Int64EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Int64Eq(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(eq.Uint8EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint8EqScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(eq.Uint8EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint8EqScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Uint8EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Uint8EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Uint8EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Uint8Eq(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(eq.Uint16EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint16EqScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(eq.Uint16EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint16EqScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Uint16EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Uint16EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Uint16EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Uint16Eq(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(eq.Uint32EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint32EqScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(eq.Uint32EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint32EqScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Uint32EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Uint32EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Uint32EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Uint32Eq(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(eq.Uint64EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint64EqScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(eq.Uint64EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint64EqScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Uint64EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Uint64EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Uint64EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Uint64Eq(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(eq.Float32EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Float32EqScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(eq.Float32EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Float32EqScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Float32EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Float32EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Float32EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Float32Eq(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(eq.Float64EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Float64EqScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(eq.Float64EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Float64EqScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Float64EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Float64EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Float64EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Float64Eq(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_char,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(eq.StrEqNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.StrEqScalar(lvs.Data, rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(eq.StrEqNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.StrEqScalar(rvs.Data, lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.StrEqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.StrEqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.StrEqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.StrEq(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_varchar,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(eq.StrEqNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.StrEqScalar(lvs.Data, rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(eq.StrEqNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.StrEqScalar(rvs.Data, lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.StrEqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.StrEqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.StrEqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.StrEq(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
	},
	LT: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Int8LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Int8LtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(lt.Int8LtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Int8LtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Int8LtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Int8LtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(lt.Int8LtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(lt.Int8Lt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Int16LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Int16LtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(lt.Int16LtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Int16LtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Int16LtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Int16LtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(lt.Int16LtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(lt.Int16Lt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Int32LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Int32LtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(lt.Int32LtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Int32LtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Int32LtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Int32LtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(lt.Int32LtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(lt.Int32Lt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Int64LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Int64LtScalar(lvs[0], rvs, rs))
					}
					vec.Nsp = rv.Nsp
					vec.SetCol(lt.Int64LtScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(lt.Int64LtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Int64LtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Int64LtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Int64LtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(lt.Int64LtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(lt.Int64Lt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Uint8LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Uint8LtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(lt.Uint8LtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Uint8LtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Uint8LtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Uint8LtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(lt.Uint8LtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(lt.Uint8Lt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Uint16LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Uint16LtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(lt.Uint16LtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Uint16LtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Uint16LtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Uint16LtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(lt.Uint16LtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(lt.Uint16Lt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Uint32LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Uint32LtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(lt.Uint32LtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Uint32LtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Uint32LtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Uint32LtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(lt.Uint32LtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(lt.Uint32Lt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Uint64LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Uint64LtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(lt.Uint64LtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Uint64LtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Uint64LtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Uint64LtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(lt.Uint64LtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(lt.Uint64Lt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Float32LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Float32LtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(lt.Float32LtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Float32LtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Float32LtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Float32LtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(lt.Float32LtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(lt.Float32Lt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Float64LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Float64LtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(lt.Float64LtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Float64LtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Float64LtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.Float64LtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(lt.Float64LtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(lt.Float64Lt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_char,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.StrLtNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.StrLtScalar(lvs.Data, rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(lt.StrLtNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.StrLtScalar(rvs.Data, lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.StrLtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.StrLtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(lt.StrLtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(lt.StrLt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_varchar,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.StrLtNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.StrLtScalar(lvs.Data, rvs, rs))
					}
					vec.Nsp = rv.Nsp
					vec.SetCol(lt.StrLtScalar(lvs.Data, rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(lt.StrLtNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.StrLtScalar(rvs.Data, lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.StrLtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(lt.StrLtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(lt.StrLtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(lt.StrLt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
	},
	LE: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Int8LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Int8LeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(le.Int8LeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Int8LeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int8LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int8LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Int8LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Int8Le(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Int16LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Int16LeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(le.Int16LeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Int16LeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int16LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int16LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Int16LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Int16Le(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Int32LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Int32LeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(le.Int32LeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Int32LeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int32LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int32LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Int32LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Int32Le(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Int64LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Int64LeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(le.Int64LeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Int64LeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int64LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int64LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Int64LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Int64Le(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Uint8LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Uint8LeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(le.Uint8LeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Uint8LeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint8LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint8LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Uint8LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Uint8Le(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Uint16LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Uint16LeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(le.Uint16LeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Uint16LeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint16LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint16LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Uint16LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Uint16Le(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Uint32LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Uint32LeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(ge.Uint32GeScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint32LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint32LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Uint32LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Uint32Le(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Uint64LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Uint64LeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(le.Uint64LeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Uint64LeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint64LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint64LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Uint64LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Uint64Le(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Float32LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Float32LeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(le.Float32LeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Float32LeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Float32LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Float32LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Float32LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Float32Le(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Float64LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Float64LeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(le.Float64LeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Float64LeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Float64LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Float64LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Float64LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Float64Le(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_char,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(le.StrLeNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.StrLeScalar(lvs.Data, rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(le.StrLeNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.StrLeScalar(rvs.Data, lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.StrLeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.StrLeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.StrLeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.StrLe(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_varchar,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(le.StrLeNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.StrLeScalar(lvs.Data, rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(le.StrLeNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.StrLeScalar(rvs.Data, lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.StrLeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.StrLeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.StrLeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.StrLe(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
	},
	GT: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(gt.Int8GtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Int8GtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Int8GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Int8GtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Int8GtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Int8GtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(gt.Int8GtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(gt.Int8Gt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(gt.Int16GtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Int16GtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Int16GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Int16GtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Int16GtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Int16GtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(gt.Int16GtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(gt.Int16Gt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(gt.Int32GtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Int32GtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Int32GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Int32GtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Int32GtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Int32GtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(gt.Int32GtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(gt.Int32Gt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(gt.Int64GtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Int64GtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Int64GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Int64GtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Int64GtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Int64GtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(gt.Int64GtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(gt.Int64Gt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(gt.Uint8GtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Uint8GtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Uint8GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Uint8GtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Uint8GtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Uint8GtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(gt.Uint8GtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(gt.Uint8Gt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(gt.Uint16GtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Uint16GtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Uint16GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Uint16GtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Uint16GtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Uint16GtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(gt.Uint16GtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(gt.Uint16Gt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(gt.Uint32GtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Uint32GtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Uint32GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Uint32GtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Uint32GtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Uint32GtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(gt.Uint32GtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(gt.Uint32Gt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(gt.Uint64GtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Uint64GtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Uint64GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Uint64GtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Uint64GtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Uint64GtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(gt.Uint64GtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(gt.Uint64Gt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(gt.Float32GtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Float32GtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Float32GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Float32GtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Float32GtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Float32GtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(gt.Float32GtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(gt.Float32Gt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(gt.Float64GtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Float64GtScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Float64GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Float64GtScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Float64GtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.Float64GtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(gt.Float64GtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(gt.Float64Gt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_char,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(gt.StrGtNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.StrGtScalar(lvs.Data, rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.StrGtNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.StrGtScalar(rvs.Data, lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.StrGtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.StrGtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(gt.StrGtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(gt.StrGt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_varchar,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(gt.StrGtNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.StrGtScalar(lvs.Data, rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.StrGtNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.StrGtScalar(rvs.Data, lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.StrGtNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(gt.StrGtNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(gt.StrGtNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(gt.StrGt(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
	},
	GE: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ge.Int8GeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Int8GeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Int8GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Int8GeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Int8GeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Int8GeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ge.Int8GeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ge.Int8Ge(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ge.Int16GeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Int16GeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Int16GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Int16GeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Int16GeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Int16GeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ge.Int16GeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ge.Int16Ge(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ge.Int32GeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Int32GeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Int32GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Int32GeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Int32GeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Int32GeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ge.Int32GeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ge.Int32Ge(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ge.Int64GeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Int64GeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Int64GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Int64GeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Int64GeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Int64GeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ge.Int64GeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ge.Int64Ge(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ge.Uint8GeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Uint8GeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Uint8GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Uint8GeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Uint8GeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Uint8GeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ge.Uint8GeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ge.Uint8Ge(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ge.Uint16GeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Uint16GeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Uint16GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Uint16GeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Uint16GeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Uint16GeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ge.Uint16GeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ge.Uint16Ge(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ge.Uint32GeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Uint32GeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Uint32GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Uint32GeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Uint32GeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Uint32GeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ge.Uint32GeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ge.Uint32Ge(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ge.Uint64GeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Uint64GeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Uint64GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Uint64GeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Uint64GeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Uint64GeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ge.Uint64GeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ge.Uint64Ge(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ge.Float32GeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Float32GeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Float32GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Float32GeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Float32GeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Float32GeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ge.Float32GeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ge.Float32Ge(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ge.Float64GeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Float64GeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Float64GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Float64GeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Float64GeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.Float64GeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ge.Float64GeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ge.Float64Ge(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_char,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(ge.StrGeNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.StrGeScalar(lvs.Data, rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.StrGeNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.StrGeScalar(rvs.Data, lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.StrGeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.StrGeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ge.StrGeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ge.StrGe(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_varchar,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(ge.StrGeNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.StrGeScalar(lvs.Data, rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.StrGeNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.StrGeScalar(rvs.Data, lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.StrGeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ge.StrGeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ge.StrGeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ge.StrGe(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
	},
	NE: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ne.Int8NeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Int8NeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ne.Int8NeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Int8NeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Int8NeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Int8NeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ne.Int8NeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ne.Int8Ne(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ne.Int16NeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Int16NeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ne.Int16NeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Int16NeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Int16NeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Int16NeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ne.Int16NeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ne.Int16Ne(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ne.Int32NeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Int32NeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ne.Int32NeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Int32NeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Int32NeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Int32NeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ne.Int32NeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ne.Int32Ne(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ne.Int64NeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Int64NeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ne.Int64NeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Int64NeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Int64NeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Int64NeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ne.Int64NeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ne.Int64Ne(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ne.Uint8NeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Uint8NeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ne.Uint8NeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Uint8NeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Uint8NeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Uint8NeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ne.Uint8NeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ne.Uint8Ne(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ne.Uint16NeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Uint16NeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ne.Uint16NeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Uint16NeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Uint16NeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Uint16NeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ne.Uint16NeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ne.Uint16Ne(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ne.Uint32NeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Uint32NeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ne.Uint32NeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Uint32NeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Uint32NeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Uint32NeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ne.Uint32NeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ne.Uint32Ne(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ne.Uint64NeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Uint64NeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ne.Uint64NeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Uint64NeScalar(rvs[0], lvs, rs))
					}
					vec.Nsp = lv.Nsp
					vec.SetCol(ne.Uint64NeScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Uint64NeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Uint64NeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ne.Uint64NeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ne.Uint64Ne(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ne.Float32NeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Float32NeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ne.Float32NeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Float32NeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Float32NeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Float32NeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ne.Float32NeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ne.Float32Ne(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(ne.Float64NeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Float64NeScalar(lvs[0], rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ne.Float64NeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.Float64NeScalar(rvs[0], lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Float64NeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.Float64NeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ne.Float64NeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ne.Float64Ne(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_char,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(ne.StrNeNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.StrNeScalar(lvs.Data, rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(ne.StrNeNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.StrNeScalar(rvs.Data, lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.StrNeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.StrNeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ne.StrNeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ne.StrNe(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_varchar,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(ne.StrNeNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.StrNeScalar(lvs.Data, rvs, rs))
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(ne.StrNeNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ne.StrNeScalar(rvs.Data, lvs, rs))
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.StrNeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(ne.StrNeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(ne.StrNeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(ne.StrNe(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
	},
	Typecast: {
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int16ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int32ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int64ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint8ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint16ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint32ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint64ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float32ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float64ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToInt8(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToInt8(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int8ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int32ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int64ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint8ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint16ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint32ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint64ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float32ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float64ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToInt16(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToInt16(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int8ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int16ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int64ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint8ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint16ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint32ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint64ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float32ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float64ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToInt32(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToInt32(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int8ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int16ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int32ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint8ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint16ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint32ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint64ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float32ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float64ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToInt64(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToInt64(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int8ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int16ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int32ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int64ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint16ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint32ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint64ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float32ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float64ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToUint8(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToUint8(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int8ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int16ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int32ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int64ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint8ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint32ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint64ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float32ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float64ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToUint16(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToUint16(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int8ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int16ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int32ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int64ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint8ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint16ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint64ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float32ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float64ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToUint32(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToUint32(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int8ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int16ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int32ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int64ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint8ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint16ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint32ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float32ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float64ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToUint64(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToUint64(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int8ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int16ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int32ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int64ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint8ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint16ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint32ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint64ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float64ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToFloat32(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToFloat32(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int8ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int16ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int32ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Int64ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint8ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint16ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint32ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint64ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Float32ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToFloat64(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToFloat64(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
	},
}
