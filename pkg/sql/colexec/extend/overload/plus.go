package overload

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/add"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func init() {
	BinOps[Plus] = []*BinOp{
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						add.Int8AddScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(add.Int8AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						add.Int8AddScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(add.Int8AddScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					add.Int8Add(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					add.Int8Add(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(add.Int8Add(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						add.Int16AddScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(add.Int16AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						add.Int16AddScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(add.Int16AddScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					add.Int16Add(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					add.Int16Add(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(add.Int16Add(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						add.Int32AddScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(add.Int32AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						add.Int32AddScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(add.Int32AddScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					add.Int32Add(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					add.Int32Add(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(add.Int32Add(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						add.Int64AddScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(add.Int64AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						add.Int64AddScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(add.Int64AddScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					add.Int64Add(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					add.Int64Add(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(add.Int64Add(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						add.Uint8AddScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(add.Uint8AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						add.Uint8AddScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(add.Uint8AddScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					add.Uint8Add(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					add.Uint8Add(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(add.Uint8Add(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						add.Uint16AddScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(add.Uint16AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						add.Uint16AddScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(add.Uint16AddScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					add.Uint16Add(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					add.Uint16Add(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(add.Uint16Add(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						add.Uint32AddScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(add.Uint32AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						add.Uint32AddScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(add.Uint32AddScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					add.Uint32Add(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					add.Uint32Add(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(add.Uint32Add(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						add.Uint64AddScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(add.Uint64AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						add.Uint64AddScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(add.Uint64AddScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					add.Uint64Add(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					add.Uint64Add(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(add.Uint64Add(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						add.Float32AddScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat32Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(add.Float32AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						add.Float32AddScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat32Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(add.Float32AddScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					add.Float32Add(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					add.Float32Add(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(add.Float32Add(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						add.Float64AddScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(add.Float64AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						add.Float64AddScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(add.Float64AddScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					add.Float64Add(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					add.Float64Add(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(add.Float64Add(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
				}
				return vec, nil
			},
		},
	}
}
