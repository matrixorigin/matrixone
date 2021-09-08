package overload

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/gt"
	"matrixone/pkg/vectorize/lt"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

func init() {
	BinOps[LT] = []*BinOp{
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Int8LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Int8LtScalar(lvs[0], rvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Int8GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Int8GtScalar(rvs[0], lvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
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
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Int16LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Int16LtScalar(lvs[0], rvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Int16GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Int16GtScalar(rvs[0], lvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
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
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Int32LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Int32LtScalar(lvs[0], rvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Int32GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Int32GtScalar(rvs[0], lvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
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
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Int64LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Int64LtScalar(lvs[0], rvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Int64GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Int64GtScalar(rvs[0], lvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
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
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Uint8LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Uint8LtScalar(lvs[0], rvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Uint8GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Uint8GtScalar(rvs[0], lvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
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
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Uint16LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Uint16LtScalar(lvs[0], rvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Uint16GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Uint16GtScalar(rvs[0], lvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
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
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Uint32LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Uint32LtScalar(lvs[0], rvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Uint32GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Uint32GtScalar(rvs[0], lvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
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
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Uint64LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Uint64LtScalar(lvs[0], rvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Uint64GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Uint64GtScalar(rvs[0], lvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
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
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Float32LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Float32LtScalar(lvs[0], rvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Float32GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Float32GtScalar(rvs[0], lvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
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
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.Float64LtNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.Float64LtScalar(lvs[0], rvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.Float64GtNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.Float64GtScalar(rvs[0], lvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, SelsType)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
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
			LeftType:   types.T_char,
			RightType:  types.T_char,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.StrLtNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.StrLtScalar(lvs.Data, rvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.StrGtNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.StrGtScalar(rvs.Data, lvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, SelsType)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
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
			LeftType:   types.T_varchar,
			RightType:  types.T_varchar,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(lt.StrLtNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(lt.StrLtScalar(lvs.Data, rvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, SelsType)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(gt.StrGtNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(gt.StrGtScalar(rvs.Data, lvs, rs))
					}
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, SelsType)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
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
