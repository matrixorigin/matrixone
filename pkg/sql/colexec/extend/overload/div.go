package overload

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/div"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func init() {
	BinOps[Div] = []*BinOp{
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							div.Int8DivScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeInt8Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(div.Int8DivScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						div.Int8DivScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(div.Int8DivScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						div.Int8DivByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(div.Int8DivByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						lv.Ref = 0
						div.Int8Div(lvs, rvs, lvs)
						lv.Nsp = lv.Nsp.Or(rv.Nsp)
						if rv.Ref == 0 {
							register.Put(proc, rv)
						}
						return lv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					div.Int8DivSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rv.Ref = 0
						div.Int8Div(lvs, rvs, rvs)
						rv.Nsp = rv.Nsp.Or(lv.Nsp)
						if lv.Ref == 0 {
							register.Put(proc, lv)
						}
						return rv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					div.Int8DivSels(lvs, rvs, rvs, sels)
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
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					vec.SetCol(div.Int8Div(lvs, rvs, rs))
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				sels := register.GetSels(proc)
				defer register.PutSels(sels, proc)
				for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
					if rv.Nsp.Contains(i) {
						continue
					}
					if rvs[i] == 0 {
						return nil, ErrDivByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(div.Int8DivSels(lvs, rvs, rs, sels))
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
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							div.Int16DivScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeInt16Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(div.Int16DivScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						div.Int16DivScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(div.Int16DivScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						div.Int16DivByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(div.Int16DivByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						lv.Ref = 0
						div.Int16Div(lvs, rvs, lvs)
						lv.Nsp = lv.Nsp.Or(rv.Nsp)
						if rv.Ref == 0 {
							register.Put(proc, rv)
						}
						return lv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					div.Int16DivSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rv.Ref = 0
						div.Int16Div(lvs, rvs, rvs)
						rv.Nsp = rv.Nsp.Or(lv.Nsp)
						if lv.Ref == 0 {
							register.Put(proc, lv)
						}
						return rv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					div.Int16DivSels(lvs, rvs, rvs, sels)
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
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					vec.SetCol(div.Int16Div(lvs, rvs, rs))
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				sels := register.GetSels(proc)
				defer register.PutSels(sels, proc)
				for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
					if rv.Nsp.Contains(i) {
						continue
					}
					if rvs[i] == 0 {
						return nil, ErrDivByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(div.Int16DivSels(lvs, rvs, rs, sels))
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
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							div.Int32DivScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeInt32Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(div.Int32DivScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						div.Int32DivScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(div.Int32DivScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						div.Int32DivByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(div.Int32DivByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						lv.Ref = 0
						div.Int32Div(lvs, rvs, lvs)
						lv.Nsp = lv.Nsp.Or(rv.Nsp)
						if rv.Ref == 0 {
							register.Put(proc, rv)
						}
						return lv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					div.Int32DivSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rv.Ref = 0
						div.Int32Div(lvs, rvs, rvs)
						rv.Nsp = rv.Nsp.Or(lv.Nsp)
						if lv.Ref == 0 {
							register.Put(proc, lv)
						}
						return rv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					div.Int32DivSels(lvs, rvs, rvs, sels)
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
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					vec.SetCol(div.Int32Div(lvs, rvs, rs))
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				sels := register.GetSels(proc)
				defer register.PutSels(sels, proc)
				for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
					if rv.Nsp.Contains(i) {
						continue
					}
					if rvs[i] == 0 {
						return nil, ErrDivByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(div.Int32DivSels(lvs, rvs, rs, sels))
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
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							div.Int64DivScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeInt64Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(div.Int64DivScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						div.Int64DivScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(div.Int64DivScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						div.Int64DivByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(div.Int64DivByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						lv.Ref = 0
						div.Int64Div(lvs, rvs, lvs)
						lv.Nsp = lv.Nsp.Or(rv.Nsp)
						if rv.Ref == 0 {
							register.Put(proc, rv)
						}
						return lv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					div.Int64DivSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rv.Ref = 0
						div.Int64Div(lvs, rvs, rvs)
						rv.Nsp = rv.Nsp.Or(lv.Nsp)
						if lv.Ref == 0 {
							register.Put(proc, lv)
						}
						return rv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					div.Int64DivSels(lvs, rvs, rvs, sels)
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
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					vec.SetCol(div.Int64Div(lvs, rvs, rs))
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				sels := register.GetSels(proc)
				defer register.PutSels(sels, proc)
				for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
					if rv.Nsp.Contains(i) {
						continue
					}
					if rvs[i] == 0 {
						return nil, ErrDivByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(div.Int64DivSels(lvs, rvs, rs, sels))
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
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							div.Uint8DivScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeUint8Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(div.Uint8DivScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						div.Uint8DivScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(div.Uint8DivScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						div.Uint8DivByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(div.Uint8DivByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						lv.Ref = 0
						div.Uint8Div(lvs, rvs, lvs)
						lv.Nsp = lv.Nsp.Or(rv.Nsp)
						if rv.Ref == 0 {
							register.Put(proc, rv)
						}
						return lv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					div.Uint8DivSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rv.Ref = 0
						div.Uint8Div(lvs, rvs, rvs)
						rv.Nsp = rv.Nsp.Or(lv.Nsp)
						if lv.Ref == 0 {
							register.Put(proc, lv)
						}
						return rv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					div.Uint8DivSels(lvs, rvs, rvs, sels)
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
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					vec.SetCol(div.Uint8Div(lvs, rvs, rs))
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				sels := register.GetSels(proc)
				defer register.PutSels(sels, proc)
				for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
					if rv.Nsp.Contains(i) {
						continue
					}
					if rvs[i] == 0 {
						return nil, ErrDivByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(div.Uint8DivSels(lvs, rvs, rs, sels))
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
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							div.Uint16DivScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeUint16Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(div.Uint16DivScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						div.Uint16DivScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(div.Uint16DivScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						div.Uint16DivByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(div.Uint16DivByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						lv.Ref = 0
						div.Uint16Div(lvs, rvs, lvs)
						lv.Nsp = lv.Nsp.Or(rv.Nsp)
						if rv.Ref == 0 {
							register.Put(proc, rv)
						}
						return lv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					div.Uint16DivSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rv.Ref = 0
						div.Uint16Div(lvs, rvs, rvs)
						rv.Nsp = rv.Nsp.Or(lv.Nsp)
						if lv.Ref == 0 {
							register.Put(proc, lv)
						}
						return rv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					div.Uint16DivSels(lvs, rvs, rvs, sels)
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
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					vec.SetCol(div.Uint16Div(lvs, rvs, rs))
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				sels := register.GetSels(proc)
				defer register.PutSels(sels, proc)
				for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
					if rv.Nsp.Contains(i) {
						continue
					}
					if rvs[i] == 0 {
						return nil, ErrDivByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(div.Uint16DivSels(lvs, rvs, rs, sels))
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
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							div.Uint32DivScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeUint32Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(div.Uint32DivScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						div.Uint32DivScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(div.Uint32DivScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						div.Uint32DivByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(div.Uint32DivByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						lv.Ref = 0
						div.Uint32Div(lvs, rvs, lvs)
						lv.Nsp = lv.Nsp.Or(rv.Nsp)
						if rv.Ref == 0 {
							register.Put(proc, rv)
						}
						return lv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					div.Uint32DivSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rv.Ref = 0
						div.Uint32Div(lvs, rvs, rvs)
						rv.Nsp = rv.Nsp.Or(lv.Nsp)
						if lv.Ref == 0 {
							register.Put(proc, lv)
						}
						return rv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					div.Uint32DivSels(lvs, rvs, rvs, sels)
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
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					vec.SetCol(div.Uint32Div(lvs, rvs, rs))
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				sels := register.GetSels(proc)
				defer register.PutSels(sels, proc)
				for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
					if rv.Nsp.Contains(i) {
						continue
					}
					if rvs[i] == 0 {
						return nil, ErrDivByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(div.Uint32DivSels(lvs, rvs, rs, sels))
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
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							div.Uint64DivScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeUint64Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(div.Uint64DivScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						div.Uint64DivScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(div.Uint64DivScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						div.Uint64DivByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(div.Uint64DivByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						lv.Ref = 0
						div.Uint64Div(lvs, rvs, lvs)
						lv.Nsp = lv.Nsp.Or(rv.Nsp)
						if rv.Ref == 0 {
							register.Put(proc, rv)
						}
						return lv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					div.Uint64DivSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rv.Ref = 0
						div.Uint64Div(lvs, rvs, rvs)
						rv.Nsp = rv.Nsp.Or(lv.Nsp)
						if lv.Ref == 0 {
							register.Put(proc, lv)
						}
						return rv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					div.Uint64DivSels(lvs, rvs, rvs, sels)
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
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					vec.SetCol(div.Uint64Div(lvs, rvs, rs))
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				sels := register.GetSels(proc)
				defer register.PutSels(sels, proc)
				for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
					if rv.Nsp.Contains(i) {
						continue
					}
					if rvs[i] == 0 {
						return nil, ErrDivByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(div.Uint64DivSels(lvs, rvs, rs, sels))
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
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							div.Float32DivScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeFloat32Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(div.Float32DivScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						div.Float32DivScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat32Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(div.Float32DivScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						div.Float32DivByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat32Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(div.Float32DivByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						lv.Ref = 0
						div.Float32Div(lvs, rvs, lvs)
						lv.Nsp = lv.Nsp.Or(rv.Nsp)
						if rv.Ref == 0 {
							register.Put(proc, rv)
						}
						return lv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					div.Float32DivSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rv.Ref = 0
						div.Float32Div(lvs, rvs, rvs)
						rv.Nsp = rv.Nsp.Or(lv.Nsp)
						if lv.Ref == 0 {
							register.Put(proc, lv)
						}
						return rv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					div.Float32DivSels(lvs, rvs, rvs, sels)
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
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					vec.SetCol(div.Float32Div(lvs, rvs, rs))
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				sels := register.GetSels(proc)
				defer register.PutSels(sels, proc)
				for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
					if rv.Nsp.Contains(i) {
						continue
					}
					if rvs[i] == 0 {
						return nil, ErrDivByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(div.Float32DivSels(lvs, rvs, rs, sels))
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
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							div.Float64DivScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeFloat64Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(div.Float64DivScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						div.Float64DivScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(div.Float64DivScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						div.Float64DivByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(div.Float64DivByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						lv.Ref = 0
						div.Float64Div(lvs, rvs, lvs)
						lv.Nsp = lv.Nsp.Or(rv.Nsp)
						if rv.Ref == 0 {
							register.Put(proc, rv)
						}
						return lv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					div.Float64DivSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rv.Ref = 0
						div.Float64Div(lvs, rvs, rvs)
						rv.Nsp = rv.Nsp.Or(lv.Nsp)
						if lv.Ref == 0 {
							register.Put(proc, lv)
						}
						return rv, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					div.Float64DivSels(lvs, rvs, rvs, sels)
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
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					vec.SetCol(div.Float64Div(lvs, rvs, rs))
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return vec, nil
				}
				sels := register.GetSels(proc)
				defer register.PutSels(sels, proc)
				for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
					if rv.Nsp.Contains(i) {
						continue
					}
					if rvs[i] == 0 {
						return nil, ErrDivByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(div.Float64DivSels(lvs, rvs, rs, sels))
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
