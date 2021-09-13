// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package overload

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/mod"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func init() {
	BinOps[Mod] = []*BinOp{
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
								return nil, ErrModByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							mod.Int8ModScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeInt8Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(mod.Int8ModScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						mod.Int8ModScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(mod.Int8ModScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						mod.Int8ModByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(mod.Int8ModByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						lv.Ref = 0
						mod.Int8Mod(lvs, rvs, lvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					mod.Int8ModSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rv.Ref = 0
						mod.Int8Mod(lvs, rvs, rvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					mod.Int8ModSels(lvs, rvs, rvs, sels)
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
							return nil, ErrModByZero
						}
					}
					vec.SetCol(mod.Int8Mod(lvs, rvs, rs))
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
						return nil, ErrModByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(mod.Int8ModSels(lvs, rvs, rs, sels))
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
								return nil, ErrModByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							mod.Int16ModScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeInt16Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(mod.Int16ModScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						mod.Int16ModScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(mod.Int16ModScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						mod.Int16ModByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(mod.Int16ModByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						lv.Ref = 0
						mod.Int16Mod(lvs, rvs, lvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					mod.Int16ModSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rv.Ref = 0
						mod.Int16Mod(lvs, rvs, rvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					mod.Int16ModSels(lvs, rvs, rvs, sels)
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
							return nil, ErrModByZero
						}
					}
					vec.SetCol(mod.Int16Mod(lvs, rvs, rs))
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
						return nil, ErrModByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(mod.Int16ModSels(lvs, rvs, rs, sels))
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
								return nil, ErrModByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							mod.Int32ModScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeInt32Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(mod.Int32ModScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						mod.Int32ModScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(mod.Int32ModScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						mod.Int32ModByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(mod.Int32ModByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						lv.Ref = 0
						mod.Int32Mod(lvs, rvs, lvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					mod.Int32ModSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rv.Ref = 0
						mod.Int32Mod(lvs, rvs, rvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					mod.Int32ModSels(lvs, rvs, rvs, sels)
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
							return nil, ErrModByZero
						}
					}
					vec.SetCol(mod.Int32Mod(lvs, rvs, rs))
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
						return nil, ErrModByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(mod.Int32ModSels(lvs, rvs, rs, sels))
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
								return nil, ErrModByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							mod.Int64ModScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeInt64Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(mod.Int64ModScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						mod.Int64ModScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(mod.Int64ModScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						mod.Int64ModByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(mod.Int64ModByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						lv.Ref = 0
						mod.Int64Mod(lvs, rvs, lvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					mod.Int64ModSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rv.Ref = 0
						mod.Int64Mod(lvs, rvs, rvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					mod.Int64ModSels(lvs, rvs, rvs, sels)
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
							return nil, ErrModByZero
						}
					}
					vec.SetCol(mod.Int64Mod(lvs, rvs, rs))
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
						return nil, ErrModByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(mod.Int64ModSels(lvs, rvs, rs, sels))
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
								return nil, ErrModByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							mod.Uint8ModScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeUint8Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(mod.Uint8ModScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						mod.Uint8ModScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(mod.Uint8ModScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						mod.Uint8ModByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(mod.Uint8ModByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						lv.Ref = 0
						mod.Uint8Mod(lvs, rvs, lvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					mod.Uint8ModSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rv.Ref = 0
						mod.Uint8Mod(lvs, rvs, rvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					mod.Uint8ModSels(lvs, rvs, rvs, sels)
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
							return nil, ErrModByZero
						}
					}
					vec.SetCol(mod.Uint8Mod(lvs, rvs, rs))
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
						return nil, ErrModByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(mod.Uint8ModSels(lvs, rvs, rs, sels))
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
								return nil, ErrModByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							mod.Uint16ModScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeUint16Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(mod.Uint16ModScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						mod.Uint16ModScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(mod.Uint16ModScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						mod.Uint16ModByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(mod.Uint16ModByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						lv.Ref = 0
						mod.Uint16Mod(lvs, rvs, lvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					mod.Uint16ModSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rv.Ref = 0
						mod.Uint16Mod(lvs, rvs, rvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					mod.Uint16ModSels(lvs, rvs, rvs, sels)
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
							return nil, ErrModByZero
						}
					}
					vec.SetCol(mod.Uint16Mod(lvs, rvs, rs))
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
						return nil, ErrModByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(mod.Uint16ModSels(lvs, rvs, rs, sels))
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
								return nil, ErrModByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							mod.Uint32ModScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeUint32Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(mod.Uint32ModScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						mod.Uint32ModScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(mod.Uint32ModScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						mod.Uint32ModByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(mod.Uint32ModByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						lv.Ref = 0
						mod.Uint32Mod(lvs, rvs, lvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					mod.Uint32ModSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rv.Ref = 0
						mod.Uint32Mod(lvs, rvs, rvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					mod.Uint32ModSels(lvs, rvs, rvs, sels)
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
							return nil, ErrModByZero
						}
					}
					vec.SetCol(mod.Uint32Mod(lvs, rvs, rs))
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
						return nil, ErrModByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(mod.Uint32ModSels(lvs, rvs, rs, sels))
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
								return nil, ErrModByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							mod.Uint64ModScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeUint64Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(mod.Uint64ModScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						mod.Uint64ModScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(mod.Uint64ModScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						mod.Uint64ModByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(mod.Uint64ModByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						lv.Ref = 0
						mod.Uint64Mod(lvs, rvs, lvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					mod.Uint64ModSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rv.Ref = 0
						mod.Uint64Mod(lvs, rvs, rvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					mod.Uint64ModSels(lvs, rvs, rvs, sels)
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
							return nil, ErrModByZero
						}
					}
					vec.SetCol(mod.Uint64Mod(lvs, rvs, rs))
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
						return nil, ErrModByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(mod.Uint64ModSels(lvs, rvs, rs, sels))
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
								return nil, ErrModByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							mod.Float32ModScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeFloat32Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(mod.Float32ModScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						mod.Float32ModScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat32Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(mod.Float32ModScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						mod.Float32ModByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat32Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(mod.Float32ModByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						lv.Ref = 0
						mod.Float32Mod(lvs, rvs, lvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					mod.Float32ModSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rv.Ref = 0
						mod.Float32Mod(lvs, rvs, rvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					mod.Float32ModSels(lvs, rvs, rvs, sels)
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
							return nil, ErrModByZero
						}
					}
					vec.SetCol(mod.Float32Mod(lvs, rvs, rs))
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
						return nil, ErrModByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(mod.Float32ModSels(lvs, rvs, rs, sels))
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
								return nil, ErrModByZero
							}
						}
						if rv.Ref == 1 || rv.Ref == 0 {
							rv.Ref = 0
							mod.Float64ModScalar(lvs[0], rvs, rvs)
							return rv, nil
						}
						vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
						if err != nil {
							return nil, err
						}
						rs := encoding.DecodeFloat64Slice(vec.Data)
						rs = rs[:len(rvs)]
						vec.Nsp.Set(rv.Nsp)
						vec.SetCol(mod.Float64ModScalar(lvs[0], rvs, rs))
						return vec, nil
					}
					sels := register.GetSels(proc)
					defer register.PutSels(sels, proc)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						mod.Float64ModScalarSels(lvs[0], rvs, rvs, sels)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(mod.Float64ModScalarSels(lvs[0], rvs, rs, sels))
					return vec, nil
				case !lc && rc:
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						mod.Float64ModByScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(mod.Float64ModByScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						lv.Ref = 0
						mod.Float64Mod(lvs, rvs, lvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					lv.Ref = 0
					mod.Float64ModSels(lvs, rvs, lvs, sels)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rv.Ref = 0
						mod.Float64Mod(lvs, rvs, rvs)
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
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rv.Ref = 0
					mod.Float64ModSels(lvs, rvs, rvs, sels)
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
							return nil, ErrModByZero
						}
					}
					vec.SetCol(mod.Float64Mod(lvs, rvs, rs))
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
						return nil, ErrModByZero
					}
					sels = append(sels, int64(i))
				}
				vec.SetCol(mod.Float64ModSels(lvs, rvs, rs, sels))
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
