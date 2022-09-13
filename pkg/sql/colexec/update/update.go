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

package update

import (
	"bytes"
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("update rows")
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

func Call(_ int, proc *process.Process, arg any) (bool, error) {
	p := arg.(*Argument)
	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Zs) == 0 {
		return false, nil
	}

	defer bat.Clean(proc.Mp())
	var affectedRows uint64 = 0
	batLen := batch.Length(bat)
	// Fill vector for constant value
	for i := range bat.Vecs {
		bat.Vecs[i] = bat.Vecs[i].ConstExpand(proc.Mp())
	}

	ctx := context.TODO()
	for i, updateCtx := range p.UpdateCtxs {

		tmpBat := &batch.Batch{}

		if updateCtx.PriKeyIdx != -1 {
			idx := updateCtx.PriKeyIdx

			tmpBat.Vecs = bat.Vecs[int(idx)+1 : int(idx)+len(updateCtx.OrderAttrs)+1]
			tmpBat.Attrs = append(tmpBat.Attrs, updateCtx.UpdateAttrs...)
			tmpBat.Attrs = append(tmpBat.Attrs, updateCtx.OtherAttrs...)
			tmpBat.Zs = bat.Zs

			for i := range tmpBat.Vecs {
				if tmpBat.Vecs[i].IsScalarNull() {
					// vector need to be filled to insert
					vector.PreAlloc(tmpBat.Vecs[i], 0, batLen, proc.Mp())
				}
			}

			err := updateCtx.TableSource.Delete(ctx, bat.GetVector(idx), updateCtx.PriKey)
			if err != nil {
				return false, err
			}

			batch.Reorder(tmpBat, updateCtx.OrderAttrs)
			if err := colexec.UpdateInsertBatch(p.Engine, p.DB[i], ctx, proc, p.TableDefVec[i].Cols, tmpBat, p.TableID[i]); err != nil {
				return false, err
			}
			err = updateCtx.TableSource.Write(ctx, tmpBat)
			if err != nil {
				return false, err
			}

			affectedRows += uint64(batch.Length(bat))
		} else {
			idx := updateCtx.HideKeyIdx
			tmpBat.Vecs = bat.Vecs[int(idx) : int(idx)+len(updateCtx.OrderAttrs)+1]

			// need to de duplicate
			var cnt uint64
			tmpBat, cnt = FilterBatch(tmpBat, batLen, proc)
			if tmpBat == nil {
				panic(any("internal error when filter Batch"))
			}

			err := updateCtx.TableSource.Delete(ctx, tmpBat.GetVector(0), updateCtx.HideKey)
			if err != nil {
				tmpBat.Clean(proc.Mp())
				return false, err
			}

			tmpBat.Vecs[0].Free(proc.Mp())
			tmpBat.Vecs = tmpBat.Vecs[1:]

			tmpBat.Attrs = append(tmpBat.Attrs, updateCtx.UpdateAttrs...)
			tmpBat.Attrs = append(tmpBat.Attrs, updateCtx.OtherAttrs...)

			batch.Reorder(tmpBat, updateCtx.OrderAttrs)
			if err := colexec.UpdateInsertBatch(p.Engine, p.DB[i], ctx, proc, p.TableDefVec[i].Cols, tmpBat, p.TableID[i]); err != nil {
				return false, err
			}
			err = updateCtx.TableSource.Write(ctx, tmpBat)
			if err != nil {
				tmpBat.Clean(proc.Mp())
				return false, err
			}
			tmpBat.Clean(proc.Mp())

			affectedRows += cnt
		}
	}

	atomic.AddUint64(&p.AffectedRows, affectedRows)
	return false, nil
}

func FilterBatch(bat *batch.Batch, batLen int, proc *process.Process) (*batch.Batch, uint64) {
	var cnt uint64 = 0
	newBat := &batch.Batch{}
	m := make(map[[16]byte]int, batLen)

	for _, vec := range bat.Vecs {
		v := vector.New(vec.Typ)
		vector.PreAlloc(v, 0, batLen, proc.Mp())
		newBat.Vecs = append(newBat.Vecs, v)
	}

	rows := bat.Vecs[0].Col.([]types.Rowid)
	for idx, row := range rows {
		if _, ok := m[row]; ok {
			continue
		}
		m[row] = 1
		cnt++

		for j, vec := range bat.Vecs {
			var val any
			if nulls.Contains(vec.Nsp, uint64(idx)) {
				nulls.Add(newBat.Vecs[j].Nsp, uint64(cnt)-1)
				val = getIndexValue(idx, vec, true)
			} else {
				val = getIndexValue(idx, vec, false)
			}

			err := newBat.Vecs[j].Append(val, false, proc.Mp())
			if err != nil {
				return nil, 0
			}
		}
	}
	newBat.Zs = make([]int64, batLen)
	return newBat, cnt
}

// XXX isn't this type switch super slow?
func getIndexValue(idx int, v *vector.Vector, isNull bool) any {
	switch v.Typ.Oid {
	case types.T_bool:
		if isNull {
			return false
		}
		col := v.Col.([]bool)
		return col[idx]
	case types.T_int8:
		if isNull {
			return int8(0)
		}
		col := v.Col.([]int8)
		return col[idx]
	case types.T_int16:
		if isNull {
			return int16(0)
		}
		col := v.Col.([]int16)
		return col[idx]
	case types.T_int32:
		if isNull {
			return int32(0)
		}
		col := v.Col.([]int32)
		return col[idx]
	case types.T_int64:
		if isNull {
			return int64(0)
		}
		col := v.Col.([]int64)
		return col[idx]
	case types.T_uint8:
		if isNull {
			return uint8(0)
		}
		col := v.Col.([]uint8)
		return col[idx]
	case types.T_uint16:
		if isNull {
			return uint16(0)
		}
		col := v.Col.([]uint16)
		return col[idx]
	case types.T_uint32:
		if isNull {
			return uint32(0)
		}
		col := v.Col.([]uint32)
		return col[idx]
	case types.T_uint64:
		if isNull {
			return uint64(0)
		}
		col := v.Col.([]uint64)
		return col[idx]
	case types.T_float32:
		if isNull {
			return float32(0)
		}
		col := v.Col.([]float32)
		return col[idx]
	case types.T_float64:
		if isNull {
			return float64(0)
		}
		col := v.Col.([]float64)
		return col[idx]
	case types.T_date:
		if isNull {
			return types.Date(0)
		}
		col := v.Col.([]types.Date)
		return col[idx]
	case types.T_datetime:
		if isNull {
			return types.Datetime(0)
		}
		col := v.Col.([]types.Datetime)
		return col[idx]
	case types.T_timestamp:
		if isNull {
			return types.Timestamp(0)
		}
		col := v.Col.([]types.Timestamp)
		return col[idx]
	case types.T_decimal64:
		if isNull {
			return types.Decimal64([8]byte{})
		}
		col := v.Col.([]types.Decimal64)
		return col[idx]
	case types.T_decimal128:
		if isNull {
			return types.Decimal128([16]byte{})
		}
		col := v.Col.([]types.Decimal128)
		return col[idx]
	case types.T_TS:
		if isNull {
			var ts types.TS
			return ts
		}
		col := v.Col.([]types.TS)
		return col[idx]
	case types.T_Rowid:
		if isNull {
			var z types.Rowid
			return z
		}
		col := v.Col.([]types.Rowid)
		return col[idx]
	case types.T_char, types.T_varchar, types.T_blob, types.T_json:
		if isNull {
			// XXX: Why don't we return nil?
			return []byte{}
		}
		return v.GetBytes(int64(idx))
	default:
		return nil
	}
}
