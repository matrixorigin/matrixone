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

package vector

import (
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container"
	// log "github.com/sirupsen/logrus"
)

func (v *BaseVector) HasNull() bool {
	return atomic.LoadUint64(&v.StatMask)&container.HasNullMask != 0
}

func (v *BaseVector) NullCnt() int {
	if !v.HasNull() {
		return 0
	}

	if !v.IsReadonly() {
		v.RLock()
		defer v.RUnlock()
	}

	return nulls.Length(v.VMask)
}

func (v *BaseVector) ResetReadonly() {
	mask := atomic.LoadUint64(&v.StatMask)
	mask &= ^container.ReadonlyMask
	atomic.StoreUint64(&v.StatMask, mask)
}

func (v *BaseVector) GetDataType() types.Type { return v.Type }
func (v *BaseVector) IsReadonly() bool {
	return atomic.LoadUint64(&v.StatMask)&container.ReadonlyMask != 0
}

func (v *BaseVector) Length() int {
	return int(atomic.LoadUint64(&v.StatMask) & container.PosMask)
}

func (v *BaseVector) IsNull(idx int) (bool, error) {
	if idx >= v.Length() {
		return false, ErrVecInvalidOffset
	}
	if !v.IsReadonly() {
		v.RLock()
		defer v.RUnlock()
	}
	return nulls.Contains(v.VMask, uint64(idx)), nil
}

func MockVector(t types.Type, rows uint64) IVector {
	var vec IVector
	switch t.Oid {
	case types.T_int8:
		vec = NewStdVector(t, rows)
		var vals []int8
		for i := uint64(0); i < rows; i++ {
			vals = append(vals, int8(i%5000))
		}
		vec.Append(len(vals), vals)
	case types.T_int16:
		vec = NewStdVector(t, rows)
		var vals []int16
		for i := uint64(0); i < rows; i++ {
			vals = append(vals, int16(i%5000))
		}
		vec.Append(len(vals), vals)
	case types.T_int32:
		vec = NewStdVector(t, rows)
		var vals []int32
		for i := uint64(0); i < rows; i++ {
			vals = append(vals, int32(i%5000))
		}
		vec.Append(len(vals), vals)
	case types.T_int64:
		vec = NewStdVector(t, rows)
		var vals []int64
		for i := uint64(0); i < rows; i++ {
			vals = append(vals, int64(i%5000))
		}
		vec.Append(len(vals), vals)
	case types.T_uint8:
		vec = NewStdVector(t, rows)
		var vals []uint8
		for i := uint64(0); i < rows; i++ {
			vals = append(vals, uint8(i%5000))
		}
		vec.Append(len(vals), vals)
	case types.T_uint16:
		vec = NewStdVector(t, rows)
		var vals []uint16
		for i := uint64(0); i < rows; i++ {
			vals = append(vals, uint16(i%5000))
		}
		vec.Append(len(vals), vals)
	case types.T_uint32:
		vec = NewStdVector(t, rows)
		var vals []uint32
		for i := uint64(0); i < rows; i++ {
			vals = append(vals, uint32(i%5000))
		}
		vec.Append(len(vals), vals)
	case types.T_uint64:
		vec = NewStdVector(t, rows)
		var vals []uint64
		for i := uint64(0); i < rows; i++ {
			vals = append(vals, uint64(i%5000))
		}
		vec.Append(len(vals), vals)
	case types.T_float32:
		vec = NewStdVector(t, rows)
		var vals []float32
		for i := uint64(0); i < rows; i++ {
			vals = append(vals, float32(i%5000))
		}
		vec.Append(len(vals), vals)
	case types.T_float64:
		vec = NewStdVector(t, rows)
		var vals []float64
		for i := uint64(0); i < rows; i++ {
			vals = append(vals, float64(i%5000))
		}
		vec.Append(len(vals), vals)
	case types.T_varchar, types.T_char:
		vec = NewStrVector(t, rows)
		vals := make([][]byte, 0, rows)
		prefix := "str"
		for i := uint64(0); i < rows; i++ {
			s := fmt.Sprintf("%s%d", prefix, i)
			vals = append(vals, []byte(s))
		}
		vec.Append(len(vals), vals)
	case types.T_datetime:
		vec = NewStdVector(t, rows)
		vals := make([]types.Datetime, 0, rows)
		for i := uint64(1); i <= rows; i++ {
			vals = append(vals, types.FromClock(int32(i*100), 1, 1, 1, 1, 1, 1))
		}
		vec.Append(len(vals), vals)
	case types.T_date:
		vec = NewStdVector(t, rows)
		vals := make([]types.Date, 0, rows)
		for i := int32(1); i <= int32(rows); i++ {
			vals = append(vals, types.FromCalendar(i*100, 1, 1))
		}
		vec.Append(len(vals), vals)
	default:
		panic("not supported")
	}
	return vec
}
