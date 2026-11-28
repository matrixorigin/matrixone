// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type MockDataProvider struct {
	providers map[int]Vector
}

func NewMockDataProvider() *MockDataProvider {
	return &MockDataProvider{
		providers: make(map[int]Vector),
	}
}

func (p *MockDataProvider) Reset() {
	p.providers = make(map[int]Vector)
}

func (p *MockDataProvider) AddColumnProvider(colIdx int, provider Vector) {
	p.providers[colIdx] = provider
}

func (p *MockDataProvider) GetColumnProvider(colIdx int) Vector {
	if p == nil {
		return nil
	}
	return p.providers[colIdx]
}

func appendMockFloats[T ~float32 | ~float64](
	vec Vector,
	rows int,
	unique bool,
	next func() T,
) {
	const maxConsecutiveCollisions = 1024

	var seen map[T]struct{}
	if unique && rows > 0 {
		seen = make(map[T]struct{}, rows)
	}
	consecutiveCollisions := 0
	for appended := 0; appended < rows; {
		value := next()
		if unique {
			if _, ok := seen[value]; ok {
				consecutiveCollisions++
				if consecutiveCollisions == maxConsecutiveCollisions {
					panic("failed to generate a unique mock float")
				}
				continue
			}
			seen[value] = struct{}{}
			consecutiveCollisions = 0
		}
		vec.MustAppendValue(value, false)
		appended++
	}
}

func MockVector(t types.Type, rows int, unique bool, provider Vector) (vec Vector) {
	vec = MakeVector(t, common.DefaultAllocator)
	if provider != nil {
		if err := extendVector(vec, provider); err != nil {
			panic(err)
		}
		return
	}

	switch t.Oid {
	case types.T_bool:
		for i := 0; i < rows; i++ {
			if i%2 == 0 {
				vec.MustAppendValue(true, false)
			} else {
				vec.MustAppendValue(false, false)
			}
		}
	case types.T_bit:
		if unique {
			for i := 0; i < rows; i++ {
				vec.MustAppendValue(uint64(i), false)
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Int63()
				vec.MustAppendValue(uint64(ival), false)
			}
		}
	case types.T_int8:
		if unique {
			if int(rows) >= math.MaxInt8-math.MinInt8 {
				panic("overflow")
			}
			for i := 0; i < rows; i++ {
				vec.MustAppendValue(math.MinInt8+int8(i), false)
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Intn(math.MaxInt8)
				vec.MustAppendValue(int8(ival), false)
			}
		}
	case types.T_int16:
		if unique {
			for i := 0; i < rows; i++ {
				vec.MustAppendValue(int16(i), false)
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Intn(math.MaxInt16)
				vec.MustAppendValue(int16(ival), false)
			}
		}
	case types.T_int32:
		if unique {
			for i := 0; i < rows; i++ {
				vec.MustAppendValue(int32(i), false)
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Intn(math.MaxInt32)
				vec.MustAppendValue(int32(ival), false)
			}
		}
	case types.T_int64:
		if unique {
			for i := 0; i < rows; i++ {
				vec.MustAppendValue(int64(i), false)
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Intn(math.MaxInt64)
				vec.MustAppendValue(int64(ival), false)
			}
		}
	case types.T_uint8:
		if unique {
			if int(rows) >= math.MaxUint8 {
				panic("overflow")
			}
			for i := 0; i < rows; i++ {
				vec.MustAppendValue(uint8(i), false)
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Intn(math.MaxUint8)
				vec.MustAppendValue(uint8(ival), false)
			}
		}
	case types.T_uint16:
		if unique {
			for i := 0; i < rows; i++ {
				vec.MustAppendValue(uint16(i), false)
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Intn(int(math.MaxInt16))
				vec.MustAppendValue(uint16(ival), false)
			}
		}
	case types.T_uint32:
		if unique {
			for i := 0; i < rows; i++ {
				vec.MustAppendValue(uint32(i), false)
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Int31()
				vec.MustAppendValue(uint32(ival), false)
			}
		}
	case types.T_uint64:
		if unique {
			for i := 0; i < rows; i++ {
				vec.MustAppendValue(uint64(i), false)
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Int63()
				vec.MustAppendValue(uint64(ival), false)
			}
		}
	case types.T_float32:
		appendMockFloats(vec, rows, unique, func() float32 {
			v1 := rand.Intn(math.MaxInt32)
			v2 := rand.Intn(math.MaxInt32) + 1
			return float32(v1) / float32(v2)
		})
	case types.T_float64:
		appendMockFloats(vec, rows, unique, func() float64 {
			v1 := rand.Intn(math.MaxInt32)
			v2 := rand.Intn(math.MaxInt32) + 1
			return float64(v1) / float64(v2)
		})
	case types.T_varchar, types.T_char, types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		if unique {
			for i := 0; i < rows; i++ {
				s := fmt.Sprintf("%d-%d", i, 0)
				vec.MustAppendValue([]byte(s), false)
			}
		} else {
			for i := 0; i < rows; i++ {
				s := fmt.Sprintf("%d%d", i, rand.Intn(10000000))
				vec.MustAppendValue([]byte(s), false)
			}
		}
	case types.T_datetime:
		for i := 1; i <= rows; i++ {
			vec.MustAppendValue(types.DatetimeFromClock(int32(i*100), 1, 1, 1, 1, 1, 1), false)
		}
	case types.T_date:
		for i := 1; i <= rows; i++ {
			vec.MustAppendValue(types.DateFromCalendar(int32(i+1)*100, 1, 1), false)
		}
	case types.T_time:
		for i := 1; i <= rows; i++ {
			vec.MustAppendValue(types.TimeFromClock(false, 1, 1, 1, 1), false)
		}
	case types.T_timestamp:
		if unique {
			for i := int32(1); i <= int32(rows); i++ {
				vec.MustAppendValue(types.Timestamp(common.NextGlobalSeqNum()), false)
			}
		} else {
			for i := int32(1); i <= int32(rows); i++ {
				ts, err := types.ParseTimestamp(time.UTC, time.Now().Format(time.DateTime), t.Scale)
				if err != nil {
					panic(err)
				}
				vec.MustAppendValue(ts, false)
			}
		}
	case types.T_decimal64:
		for i := int32(1); i <= int32(rows); i++ {
			d := types.Decimal64(common.NextGlobalSeqNum())
			vec.MustAppendValue(d, false)
		}
	case types.T_decimal128:
		for i := int32(1); i <= int32(rows); i++ {
			d := types.Decimal128{B0_63: common.NextGlobalSeqNum(), B64_127: 0}
			vec.MustAppendValue(d, false)
		}
	case types.T_TS:
		for i := int32(1); i <= int32(rows); i++ {
			vec.MustAppendValue(types.BuildTS(int64(i+1), uint32(i%16)), false)
		}
	case types.T_Rowid:
		for i := int32(1); i <= int32(rows); i++ {
			vec.MustAppendValue(types.BuildTestRowid(int64(i+1), int64(i)), false)
		}
	case types.T_Blockid:
		for i := int32(1); i <= int32(rows); i++ {
			vec.MustAppendValue(types.BuildTestBlockid(int64(i+1), int64(i)), false)
		}
	case types.T_enum:
		for i := int32(1); i <= int32(rows); i++ {
			vec.MustAppendValue(types.Enum(i), false)
		}
	case types.T_array_float32:
		// Used for creating MockBatch in DN txn_test.go
		for i := float32(1); i <= float32(rows); i++ {
			vec.MustAppendValue(types.ArrayToBytes([]float32{i + 1, i}), false)
		}
	case types.T_array_float64:
		for i := float64(1); i <= float64(rows); i++ {
			vec.MustAppendValue(types.ArrayToBytes([]float64{i + 1, i}), false)
		}
	default:
		if t.IsVarlen() {
			panic(fmt.Sprintf("%v %s", t.Oid, "not supported"))
		}
		// any other fixed-size type: distinct values from the global sequence
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(vector.FixedValueOfInt(t.Oid, int64(common.NextGlobalSeqNum())), false)
		}
	}
	return
}

func MockVector2(typ types.Type, rows int, offset int) Vector {
	vec := MakeVector(typ, common.DefaultAllocator)
	switch typ.Oid {
	case types.T_bool:
		for i := 0; i < rows; i++ {
			if i%2 == 0 {
				vec.MustAppendValue(true, false)
			} else {
				vec.MustAppendValue(false, false)
			}
		}
	case types.T_bit:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(uint64(i+offset), false)
		}
	case types.T_int8:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(int8(i+offset), false)
		}
	case types.T_int16:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(int16(i+offset), false)
		}
	case types.T_int32:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(int32(i+offset), false)
		}
	case types.T_int64:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(int64(i+offset), false)
		}
	case types.T_uint8:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(uint8(i+offset), false)
		}
	case types.T_uint16:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(uint16(i+offset), false)
		}
	case types.T_uint32:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(uint32(i+offset), false)
		}
	case types.T_uint64:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(uint64(i+offset), false)
		}
	case types.T_float32:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(float32(i+offset), false)
		}
	case types.T_float64:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(float64(i+offset), false)
		}
	case types.T_decimal64:
		for i := 0; i < rows; i++ {
			d := types.Decimal64(int64(i + offset))
			vec.MustAppendValue(d, false)
		}
	case types.T_decimal128:
		for i := 0; i < rows; i++ {
			d := types.Decimal128{B0_63: uint64(i + offset), B64_127: 0}
			vec.MustAppendValue(d, false)
		}
	case types.T_timestamp:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(types.Timestamp(i+offset), false)
		}
	case types.T_date:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(types.Date(i+offset), false)
		}
	case types.T_time:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(types.Time(i+offset), false)
		}
	case types.T_datetime:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(types.Datetime(i+offset), false)
		}
	case types.T_enum:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(types.Enum(i+offset), false)
		}
	case types.T_char, types.T_varchar, types.T_binary,
		types.T_varbinary, types.T_blob, types.T_text:
		for i := 0; i < rows; i++ {
			vec.MustAppendValue([]byte(strconv.Itoa(i+offset)), false)
		}
	case types.T_array_float32:
		for i := 0; i < rows; i++ {
			arr := types.ArrayToBytes[float32]([]float32{float32(i + offset)}) // 1-D vector
			vec.MustAppendValue(arr, false)
		}
	case types.T_array_float64:
		for i := 0; i < rows; i++ {
			arr := types.ArrayToBytes[float64]([]float64{float64(i + offset)}) // 1-D vector
			vec.MustAppendValue(arr, false)
		}
	default:
		if typ.IsVarlen() {
			panic("not support")
		}
		for i := 0; i < rows; i++ {
			vec.MustAppendValue(vector.FixedValueOfInt(typ.Oid, int64(i+offset)), false)
		}
	}
	return vec
}

func MockBatchWithAttrs(vecTypes []types.Type, attrs []string, rows int, uniqueIdx int, provider *MockDataProvider) (bat *Batch) {
	bat = MockNullableBatch(vecTypes, rows, uniqueIdx, provider)
	bat.Attrs = attrs
	return
}

func MockNullableBatch(vecTypes []types.Type, rows int, uniqueIdx int, provider *MockDataProvider) (bat *Batch) {
	bat = NewEmptyBatch()
	for idx := range vecTypes {
		attr := "mock_" + strconv.Itoa(idx)
		unique := uniqueIdx == idx
		vec := MockVector(vecTypes[idx], rows, unique, provider.GetColumnProvider(idx))
		bat.AddVector(attr, vec)
	}
	return bat
}

func MockBatch(vecTypes []types.Type, rows int, uniqueIdx int, provider *MockDataProvider) *Batch {
	return MockNullableBatch(vecTypes, rows, uniqueIdx, provider)
}

func MockBatchWithAttrsAndOffset(vecTypes []types.Type, attrs []string, rows int, offset int) (bat *Batch) {
	bat = NewEmptyBatch()
	for idx := range vecTypes {
		vec := MockVector2(vecTypes[idx], rows, offset)
		bat.AddVector(attrs[idx], vec)
	}
	return bat
}
