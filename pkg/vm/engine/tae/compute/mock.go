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

package compute

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"

	"github.com/bxcodec/faker/v3"
	mobat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
)

func MockIVector(t types.Type, rows uint64, unique bool, provider *movec.Vector) vector.IVector {
	if provider != nil {
		vec := vector.NewVector(t, rows)
		if _, err := vec.AppendVector(provider, 0); err != nil {
			panic(err)
		}
		return vec
	}
	var vec vector.IVector
	switch t.Oid {
	case types.T_bool:
		vec = vector.NewStdVector(t, rows)
		vals := make([]bool, 0, rows)
		for i := int32(1); i <= int32(rows); i++ {
			if i%2 == 0 {
				vals = append(vals, true)
			} else {
				vals = append(vals, false)
			}
		}
		_ = vec.Append(len(vals), vals)
	case types.T_int8:
		vec = vector.NewStdVector(t, rows)
		var vals []int8
		if unique {
			if int(rows) >= math.MaxInt8-math.MinInt8 {
				panic("overflow")
			}
			for i := uint64(0); i < rows; i++ {
				vals = append(vals, math.MinInt8+int8(i))
			}
		} else {
			for i := uint64(0); i < rows; i++ {
				ival := rand.Intn(math.MaxInt8)
				vals = append(vals, int8(ival))
			}
		}
		_ = vec.Append(len(vals), vals)
	case types.T_int16:
		vec = vector.NewStdVector(t, rows)
		var vals []int16
		if unique {
			for i := uint64(0); i < rows; i++ {
				vals = append(vals, math.MinInt16+int16(i))
			}
		} else {
			for i := uint64(0); i < rows; i++ {
				ival := rand.Intn(math.MaxInt16)
				vals = append(vals, int16(ival))
			}
		}
		_ = vec.Append(len(vals), vals)
	case types.T_int32:
		vec = vector.NewStdVector(t, rows)
		var vals []int32
		if unique {
			for i := uint64(0); i < rows; i++ {
				vals = append(vals, int32(i))
			}
		} else {
			for i := uint64(0); i < rows; i++ {
				ival := rand.Intn(math.MaxInt32)
				vals = append(vals, int32(ival))
			}
		}
		_ = vec.Append(len(vals), vals)
	case types.T_int64:
		vec = vector.NewStdVector(t, rows)
		var vals []int64
		if unique {
			for i := uint64(0); i < rows; i++ {
				vals = append(vals, int64(i))
			}
		} else {
			for i := uint64(0); i < rows; i++ {
				ival := rand.Intn(math.MaxInt32)
				vals = append(vals, int64(ival))
			}
		}
		_ = vec.Append(len(vals), vals)
	case types.T_uint8:
		vec = vector.NewStdVector(t, rows)
		var vals []uint8
		if unique {
			if int(rows) >= math.MaxUint8 {
				panic("overflow")
			}
			for i := uint64(0); i < rows; i++ {
				vals = append(vals, uint8(i))
			}
		} else {
			for i := uint64(0); i < rows; i++ {
				ival := rand.Intn(math.MaxUint8)
				vals = append(vals, uint8(ival))
			}
		}
		_ = vec.Append(len(vals), vals)
	case types.T_uint16:
		vec = vector.NewStdVector(t, rows)
		var vals []uint16
		if unique {
			for i := uint64(0); i < rows; i++ {
				vals = append(vals, uint16(i))
			}
		} else {
			for i := uint64(0); i < rows; i++ {
				ival := rand.Intn(math.MaxUint16)
				vals = append(vals, uint16(ival))
			}
		}
		_ = vec.Append(len(vals), vals)
	case types.T_uint32:
		vec = vector.NewStdVector(t, rows)
		var vals []uint32
		if unique {
			for i := uint64(0); i < rows; i++ {
				vals = append(vals, uint32(i))
			}
		} else {
			for i := uint64(0); i < rows; i++ {
				ival := rand.Intn(math.MaxUint32)
				vals = append(vals, uint32(ival))
			}
		}
		_ = vec.Append(len(vals), vals)
	case types.T_uint64:
		vec = vector.NewStdVector(t, rows)
		var vals []uint64
		if unique {
			for i := uint64(0); i < rows; i++ {
				vals = append(vals, uint64(i))
			}
		} else {
			for i := uint64(0); i < rows; i++ {
				ival := rand.Intn(math.MaxUint32)
				vals = append(vals, uint64(ival))
			}
		}
		_ = vec.Append(len(vals), vals)
	case types.T_float32:
		vec = vector.NewStdVector(t, rows)
		var vals []float32
		for i := uint64(0); i < rows; i++ {
			val1 := rand.Intn(math.MaxInt32)
			val2 := rand.Intn(math.MaxInt32) + 1
			vals = append(vals, float32(val1)/float32(val2))
		}
		_ = vec.Append(len(vals), vals)
	case types.T_float64:
		vec = vector.NewStdVector(t, rows)
		var vals []float64
		for i := uint64(0); i < rows; i++ {
			val1 := rand.Intn(math.MaxInt32)
			val2 := rand.Intn(math.MaxInt32) + 1
			vals = append(vals, float64(val1)/float64(val2))
		}
		_ = vec.Append(len(vals), vals)
	case types.T_varchar, types.T_char:
		vec = vector.NewStrVector(t, rows)
		vals := make([][]byte, 0, rows)
		if unique {
			for i := uint64(0); i < rows; i++ {
				str, _ := faker.GetAddress().Latitude(reflect.Value{})
				s := fmt.Sprintf("%d-%v", i, str)
				vals = append(vals, []byte(s))
			}
		} else {
			for i := uint64(0); i < rows; i++ {
				str1, _ := faker.GetPerson().FirstName(reflect.Value{})
				str2, _ := faker.GetPerson().LastName(reflect.Value{})
				s := fmt.Sprintf("%v%v", str1, str2)
				vals = append(vals, []byte(s))
			}
		}
		_ = vec.Append(len(vals), vals)
	case types.T_datetime:
		vec = vector.NewStdVector(t, rows)
		vals := make([]types.Datetime, 0, rows)
		for i := uint64(1); i <= rows; i++ {
			vals = append(vals, types.FromClock(int32(i*100), 1, 1, 1, 1, 1, 1))
		}
		_ = vec.Append(len(vals), vals)
	case types.T_date:
		vec = vector.NewStdVector(t, rows)
		vals := make([]types.Date, 0, rows)
		for i := int32(1); i <= int32(rows); i++ {
			vals = append(vals, types.FromCalendar(i*100, 1, 1))
		}
		_ = vec.Append(len(vals), vals)
	case types.T_timestamp:
		vec = vector.NewStdVector(t, rows)
		vals := make([]types.Timestamp, 0, rows)
		for i := int32(1); i <= int32(rows); i++ {
			vals = append(vals, types.Timestamp(common.NextGlobalSeqNum()))
		}
		_ = vec.Append(len(vals), vals)
	case types.T_decimal64:
		vec = vector.NewStdVector(t, rows)
		vals := make([]types.Decimal64, 0, rows)
		for i := int32(1); i <= int32(rows); i++ {
			vals = append(vals, types.Decimal64(common.NextGlobalSeqNum()))
		}
		_ = vec.Append(len(vals), vals)
	case types.T_decimal128:
		vec = vector.NewStdVector(t, rows)
		vals := make([]types.Decimal128, 0, rows)
		for i := int32(1); i <= int32(rows); i++ {
			vals = append(vals, types.Decimal128{Lo: int64(common.NextGlobalSeqNum())})
		}
		_ = vec.Append(len(vals), vals)
	default:
		panic("not supported")
	}
	return vec
}

type MockDataProvider struct {
	providers map[int]*movec.Vector
}

func NewMockDataProvider() *MockDataProvider {
	return &MockDataProvider{
		providers: make(map[int]*movec.Vector),
	}
}

func (p *MockDataProvider) Reset() {
	p.providers = make(map[int]*movec.Vector)
}

func (p *MockDataProvider) AddColumnProvider(colIdx int, provider *movec.Vector) {
	p.providers[colIdx] = provider
}

func (p *MockDataProvider) GetColumnProvider(colIdx int) *movec.Vector {
	if p == nil {
		return nil
	}
	return p.providers[colIdx]
}

func MockBatchWithAttrs(types []types.Type, attrs []string, rows uint64, uniqueIdx int, provider *MockDataProvider) *mobat.Batch {
	bat := MockBatch(types, rows, uniqueIdx, provider)
	bat.Attrs = attrs
	return bat
}

func MockBatch(types []types.Type, rows uint64, uniqueIdx int, provider *MockDataProvider) *mobat.Batch {
	attrs := make([]string, len(types))
	for idx := range types {
		attrs[idx] = "mock_" + strconv.Itoa(idx)
	}

	bat := mobat.New(true, attrs)
	var err error
	for i, colType := range types {
		unique := false
		if uniqueIdx == i {
			unique = true
		}
		vec := MockIVector(colType, rows, unique, provider.GetColumnProvider(i))
		vec2 := vec.GetLatestView()
		bat.Vecs[i], err = vec2.CopyToVector()
		if err != nil {
			panic(err)
		}
		vec.Close()
	}
	return bat
}

func MockVec(typ types.Type, rows int, offset int) *movec.Vector {
	vec := movec.New(typ)
	switch typ.Oid {
	case types.T_bool:
		data := make([]bool, 0)
		for i := 0; i < rows; i++ {
			if i%2 == 0 {
				data = append(data, true)
			} else {
				data = append(data, false)
			}
		}
		_ = movec.Append(vec, data)
	case types.T_int8:
		data := make([]int8, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int8(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_int16:
		data := make([]int16, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int16(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_int32:
		data := make([]int32, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int32(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_int64:
		data := make([]int64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int64(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_uint8:
		data := make([]uint8, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint8(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_uint16:
		data := make([]uint16, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint16(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_uint32:
		data := make([]uint32, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint32(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_uint64:
		data := make([]uint64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint64(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_float32:
		data := make([]float32, 0)
		for i := 0; i < rows; i++ {
			data = append(data, float32(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_float64:
		data := make([]float64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, float64(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_decimal64:
		data := make([]types.Decimal64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Decimal64(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_decimal128:
		data := make([]types.Decimal128, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Decimal128{Lo: int64(i + offset)})
		}
		_ = movec.Append(vec, data)
	case types.T_timestamp:
		data := make([]types.Timestamp, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Timestamp(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_date:
		data := make([]types.Date, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Date(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_datetime:
		data := make([]types.Datetime, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Datetime(i+offset))
		}
		_ = movec.Append(vec, data)
	case types.T_char, types.T_varchar:
		data := make([][]byte, 0)
		for i := 0; i < rows; i++ {
			data = append(data, []byte(strconv.Itoa(i+offset)))
		}
		_ = movec.Append(vec, data)
	default:
		panic("not support")
	}
	return vec
}
