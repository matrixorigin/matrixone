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

func MockVector(t types.Type, rows int, unique, nullable bool, provider Vector) (vec Vector) {
	rand.Seed(time.Now().UnixNano())
	vec = MakeVector(t, nullable)
	if provider != nil {
		vec.Extend(provider)
		return
	}

	switch t.Oid {
	case types.T_bool:
		for i := 0; i < rows; i++ {
			if i%2 == 0 {
				vec.Append(true)
			} else {
				vec.Append(false)
			}
		}
	case types.T_int8:
		if unique {
			if int(rows) >= math.MaxInt8-math.MinInt8 {
				panic("overflow")
			}
			for i := 0; i < rows; i++ {
				vec.Append(math.MinInt8 + int8(i))
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Intn(math.MaxInt8)
				vec.Append(int8(ival))
			}
		}
	case types.T_int16:
		if unique {
			for i := 0; i < rows; i++ {
				vec.Append(int16(i))
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Intn(math.MaxInt16)
				vec.Append(int16(ival))
			}
		}
	case types.T_int32:
		if unique {
			for i := 0; i < rows; i++ {
				vec.Append(int32(i))
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Intn(math.MaxInt32)
				vec.Append(int32(ival))
			}
		}
	case types.T_int64:
		if unique {
			for i := 0; i < rows; i++ {
				vec.Append(int64(i))
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Intn(math.MaxInt64)
				vec.Append(int64(ival))
			}
		}
	case types.T_uint8:
		if unique {
			if int(rows) >= math.MaxUint8 {
				panic("overflow")
			}
			for i := 0; i < rows; i++ {
				vec.Append(uint8(i))
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Intn(math.MaxUint8)
				vec.Append(uint8(ival))
			}
		}
	case types.T_uint16:
		if unique {
			for i := 0; i < rows; i++ {
				vec.Append(uint16(i))
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Intn(int(math.MaxInt16))
				vec.Append(uint16(ival))
			}
		}
	case types.T_uint32:
		if unique {
			for i := 0; i < rows; i++ {
				vec.Append(uint32(i))
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Int31()
				vec.Append(uint32(ival))
			}
		}
	case types.T_uint64:
		if unique {
			for i := 0; i < rows; i++ {
				vec.Append(uint64(i))
			}
		} else {
			for i := 0; i < rows; i++ {
				ival := rand.Int63()
				vec.Append(uint64(ival))
			}
		}
	case types.T_float32:
		for i := 0; i < rows; i++ {
			v1 := rand.Intn(math.MaxInt32)
			v2 := rand.Intn(math.MaxInt32) + 1
			vec.Append(float32(v1) / float32(v2))
		}
	case types.T_float64:
		for i := 0; i < rows; i++ {
			v1 := rand.Intn(math.MaxInt32)
			v2 := rand.Intn(math.MaxInt32) + 1
			vec.Append(float64(v1) / float64(v2))
		}
	case types.T_varchar, types.T_char, types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		if unique {
			for i := 0; i < rows; i++ {
				s := fmt.Sprintf("%d-%d", i, 0)
				vec.Append([]byte(s))
			}
		} else {
			for i := 0; i < rows; i++ {
				s := fmt.Sprintf("%d%d", i, rand.Intn(10000000))
				vec.Append([]byte(s))
			}
		}
	case types.T_datetime:
		for i := 1; i <= rows; i++ {
			vec.Append(types.DatetimeFromClock(int32(i*100), 1, 1, 1, 1, 1, 1))
		}
	case types.T_date:
		for i := 1; i <= rows; i++ {
			vec.Append(types.DateFromCalendar(int32(i)*100, 1, 1))
		}
	case types.T_time:
		for i := 1; i <= rows; i++ {
			vec.Append(types.TimeFromClock(false, 1, 1, 1, 1))
		}
	case types.T_timestamp:
		for i := int32(1); i <= int32(rows); i++ {
			vec.Append(types.Timestamp(common.NextGlobalSeqNum()))
		}
	case types.T_decimal64:
		for i := int32(1); i <= int32(rows); i++ {
			d := types.Decimal64(common.NextGlobalSeqNum())
			vec.Append(d)
		}
	case types.T_decimal128:
		for i := int32(1); i <= int32(rows); i++ {
			d := types.Decimal128{B0_63: common.NextGlobalSeqNum(), B64_127: 0}
			vec.Append(d)
		}
	case types.T_TS:
		for i := int32(1); i <= int32(rows); i++ {
			vec.Append(types.BuildTS(int64(i+1), uint32(i%16)))
		}
	case types.T_Rowid:
		for i := int32(1); i <= int32(rows); i++ {
			vec.Append(types.BuildTestRowid(int64(i+1), int64(i)))
		}
	case types.T_Blockid:
		for i := int32(1); i <= int32(rows); i++ {
			vec.Append(types.BuildTestBlockid(int64(i+1), int64(i)))
		}
	default:
		panic("not supported")
	}
	return
}

func MockVector2(typ types.Type, rows int, offset int) Vector {
	vec := MakeVector(typ, true)
	switch typ.Oid {
	case types.T_bool:
		for i := 0; i < rows; i++ {
			if i%2 == 0 {
				vec.Append(true)
			} else {
				vec.Append(false)
			}
		}
	case types.T_int8:
		for i := 0; i < rows; i++ {
			vec.Append(int8(i + offset))
		}
	case types.T_int16:
		for i := 0; i < rows; i++ {
			vec.Append(int16(i + offset))
		}
	case types.T_int32:
		for i := 0; i < rows; i++ {
			vec.Append(int32(i + offset))
		}
	case types.T_int64:
		for i := 0; i < rows; i++ {
			vec.Append(int64(i + offset))
		}
	case types.T_uint8:
		for i := 0; i < rows; i++ {
			vec.Append(uint8(i + offset))
		}
	case types.T_uint16:
		for i := 0; i < rows; i++ {
			vec.Append(uint16(i + offset))
		}
	case types.T_uint32:
		for i := 0; i < rows; i++ {
			vec.Append(uint32(i + offset))
		}
	case types.T_uint64:
		for i := 0; i < rows; i++ {
			vec.Append(uint64(i + offset))
		}
	case types.T_float32:
		for i := 0; i < rows; i++ {
			vec.Append(float32(i + offset))
		}
	case types.T_float64:
		for i := 0; i < rows; i++ {
			vec.Append(float64(i + offset))
		}
	case types.T_decimal64:
		for i := 0; i < rows; i++ {
			d := types.Decimal64(int64(i + offset))
			vec.Append(d)
		}
	case types.T_decimal128:
		for i := 0; i < rows; i++ {
			d := types.Decimal128{B0_63: uint64(i + offset), B64_127: 0}
			vec.Append(d)
		}
	case types.T_timestamp:
		for i := 0; i < rows; i++ {
			vec.Append(types.Timestamp(i + offset))
		}
	case types.T_date:
		for i := 0; i < rows; i++ {
			vec.Append(types.Date(i + offset))
		}
	case types.T_time:
		for i := 0; i < rows; i++ {
			vec.Append(types.Time(i + offset))
		}
	case types.T_datetime:
		for i := 0; i < rows; i++ {
			vec.Append(types.Datetime(i + offset))
		}
	case types.T_char, types.T_varchar, types.T_binary,
		types.T_varbinary, types.T_blob, types.T_text:
		for i := 0; i < rows; i++ {
			vec.Append([]byte(strconv.Itoa(i + offset)))
		}
	default:
		panic("not support")
	}
	return vec
}

func MockVector3(typ types.Type, rows int) Vector {
	vec := MakeVector(typ, true)
	switch typ.Oid {
	case types.T_int32:
		for i := 0; i < rows; i++ {
			vec.Append(int32(rows))
		}
	case types.T_int64:
		for i := 0; i < rows; i++ {
			vec.Append(int64(rows))
		}
	case types.T_uint32:
		for i := 0; i < rows; i++ {
			vec.Append(uint32(i))
			vec.Append(uint32(i))
			i++
		}
	case types.T_uint64:
		for i := 0; i < rows; i++ {
			vec.Append(uint64(rows))
		}
	default:
		panic("not support")
	}
	return vec
}

func MockBatchWithAttrs(vecTypes []types.Type, attrs []string, nullables []bool, rows int, uniqueIdx int, provider *MockDataProvider) (bat *Batch) {
	bat = MockNullableBatch(vecTypes, nullables, rows, uniqueIdx, provider)
	bat.Attrs = attrs
	return
}

func MockNullableBatch(vecTypes []types.Type, nullables []bool, rows int, uniqueIdx int, provider *MockDataProvider) (bat *Batch) {
	bat = NewEmptyBatch()
	for idx := range vecTypes {
		attr := "mock_" + strconv.Itoa(idx)
		unique := uniqueIdx == idx
		vec := MockVector(vecTypes[idx], rows, unique, nullables[idx], provider.GetColumnProvider(idx))
		bat.AddVector(attr, vec)
	}
	return bat
}

func MockBatch(vecTypes []types.Type, rows int, uniqueIdx int, provider *MockDataProvider) (bat *Batch) {
	bat = NewEmptyBatch()
	for idx := range vecTypes {
		attr := "mock_" + strconv.Itoa(idx)
		unique := uniqueIdx == idx
		nullable := !unique
		vec := MockVector(vecTypes[idx], rows, unique, nullable, provider.GetColumnProvider(idx))
		bat.AddVector(attr, vec)
	}
	return bat
}

type compressedFileInfo struct {
	size  int64
	osize int64
	algo  int
}

func (i *compressedFileInfo) Name() string      { return "" }
func (i *compressedFileInfo) Size() int64       { return i.size }
func (i *compressedFileInfo) OriginSize() int64 { return i.osize }
func (i *compressedFileInfo) CompressAlgo() int { return i.algo }

type mockCompressedFile struct {
	stat compressedFileInfo
	buf  []byte
}

func (f *mockCompressedFile) Read(p []byte) (n int, err error) {
	copy(p, f.buf)
	n = len(f.buf)
	return
}

func (f *mockCompressedFile) Ref()                         {}
func (f *mockCompressedFile) Unref()                       {}
func (f *mockCompressedFile) RefCount() int64              { return 0 }
func (f *mockCompressedFile) Stat() common.FileInfo        { return &f.stat }
func (f *mockCompressedFile) GetFileType() common.FileType { return common.MemFile }

func MockCompressedFile(buf []byte, osize int, algo int) common.IVFile {
	return &mockCompressedFile{
		stat: compressedFileInfo{
			size:  int64(len(buf)),
			osize: int64(osize),
			algo:  algo,
		},
		buf: buf,
	}
}
