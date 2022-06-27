package containers

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
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
	case types.Type_BOOL:
		for i := 0; i < rows; i++ {
			if i%2 == 0 {
				vec.Append(true)
			} else {
				vec.Append(false)
			}
		}
	case types.Type_INT8:
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
	case types.Type_INT16:
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
	case types.Type_INT32:
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
	case types.Type_INT64:
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
	case types.Type_UINT8:
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
	case types.Type_UINT16:
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
	case types.Type_UINT32:
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
	case types.Type_UINT64:
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
	case types.Type_FLOAT32:
		for i := 0; i < rows; i++ {
			v1 := rand.Intn(math.MaxInt32)
			v2 := rand.Intn(math.MaxInt32) + 1
			vec.Append(float32(v1) / float32(v2))
		}
	case types.Type_FLOAT64:
		for i := 0; i < rows; i++ {
			v1 := rand.Intn(math.MaxInt32)
			v2 := rand.Intn(math.MaxInt32) + 1
			vec.Append(float64(v1) / float64(v2))
		}
	case types.Type_VARCHAR, types.Type_CHAR:
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
	case types.Type_DATETIME:
		for i := 1; i <= rows; i++ {
			vec.Append(types.FromClock(int32(i*100), 1, 1, 1, 1, 1, 1))
		}
	case types.Type_DATE:
		for i := 1; i <= rows; i++ {
			vec.Append(types.FromCalendar(int32(i)*100, 1, 1))
		}
	case types.Type_TIMESTAMP:
		for i := int32(1); i <= int32(rows); i++ {
			vec.Append(types.Timestamp(common.NextGlobalSeqNum()))
		}
	case types.Type_DECIMAL64:
		for i := int32(1); i <= int32(rows); i++ {
			vec.Append(types.Decimal64(common.NextGlobalSeqNum()))
		}
	case types.Type_DECIMAL128:
		for i := int32(1); i <= int32(rows); i++ {
			vec.Append(types.Decimal128{Lo: int64(common.NextGlobalSeqNum())})
		}
	default:
		panic("not supported")
	}
	return
}

func MockVector2(typ types.Type, rows int, offset int) Vector {
	vec := MakeVector(typ, true)
	switch typ.Oid {
	case types.Type_BOOL:
		for i := 0; i < rows; i++ {
			if i%2 == 0 {
				vec.Append(true)
			} else {
				vec.Append(false)
			}
		}
	case types.Type_INT8:
		for i := 0; i < rows; i++ {
			vec.Append(int8(i + offset))
		}
	case types.Type_INT16:
		for i := 0; i < rows; i++ {
			vec.Append(int16(i + offset))
		}
	case types.Type_INT32:
		for i := 0; i < rows; i++ {
			vec.Append(int32(i + offset))
		}
	case types.Type_INT64:
		for i := 0; i < rows; i++ {
			vec.Append(int64(i + offset))
		}
	case types.Type_UINT8:
		for i := 0; i < rows; i++ {
			vec.Append(uint8(i + offset))
		}
	case types.Type_UINT16:
		for i := 0; i < rows; i++ {
			vec.Append(uint16(i + offset))
		}
	case types.Type_UINT32:
		for i := 0; i < rows; i++ {
			vec.Append(uint32(i + offset))
		}
	case types.Type_UINT64:
		for i := 0; i < rows; i++ {
			vec.Append(uint64(i + offset))
		}
	case types.Type_FLOAT32:
		for i := 0; i < rows; i++ {
			vec.Append(float32(i + offset))
		}
	case types.Type_FLOAT64:
		for i := 0; i < rows; i++ {
			vec.Append(float64(i + offset))
		}
	case types.Type_DECIMAL64:
		for i := 0; i < rows; i++ {
			vec.Append(types.Decimal64(i + offset))
		}
	case types.Type_DECIMAL128:
		for i := 0; i < rows; i++ {
			vec.Append(types.Decimal128{Lo: int64(i + offset)})
		}
	case types.Type_TIMESTAMP:
		for i := 0; i < rows; i++ {
			vec.Append(types.Timestamp(i + offset))
		}
	case types.Type_DATE:
		for i := 0; i < rows; i++ {
			vec.Append(types.Date(i + offset))
		}
	case types.Type_DATETIME:
		for i := 0; i < rows; i++ {
			vec.Append(types.Datetime(i + offset))
		}
	case types.Type_CHAR, types.Type_VARCHAR:
		for i := 0; i < rows; i++ {
			vec.Append([]byte(strconv.Itoa(i + offset)))
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
