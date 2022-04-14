package compute

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"

	"github.com/bxcodec/faker/v3"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
)

func MockIVector(t types.Type, rows uint64, unique bool, provider *gvec.Vector) vector.IVector {
	if provider != nil {
		vec := vector.NewVector(t, rows)
		if _, err := vec.AppendVector(provider, 0); err != nil {
			panic(err)
		}
		return vec
	}
	var vec vector.IVector
	switch t.Oid {
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
		vec.Append(len(vals), vals)
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
		vec.Append(len(vals), vals)
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
		vec.Append(len(vals), vals)
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
		vec.Append(len(vals), vals)
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
		vec.Append(len(vals), vals)
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
		vec.Append(len(vals), vals)
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
		vec.Append(len(vals), vals)
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
		vec.Append(len(vals), vals)
	case types.T_float32:
		vec = vector.NewStdVector(t, rows)
		var vals []float32
		for i := uint64(0); i < rows; i++ {
			val1 := rand.Intn(math.MaxInt32)
			val2 := rand.Intn(math.MaxInt32) + 1
			vals = append(vals, float32(val1)/float32(val2))
		}
		vec.Append(len(vals), vals)
	case types.T_float64:
		vec = vector.NewStdVector(t, rows)
		var vals []float64
		for i := uint64(0); i < rows; i++ {
			val1 := rand.Intn(math.MaxInt32)
			val2 := rand.Intn(math.MaxInt32) + 1
			vals = append(vals, float64(val1)/float64(val2))
		}
		vec.Append(len(vals), vals)
	case types.T_varchar, types.T_char:
		vec = vector.NewStrVector(t, rows)
		vals := make([][]byte, 0, rows)
		if unique {
			for i := uint64(0); i < rows; i++ {
				str, _ := faker.GetAddress().Latitude(reflect.Value{})
				s := fmt.Sprintf("%d-%s", i, str)
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
		vec.Append(len(vals), vals)
	case types.T_datetime:
		vec = vector.NewStdVector(t, rows)
		vals := make([]types.Datetime, 0, rows)
		for i := uint64(1); i <= rows; i++ {
			vals = append(vals, types.FromClock(int32(i*100), 1, 1, 1, 1, 1, 1))
		}
		vec.Append(len(vals), vals)
	case types.T_date:
		vec = vector.NewStdVector(t, rows)
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

type MockDataProvider struct {
	providers map[int]*gvec.Vector
}

func NewMockDataProvider() *MockDataProvider {
	return &MockDataProvider{
		providers: make(map[int]*gvec.Vector),
	}
}

func (p *MockDataProvider) Reset() {
	p.providers = make(map[int]*gvec.Vector)
}

func (p *MockDataProvider) AddColumnProvider(colIdx int, provider *gvec.Vector) {
	p.providers[colIdx] = provider
}

func (p *MockDataProvider) GetColumnProvider(colIdx int) *gvec.Vector {
	if p == nil {
		return nil
	}
	return p.providers[colIdx]
}

func MockBatch(types []types.Type, rows uint64, uniqueIdx int, provider *MockDataProvider) *gbat.Batch {
	attrs := make([]string, len(types))
	for idx := range types {
		attrs[idx] = "mock_" + strconv.Itoa(idx)
	}

	bat := gbat.New(true, attrs)
	var err error
	for i, colType := range types {
		unique := false
		if uniqueIdx == i {
			unique = true
		}
		vec := MockIVector(colType, rows, unique, provider.GetColumnProvider(i))
		vec = vec.GetLatestView()
		bat.Vecs[i], err = vec.CopyToVector()
		if err != nil {
			panic(err)
		}
		vec.Close()
	}
	return bat
}
