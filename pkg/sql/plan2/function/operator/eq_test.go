package operator

import (
	"errors"
	"log"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
)

var EqintBool = []bool{true, false, false, false, false, true, false, true, false, false, true, false, false, true, false, true}
var intNullPos = []int{3, 7, 11, 12, 13, 14, 15}
var intNotNullPos = []int{0, 1, 2, 4, 5, 6, 8, 9, 10}

func GetInt8() []*vector.Vector {
	v0 := []int8{-1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0}
	v1 := []int8{-1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_int8
	nulls.Add(vec[0].Nsp, 12)
	nulls.Add(vec[0].Nsp, 13)
	nulls.Add(vec[0].Nsp, 14)
	nulls.Add(vec[0].Nsp, 15)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_int8
	nulls.Add(vec[1].Nsp, 3)
	nulls.Add(vec[1].Nsp, 7)
	nulls.Add(vec[1].Nsp, 11)
	nulls.Add(vec[1].Nsp, 15)
	return vec
}

func GetInt16() []*vector.Vector {
	v0 := []int16{-1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0}
	v1 := []int16{-1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_int16
	nulls.Add(vec[0].Nsp, 12)
	nulls.Add(vec[0].Nsp, 13)
	nulls.Add(vec[0].Nsp, 14)
	nulls.Add(vec[0].Nsp, 15)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_int16
	nulls.Add(vec[1].Nsp, 3)
	nulls.Add(vec[1].Nsp, 7)
	nulls.Add(vec[1].Nsp, 11)
	nulls.Add(vec[1].Nsp, 15)
	return vec
}

func GetInt32() []*vector.Vector {
	v0 := []int32{-1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0}
	v1 := []int32{-1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_int32
	nulls.Add(vec[0].Nsp, 12)
	nulls.Add(vec[0].Nsp, 13)
	nulls.Add(vec[0].Nsp, 14)
	nulls.Add(vec[0].Nsp, 15)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_int32
	nulls.Add(vec[1].Nsp, 3)
	nulls.Add(vec[1].Nsp, 7)
	nulls.Add(vec[1].Nsp, 11)
	nulls.Add(vec[1].Nsp, 15)
	return vec
}

func GetInt64() []*vector.Vector {
	v0 := []int64{-1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0}
	v1 := []int64{-1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_int64
	nulls.Add(vec[0].Nsp, 12)
	nulls.Add(vec[0].Nsp, 13)
	nulls.Add(vec[0].Nsp, 14)
	nulls.Add(vec[0].Nsp, 15)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_int64
	nulls.Add(vec[1].Nsp, 3)
	nulls.Add(vec[1].Nsp, 7)
	nulls.Add(vec[1].Nsp, 11)
	nulls.Add(vec[1].Nsp, 15)
	return vec
}

var EquintBool = []bool{true, false, true, false, true, false, true, false, true}
var uintNullPos = []int{2, 5, 6, 7, 8}
var uintNotNullPos = []int{0, 1, 3, 4}

func GetUint8() []*vector.Vector {
	v0 := []uint8{0, 0, 0, 1, 1, 1, 0, 0, 0}
	v1 := []uint8{0, 1, 0, 0, 1, 0, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_uint8
	nulls.Add(vec[0].Nsp, 6)
	nulls.Add(vec[0].Nsp, 7)
	nulls.Add(vec[0].Nsp, 8)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_uint8
	nulls.Add(vec[1].Nsp, 2)
	nulls.Add(vec[1].Nsp, 5)
	nulls.Add(vec[1].Nsp, 8)
	return vec
}

func GetUint16() []*vector.Vector {
	v0 := []uint16{0, 0, 0, 1, 1, 1, 0, 0, 0}
	v1 := []uint16{0, 1, 0, 0, 1, 0, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_uint16
	nulls.Add(vec[0].Nsp, 6)
	nulls.Add(vec[0].Nsp, 7)
	nulls.Add(vec[0].Nsp, 8)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_uint16
	nulls.Add(vec[1].Nsp, 2)
	nulls.Add(vec[1].Nsp, 5)
	nulls.Add(vec[1].Nsp, 8)
	return vec
}

func GetUint32() []*vector.Vector {
	v0 := []uint32{0, 0, 0, 1, 1, 1, 0, 0, 0}
	v1 := []uint32{0, 1, 0, 0, 1, 0, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_uint32
	nulls.Add(vec[0].Nsp, 6)
	nulls.Add(vec[0].Nsp, 7)
	nulls.Add(vec[0].Nsp, 8)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_uint32
	nulls.Add(vec[1].Nsp, 2)
	nulls.Add(vec[1].Nsp, 5)
	nulls.Add(vec[1].Nsp, 8)
	return vec
}

func GetUint64() []*vector.Vector {
	v0 := []uint64{0, 0, 0, 1, 1, 1, 0, 0, 0}
	v1 := []uint64{0, 1, 0, 0, 1, 0, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_uint64
	nulls.Add(vec[0].Nsp, 6)
	nulls.Add(vec[0].Nsp, 7)
	nulls.Add(vec[0].Nsp, 8)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_uint64
	nulls.Add(vec[1].Nsp, 2)
	nulls.Add(vec[1].Nsp, 5)
	nulls.Add(vec[1].Nsp, 8)
	return vec
}

func GetFloat32() []*vector.Vector {
	v0 := []float32{-1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0}
	v1 := []float32{-1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_float32
	nulls.Add(vec[0].Nsp, 12)
	nulls.Add(vec[0].Nsp, 13)
	nulls.Add(vec[0].Nsp, 14)
	nulls.Add(vec[0].Nsp, 15)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_float32
	nulls.Add(vec[1].Nsp, 3)
	nulls.Add(vec[1].Nsp, 7)
	nulls.Add(vec[1].Nsp, 11)
	nulls.Add(vec[1].Nsp, 15)
	return vec
}

func GetFloat64() []*vector.Vector {
	v0 := []float64{-1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0}
	v1 := []float64{-1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_float64
	nulls.Add(vec[0].Nsp, 12)
	nulls.Add(vec[0].Nsp, 13)
	nulls.Add(vec[0].Nsp, 14)
	nulls.Add(vec[0].Nsp, 15)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_float64
	nulls.Add(vec[1].Nsp, 3)
	nulls.Add(vec[1].Nsp, 7)
	nulls.Add(vec[1].Nsp, 11)
	nulls.Add(vec[1].Nsp, 15)
	return vec
}

func GetDate() []*vector.Vector {
	v0 := []types.Date{-1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0}
	v1 := []types.Date{-1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_date
	nulls.Add(vec[0].Nsp, 12)
	nulls.Add(vec[0].Nsp, 13)
	nulls.Add(vec[0].Nsp, 14)
	nulls.Add(vec[0].Nsp, 15)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_date
	nulls.Add(vec[1].Nsp, 3)
	nulls.Add(vec[1].Nsp, 7)
	nulls.Add(vec[1].Nsp, 11)
	nulls.Add(vec[1].Nsp, 15)
	return vec
}

func GetDatetime() []*vector.Vector {
	v0 := []types.Datetime{-1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0}
	v1 := []types.Datetime{-1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_datetime
	nulls.Add(vec[0].Nsp, 12)
	nulls.Add(vec[0].Nsp, 13)
	nulls.Add(vec[0].Nsp, 14)
	nulls.Add(vec[0].Nsp, 15)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_datetime
	nulls.Add(vec[1].Nsp, 3)
	nulls.Add(vec[1].Nsp, 7)
	nulls.Add(vec[1].Nsp, 11)
	nulls.Add(vec[1].Nsp, 15)
	return vec
}

func GetDecimal64() []*vector.Vector {
	v0 := []types.Decimal64{-1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0}
	v1 := []types.Decimal64{-1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0, -1, 0, 1, 0}
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: v0,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_decimal64
	nulls.Add(vec[0].Nsp, 12)
	nulls.Add(vec[0].Nsp, 13)
	nulls.Add(vec[0].Nsp, 14)
	nulls.Add(vec[0].Nsp, 15)

	vec[1] = &vector.Vector{
		Col: v1,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_decimal64
	nulls.Add(vec[1].Nsp, 3)
	nulls.Add(vec[1].Nsp, 7)
	nulls.Add(vec[1].Nsp, 11)
	nulls.Add(vec[1].Nsp, 15)
	return vec
}

var EqboolBool = []bool{true, false, false, false, true, true, false, true, true}

func GetBool() []*vector.Vector {
	vec := make([]*vector.Vector, 2)
	vec[0] = &vector.Vector{
		Col: boolVec,
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_bool
	nulls.Add(vec[0].Nsp, 6)
	nulls.Add(vec[0].Nsp, 7)
	nulls.Add(vec[0].Nsp, 8)

	vec[1] = &vector.Vector{
		Col: []bool{true, false, false, true, false, false, true, false, false},
		Nsp: &nulls.Nulls{},
	}
	vec[1].Typ.Oid = types.T_bool
	nulls.Add(vec[1].Nsp, 2)
	nulls.Add(vec[1].Nsp, 5)
	nulls.Add(vec[1].Nsp, 8)
	return vec
}

var EqstringBool = []bool{true, false, false, false, false, true, false, false, false, false, true, false, false, false, false, true}

func GetString() []*vector.Vector {
	vec := make([]*vector.Vector, 2)
	str1 := []string{"-1", "-1", "-1", "-1", "0", "0", "0", "0", "1", "1", "1", "1", "", "", "", ""}
	str2 := []string{"-1", "0", "1", "", "-1", "0", "1", "", "-1", "0", "1", "", "-1", "0", "1", ""}
	offsets1 := []uint32{0, 2, 4, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 16, 16, 16}
	lengths1 := []uint32{2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0}
	offsets2 := []uint32{0, 2, 3, 4, 4, 6, 7, 8, 8, 10, 11, 12, 13, 14, 15, 16, 16}
	lengths2 := []uint32{2, 1, 1, 0, 2, 1, 1, 0, 2, 1, 1, 0, 2, 1, 1, 0}
	var str3, str4 string
	for i := 0; i < len(str1); i++ {
		str3 = str3 + str1[i]
		str4 = str4 + str2[i]
	}
	vec[0] = &vector.Vector{
		Col: &types.Bytes{
			Data:    []byte(str3),
			Offsets: offsets1,
			Lengths: lengths1,
		},
		Nsp: &nulls.Nulls{},
	}
	vec[0].Typ.Oid = types.T_varchar
	nulls.Add(vec[0].Nsp, 12)
	nulls.Add(vec[0].Nsp, 13)
	nulls.Add(vec[0].Nsp, 14)
	nulls.Add(vec[0].Nsp, 15)

	vec[1] = &vector.Vector{
		Col: &types.Bytes{
			Data:    []byte(str4),
			Offsets: offsets2,
			Lengths: lengths2,
		},
		Nsp: &nulls.Nulls{},
	}
	vec[1].Typ.Oid = types.T_varchar
	nulls.Add(vec[1].Nsp, 3)
	nulls.Add(vec[1].Nsp, 7)
	nulls.Add(vec[1].Nsp, 11)
	nulls.Add(vec[1].Nsp, 15)
	return vec
}

type testFunc = func() []*vector.Vector

var testFuncVec = []testFunc{
	GetInt8, GetInt16, GetInt32, GetInt64, GetUint8, GetUint16, GetUint32, GetUint64,
	GetFloat32, GetFloat64, GetDate, GetDatetime, GetDecimal64, GetBool, GetString,
}

type testEqFunc = func(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error)

var testEqFuncVec = []testEqFunc{
	EqDataValue[int8], EqDataValue[int16], EqDataValue[int32], EqDataValue[int64],
	EqDataValue[uint8], EqDataValue[uint16], EqDataValue[uint32], EqDataValue[uint64],
	EqDataValue[float32], EqDataValue[float64], EqDataValue[types.Date],
	EqDataValue[types.Datetime], EqDataValue[types.Decimal64], EqDataValue[bool], EqDataValue[string],
}

var EqretVec = [][]bool{
	EqintBool, EqintBool, EqintBool, EqintBool, EquintBool, EquintBool, EquintBool, EquintBool,
	EqintBool, EqintBool, EqintBool, EqintBool, EqintBool, EqboolBool, EqstringBool,
}

var retNullPosVec = [][]int{
	intNullPos, intNullPos, intNullPos, intNullPos, uintNullPos, uintNullPos, uintNullPos, uintNullPos,
	intNullPos, intNullPos, intNullPos, intNullPos, intNullPos, uintNullPos, intNullPos,
}

var retNotNullPosVec = [][]int{
	intNotNullPos, intNotNullPos, intNotNullPos, intNotNullPos, uintNotNullPos, uintNotNullPos, uintNotNullPos, uintNotNullPos,
	intNotNullPos, intNotNullPos, intNotNullPos, intNotNullPos, intNotNullPos, uintNotNullPos, intNotNullPos,
}

func Test_ColEqCol(t *testing.T) {
	convey.Convey("Test col eq col operator succ", t, func() {
		InitFuncMap()
		proc := process.New(mheap.New(&guest.Mmu{Mmu: host.New(1000), Limit: 1000}))

		for i := 0; i < 15; i++ {
			vec := testFuncVec[i]()
			ret, err := testEqFuncVec[i](vec, proc)
			if err != nil {
				log.Fatal(err)
			}
			data, ok := ret.Col.([]bool)
			if !ok {
				log.Fatal(errors.New("the return vec type is not right"))
			}
			for j := 0; j < len(data); j++ {
				convey.So(data[j], convey.ShouldEqual, EqretVec[i][j])
			}
			for j := 0; j < len(retNullPosVec[i]); j++ {
				convey.So(nulls.Contains(ret.Nsp, uint64(retNullPosVec[i][j])), convey.ShouldEqual, true)
			}
			for j := 0; j < len(retNotNullPosVec[i]); j++ {
				convey.So(nulls.Contains(ret.Nsp, uint64(retNotNullPosVec[i][j])), convey.ShouldEqual, false)
			}
		}

	})
}
