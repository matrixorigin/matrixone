// Copyright 2022 Matrix Origin
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

package operator

import (
	"testing"
	"time"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func TestCastStringToJson(t *testing.T) {
	makeTempVectors := func(src string, srcIsConst bool, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeStringVector(src, types.T_varchar, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}
	type caseStruct struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantScalar bool
	}
	makeCase := func(name string, src string, srcIsConst bool, procs *process.Process, destType types.T, wantScalar bool) caseStruct {
		return caseStruct{
			name:       name,
			vecs:       makeTempVectors(src, srcIsConst, destType),
			proc:       procs,
			wantValues: src,
			wantScalar: wantScalar,
		}
	}
	procs := testutil.NewProc()
	cases := []caseStruct{
		makeCase("Test01", `{"a":1,"b":2}`, true, procs, types.T_json, true),
		makeCase("Test02", `{"a":1,"b":2}`, false, procs, types.T_json, false),
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.JSONEq(t, c.wantValues.(string), types.DecodeJson(castRes.GetBytes(0)).String())
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}
}

func TestCastSameType(t *testing.T) {
	makeTempVectors := func(src interface{}, destType types.T, srcIsConst bool) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeVector(src, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors(int8(-23), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{-23},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors(int16(-23), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{-23},
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors(int32(-23), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{-23},
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors(int64(-23), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{-23},
			wantScalar: true,
		},
		{
			name:       "Test05",
			vecs:       makeTempVectors(uint8(23), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{23},
			wantScalar: true,
		},
		{
			name:       "Test06",
			vecs:       makeTempVectors(uint16(23), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{23},
			wantScalar: true,
		},
		{
			name:       "Test07",
			vecs:       makeTempVectors(uint32(23), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{23},
			wantScalar: true,
		},
		{
			name:       "Test08",
			vecs:       makeTempVectors(uint64(23), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{23},
			wantScalar: true,
		},
		{
			name:       "Test09",
			vecs:       makeTempVectors(float32(23.5), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{23.5},
			wantScalar: true,
		},
		{
			name:       "Test10",
			vecs:       makeTempVectors(float64(23.5), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{23.5},
			wantScalar: true,
		},
		{
			name:       "Test11",
			vecs:       makeTempVectors(int8(-23), types.T_int8, false),
			proc:       procs,
			wantValues: []int8{-23},
			wantScalar: false,
		},
		{
			name:       "Test12",
			vecs:       makeTempVectors(int16(-23), types.T_int16, false),
			proc:       procs,
			wantValues: []int16{-23},
			wantScalar: false,
		},
		{
			name:       "Test13",
			vecs:       makeTempVectors(int32(-23), types.T_int32, false),
			proc:       procs,
			wantValues: []int32{-23},
			wantScalar: false,
		},
		{
			name:       "Test14",
			vecs:       makeTempVectors(int64(-23), types.T_int64, false),
			proc:       procs,
			wantValues: []int64{-23},
			wantScalar: false,
		},
		{
			name:       "Test15",
			vecs:       makeTempVectors(uint8(23), types.T_uint8, false),
			proc:       procs,
			wantValues: []uint8{23},
			wantScalar: false,
		},
		{
			name:       "Test16",
			vecs:       makeTempVectors(uint16(23), types.T_uint16, false),
			proc:       procs,
			wantValues: []uint16{23},
			wantScalar: false,
		},
		{
			name:       "Test17",
			vecs:       makeTempVectors(uint32(23), types.T_uint32, false),
			proc:       procs,
			wantValues: []uint32{23},
			wantScalar: false,
		},
		{
			name:       "Test18",
			vecs:       makeTempVectors(uint64(23), types.T_uint64, false),
			proc:       procs,
			wantValues: []uint64{23},
			wantScalar: false,
		},
		{
			name:       "Test19",
			vecs:       makeTempVectors(float32(23.5), types.T_float32, false),
			proc:       procs,
			wantValues: []float32{23.5},
			wantScalar: false,
		},
		{
			name:       "Test20",
			vecs:       makeTempVectors(float64(23.5), types.T_float64, false),
			proc:       procs,
			wantValues: []float64{23.5},
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantValues, castRes.Col)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}
}

func TestCastSameType2(t *testing.T) {
	makeTempVectors := func(src interface{}, destType types.T, srcIsConst bool) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeVector(src, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	//types.Date | types.Time | types.Datetime | types.Timestamp
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors(types.Date(729848), types.T_date, true),
			proc:       procs,
			wantValues: []types.Date{729848},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors(types.Datetime(66122056321728512), types.T_datetime, true),
			proc:       procs,
			wantValues: []types.Datetime{66122056321728512},
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors(types.Timestamp(66122026122739712), types.T_timestamp, true),
			proc:       procs,
			wantValues: []types.Timestamp{66122026122739712},
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors(types.Date(729848), types.T_date, false),
			proc:       procs,
			wantValues: []types.Date{729848},
			wantScalar: false,
		},
		{
			name:       "Test05",
			vecs:       makeTempVectors(types.Datetime(66122056321728512), types.T_datetime, false),
			proc:       procs,
			wantValues: []types.Datetime{66122056321728512},
			wantScalar: false,
		},
		{
			name:       "Test06",
			vecs:       makeTempVectors(types.Timestamp(66122026122739712), types.T_timestamp, false),
			proc:       procs,
			wantValues: []types.Timestamp{66122026122739712},
			wantScalar: false,
		},
		{
			name:       "Test07",
			vecs:       makeTempVectors(types.Time(661220261227), types.T_time, false),
			proc:       procs,
			wantValues: []types.Time{661220261227},
			wantScalar: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantValues, castRes.Col)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}
}

func TestCastLeftToRight(t *testing.T) {
	// int8 -> (int16/int32/int64/uint8/uint16/uint32/uint64/float32/float64)
	// int16 -> (int8/int32/int64/uint8/uint16/uint32/uint64/float32/float64)
	// int32 -> (int8/int16/int64/uint8/uint16/uint32/uint64/float32/float64)
	// int64 -> (int8/int16/int32/uint8/uint16/uint32/uint64/float32/float64)
	// uint8 -> (int8/int16/int32/int64/uint16/uint32/uint64/float32/float64)
	// uint16 -> (int8/int16/int32/int64/uint8/uint32/uint64/float32/float64)
	// uint32 -> (int8/int16/int32/int64/uint8/uint16/uint64/float32/float64)
	// uint64 -> (int8/int16/int32/int64/uint8/uint16/uint32/float32/float64)
	// float32 -> (int8/int16/int32/int64/uint8/uint16/uint32/uint64/float64)
	// float64 -> (int8/int16/int32/int64/uint8/uint16/uint32/uint64/float32)

	makeTempVectors := func(src interface{}, destType types.T, srcIsConst bool) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeVector(src, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors(int8(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors(int8(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors(int8(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors(int8(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test05",
			vecs:       makeTempVectors(int8(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test06",
			vecs:       makeTempVectors(int8(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test07",
			vecs:       makeTempVectors(int8(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test08",
			vecs:       makeTempVectors(int8(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test09",
			vecs:       makeTempVectors(int8(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test10",
			vecs:       makeTempVectors(int8(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test11",
			vecs:       makeTempVectors(int16(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test12",
			vecs:       makeTempVectors(int16(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test13",
			vecs:       makeTempVectors(int16(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test14",
			vecs:       makeTempVectors(int16(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test15",
			vecs:       makeTempVectors(int16(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test16",
			vecs:       makeTempVectors(int16(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test17",
			vecs:       makeTempVectors(int16(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test18",
			vecs:       makeTempVectors(int16(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test19",
			vecs:       makeTempVectors(int16(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test20",
			vecs:       makeTempVectors(int16(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test21",
			vecs:       makeTempVectors(int32(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test22",
			vecs:       makeTempVectors(int32(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test23",
			vecs:       makeTempVectors(int32(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test24",
			vecs:       makeTempVectors(int32(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test25",
			vecs:       makeTempVectors(int32(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test26",
			vecs:       makeTempVectors(int32(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test27",
			vecs:       makeTempVectors(int32(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test28",
			vecs:       makeTempVectors(int32(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test29",
			vecs:       makeTempVectors(int32(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test30",
			vecs:       makeTempVectors(int32(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test31",
			vecs:       makeTempVectors(int64(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test32",
			vecs:       makeTempVectors(int64(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test33",
			vecs:       makeTempVectors(int64(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test34",
			vecs:       makeTempVectors(int64(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test35",
			vecs:       makeTempVectors(int64(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test36",
			vecs:       makeTempVectors(int64(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test37",
			vecs:       makeTempVectors(int64(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test38",
			vecs:       makeTempVectors(int64(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test39",
			vecs:       makeTempVectors(int64(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test40",
			vecs:       makeTempVectors(int64(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test41",
			vecs:       makeTempVectors(uint8(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test42",
			vecs:       makeTempVectors(uint8(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test43",
			vecs:       makeTempVectors(uint8(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test44",
			vecs:       makeTempVectors(uint8(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test45",
			vecs:       makeTempVectors(uint8(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test46",
			vecs:       makeTempVectors(uint8(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test47",
			vecs:       makeTempVectors(uint8(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test48",
			vecs:       makeTempVectors(uint8(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test49",
			vecs:       makeTempVectors(uint8(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test50",
			vecs:       makeTempVectors(uint8(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test51",
			vecs:       makeTempVectors(uint16(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test52",
			vecs:       makeTempVectors(uint16(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test53",
			vecs:       makeTempVectors(uint16(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test54",
			vecs:       makeTempVectors(uint16(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test55",
			vecs:       makeTempVectors(uint16(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test56",
			vecs:       makeTempVectors(uint16(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test57",
			vecs:       makeTempVectors(uint16(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test58",
			vecs:       makeTempVectors(uint16(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test59",
			vecs:       makeTempVectors(uint16(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test60",
			vecs:       makeTempVectors(uint16(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test61",
			vecs:       makeTempVectors(uint32(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test62",
			vecs:       makeTempVectors(uint32(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test63",
			vecs:       makeTempVectors(uint32(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test64",
			vecs:       makeTempVectors(uint32(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test65",
			vecs:       makeTempVectors(uint32(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test66",
			vecs:       makeTempVectors(uint32(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test67",
			vecs:       makeTempVectors(uint32(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test68",
			vecs:       makeTempVectors(uint32(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test69",
			vecs:       makeTempVectors(uint32(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test70",
			vecs:       makeTempVectors(uint32(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test71",
			vecs:       makeTempVectors(uint64(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test72",
			vecs:       makeTempVectors(uint64(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test73",
			vecs:       makeTempVectors(uint64(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test74",
			vecs:       makeTempVectors(uint64(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test75",
			vecs:       makeTempVectors(uint64(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test76",
			vecs:       makeTempVectors(uint64(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test77",
			vecs:       makeTempVectors(uint64(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test78",
			vecs:       makeTempVectors(uint64(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test79",
			vecs:       makeTempVectors(uint64(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test80",
			vecs:       makeTempVectors(uint64(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test81",
			vecs:       makeTempVectors(float32(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test82",
			vecs:       makeTempVectors(float32(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test83",
			vecs:       makeTempVectors(float32(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test84",
			vecs:       makeTempVectors(float32(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test85",
			vecs:       makeTempVectors(float32(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test86",
			vecs:       makeTempVectors(float32(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test87",
			vecs:       makeTempVectors(float32(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test88",
			vecs:       makeTempVectors(float32(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test89",
			vecs:       makeTempVectors(float32(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test90",
			vecs:       makeTempVectors(float32(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test91",
			vecs:       makeTempVectors(float64(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test92",
			vecs:       makeTempVectors(float64(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test93",
			vecs:       makeTempVectors(float64(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test94",
			vecs:       makeTempVectors(float64(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test95",
			vecs:       makeTempVectors(float64(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test96",
			vecs:       makeTempVectors(float64(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test97",
			vecs:       makeTempVectors(float64(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test98",
			vecs:       makeTempVectors(float64(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test99",
			vecs:       makeTempVectors(float64(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test100",
			vecs:       makeTempVectors(float64(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}

			require.Equal(t, c.wantValues, castRes.Col)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}
}

func TestCastSpecials1Int(t *testing.T) {
	// (char / varhcar / blob / text) -> (int8 / int16 / int32/ int64 / uint8 / uint16 / uint32 / uint64)

	makeTempVectors := func(src string, srcType types.T, srcIsConst bool, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeStringVector(src, srcType, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors("15", types.T_varchar, true, types.T_int8),
			proc:       procs,
			wantValues: []int8{15},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors("15", types.T_varchar, true, types.T_int16),
			proc:       procs,
			wantValues: []int16{15},
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors("15", types.T_varchar, true, types.T_int32),
			proc:       procs,
			wantValues: []int32{15},
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors("15", types.T_varchar, true, types.T_int64),
			proc:       procs,
			wantValues: []int64{15},
			wantScalar: true,
		},
		{
			name:       "Test05",
			vecs:       makeTempVectors("15", types.T_varchar, true, types.T_uint8),
			proc:       procs,
			wantValues: []uint8{15},
			wantScalar: true,
		},
		{
			name:       "Test06",
			vecs:       makeTempVectors("15", types.T_varchar, true, types.T_uint16),
			proc:       procs,
			wantValues: []uint16{15},
			wantScalar: true,
		},
		{
			name:       "Test07",
			vecs:       makeTempVectors("15", types.T_varchar, true, types.T_uint32),
			proc:       procs,
			wantValues: []uint32{15},
			wantScalar: true,
		},
		{
			name:       "Test08",
			vecs:       makeTempVectors("15", types.T_varchar, true, types.T_uint64),
			proc:       procs,
			wantValues: []uint64{15},
			wantScalar: true,
		},
		{
			name:       "Test09",
			vecs:       makeTempVectors("15", types.T_char, true, types.T_int8),
			proc:       procs,
			wantValues: []int8{15},
			wantScalar: true,
		},
		{
			name:       "Test10",
			vecs:       makeTempVectors("15", types.T_char, true, types.T_int16),
			proc:       procs,
			wantValues: []int16{15},
			wantScalar: true,
		},
		{
			name:       "Test11",
			vecs:       makeTempVectors("15", types.T_char, true, types.T_int32),
			proc:       procs,
			wantValues: []int32{15},
			wantScalar: true,
		},
		{
			name:       "Test12",
			vecs:       makeTempVectors("15", types.T_char, true, types.T_int64),
			proc:       procs,
			wantValues: []int64{15},
			wantScalar: true,
		},
		{
			name:       "Test13",
			vecs:       makeTempVectors("15", types.T_char, true, types.T_uint8),
			proc:       procs,
			wantValues: []uint8{15},
			wantScalar: true,
		},
		{
			name:       "Test14",
			vecs:       makeTempVectors("15", types.T_char, true, types.T_uint16),
			proc:       procs,
			wantValues: []uint16{15},
			wantScalar: true,
		},
		{
			name:       "Test15",
			vecs:       makeTempVectors("15", types.T_char, true, types.T_uint32),
			proc:       procs,
			wantValues: []uint32{15},
			wantScalar: true,
		},
		{
			name:       "Test16",
			vecs:       makeTempVectors("15", types.T_char, true, types.T_uint64),
			proc:       procs,
			wantValues: []uint64{15},
			wantScalar: true,
		},
		{
			name:       "Test17",
			vecs:       makeTempVectors("15", types.T_varchar, false, types.T_int8),
			proc:       procs,
			wantValues: []int8{15},
			wantScalar: false,
		},
		{
			name:       "Test18",
			vecs:       makeTempVectors("15", types.T_varchar, false, types.T_int16),
			proc:       procs,
			wantValues: []int16{15},
			wantScalar: false,
		},
		{
			name:       "Test19",
			vecs:       makeTempVectors("15", types.T_varchar, false, types.T_int32),
			proc:       procs,
			wantValues: []int32{15},
			wantScalar: false,
		},
		{
			name:       "Test20",
			vecs:       makeTempVectors("15", types.T_varchar, false, types.T_int64),
			proc:       procs,
			wantValues: []int64{15},
			wantScalar: false,
		},
		{
			name:       "Test21",
			vecs:       makeTempVectors("15", types.T_varchar, false, types.T_uint8),
			proc:       procs,
			wantValues: []uint8{15},
			wantScalar: false,
		},
		{
			name:       "Test22",
			vecs:       makeTempVectors("15", types.T_varchar, false, types.T_uint16),
			proc:       procs,
			wantValues: []uint16{15},
			wantScalar: false,
		},
		{
			name:       "Test23",
			vecs:       makeTempVectors("15", types.T_varchar, false, types.T_uint32),
			proc:       procs,
			wantValues: []uint32{15},
			wantScalar: false,
		},
		{
			name:       "Test24",
			vecs:       makeTempVectors("15", types.T_varchar, false, types.T_uint64),
			proc:       procs,
			wantValues: []uint64{15},
			wantScalar: false,
		},
		{
			name:       "Test25",
			vecs:       makeTempVectors("15", types.T_char, false, types.T_int8),
			proc:       procs,
			wantValues: []int8{15},
			wantScalar: false,
		},
		{
			name:       "Test26",
			vecs:       makeTempVectors("15", types.T_char, false, types.T_int16),
			proc:       procs,
			wantValues: []int16{15},
			wantScalar: false,
		},
		{
			name:       "Test27",
			vecs:       makeTempVectors("15", types.T_char, false, types.T_int32),
			proc:       procs,
			wantValues: []int32{15},
			wantScalar: false,
		},
		{
			name:       "Test28",
			vecs:       makeTempVectors("15", types.T_char, false, types.T_int64),
			proc:       procs,
			wantValues: []int64{15},
			wantScalar: false,
		},
		{
			name:       "Test29",
			vecs:       makeTempVectors("15", types.T_char, false, types.T_uint8),
			proc:       procs,
			wantValues: []uint8{15},
			wantScalar: false,
		},
		{
			name:       "Test30",
			vecs:       makeTempVectors("15", types.T_char, false, types.T_uint16),
			proc:       procs,
			wantValues: []uint16{15},
			wantScalar: false,
		},
		{
			name:       "Test31",
			vecs:       makeTempVectors("15", types.T_char, false, types.T_uint32),
			proc:       procs,
			wantValues: []uint32{15},
			wantScalar: false,
		},
		{
			name:       "Test32",
			vecs:       makeTempVectors("15", types.T_char, false, types.T_uint64),
			proc:       procs,
			wantValues: []uint64{15},
			wantScalar: false,
		},
		{
			name:       "Test33",
			vecs:       makeTempVectors("15", types.T_blob, false, types.T_int8),
			proc:       procs,
			wantValues: []int8{15},
			wantScalar: false,
		},
		{
			name:       "Test34",
			vecs:       makeTempVectors("15", types.T_blob, false, types.T_int16),
			proc:       procs,
			wantValues: []int16{15},
			wantScalar: false,
		},
		{
			name:       "Test35",
			vecs:       makeTempVectors("15", types.T_blob, false, types.T_int32),
			proc:       procs,
			wantValues: []int32{15},
			wantScalar: false,
		},
		{
			name:       "Test36",
			vecs:       makeTempVectors("15", types.T_blob, false, types.T_int64),
			proc:       procs,
			wantValues: []int64{15},
			wantScalar: false,
		},
		{
			name:       "Test37",
			vecs:       makeTempVectors("15", types.T_blob, false, types.T_uint8),
			proc:       procs,
			wantValues: []uint8{15},
			wantScalar: false,
		},
		{
			name:       "Test38",
			vecs:       makeTempVectors("15", types.T_blob, false, types.T_uint16),
			proc:       procs,
			wantValues: []uint16{15},
			wantScalar: false,
		},
		{
			name:       "Test39",
			vecs:       makeTempVectors("15", types.T_blob, false, types.T_uint32),
			proc:       procs,
			wantValues: []uint32{15},
			wantScalar: false,
		},
		{
			name:       "Test40",
			vecs:       makeTempVectors("15", types.T_blob, false, types.T_uint64),
			proc:       procs,
			wantValues: []uint64{15},
			wantScalar: false,
		},

		{
			name:       "Test41",
			vecs:       makeTempVectors("15", types.T_text, false, types.T_int8),
			proc:       procs,
			wantValues: []int8{15},
			wantScalar: false,
		},
		{
			name:       "Test42",
			vecs:       makeTempVectors("15", types.T_text, false, types.T_int16),
			proc:       procs,
			wantValues: []int16{15},
			wantScalar: false,
		},
		{
			name:       "Test43",
			vecs:       makeTempVectors("15", types.T_text, false, types.T_int32),
			proc:       procs,
			wantValues: []int32{15},
			wantScalar: false,
		},
		{
			name:       "Test44",
			vecs:       makeTempVectors("15", types.T_text, false, types.T_int64),
			proc:       procs,
			wantValues: []int64{15},
			wantScalar: false,
		},
		{
			name:       "Test45",
			vecs:       makeTempVectors("15", types.T_text, false, types.T_uint8),
			proc:       procs,
			wantValues: []uint8{15},
			wantScalar: false,
		},
		{
			name:       "Test46",
			vecs:       makeTempVectors("15", types.T_text, false, types.T_uint16),
			proc:       procs,
			wantValues: []uint16{15},
			wantScalar: false,
		},
		{
			name:       "Test47",
			vecs:       makeTempVectors("15", types.T_text, false, types.T_uint32),
			proc:       procs,
			wantValues: []uint32{15},
			wantScalar: false,
		},
		{
			name:       "Test48",
			vecs:       makeTempVectors("15", types.T_text, false, types.T_uint64),
			proc:       procs,
			wantValues: []uint64{15},
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantValues, castRes.Col)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}

}

func TestCastSpecials1Float(t *testing.T) {
	// (char / varhcar / blob) -> (float32 / float64)
	makeTempVectors := func(src string, srcType types.T, srcIsConst bool, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeStringVector(src, srcType, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors("15.23", types.T_varchar, true, types.T_float32),
			proc:       procs,
			wantValues: []float32{15.23},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors("15.23", types.T_varchar, true, types.T_float64),
			proc:       procs,
			wantValues: []float64{15.23},
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors("15.23", types.T_char, true, types.T_float32),
			proc:       procs,
			wantValues: []float32{15.23},
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors("15.23", types.T_char, true, types.T_float64),
			proc:       procs,
			wantValues: []float64{15.23},
			wantScalar: true,
		},
		{
			name:       "Test05",
			vecs:       makeTempVectors("15.23", types.T_varchar, false, types.T_float32),
			proc:       procs,
			wantValues: []float32{15.23},
			wantScalar: false,
		},
		{
			name:       "Test06",
			vecs:       makeTempVectors("15.23", types.T_varchar, false, types.T_float64),
			proc:       procs,
			wantValues: []float64{15.23},
			wantScalar: false,
		},
		{
			name:       "Test07",
			vecs:       makeTempVectors("15.23", types.T_char, false, types.T_float32),
			proc:       procs,
			wantValues: []float32{15.23},
			wantScalar: false,
		},
		{
			name:       "Test08",
			vecs:       makeTempVectors("15.23", types.T_char, false, types.T_float64),
			proc:       procs,
			wantValues: []float64{15.23},
			wantScalar: false,
		},
		{
			name:       "Test09",
			vecs:       makeTempVectors("15.23", types.T_blob, false, types.T_float32),
			proc:       procs,
			wantValues: []float32{15.23},
			wantScalar: false,
		},
		{
			name:       "Test10",
			vecs:       makeTempVectors("15.23", types.T_blob, true, types.T_float64),
			proc:       procs,
			wantValues: []float64{15.23},
			wantScalar: true,
		},
		{
			name:       "Test11",
			vecs:       makeTempVectors("15.23", types.T_blob, false, types.T_float32),
			proc:       procs,
			wantValues: []float32{15.23},
			wantScalar: false,
		},
		{
			name:       "Test12",
			vecs:       makeTempVectors("15.23", types.T_blob, true, types.T_float64),
			proc:       procs,
			wantValues: []float64{15.23},
			wantScalar: true,
		},
		{
			name:       "Test13",
			vecs:       makeTempVectors("15.23", types.T_text, false, types.T_float32),
			proc:       procs,
			wantValues: []float32{15.23},
			wantScalar: false,
		},
		{
			name:       "Test14",
			vecs:       makeTempVectors("15.23", types.T_text, true, types.T_float64),
			proc:       procs,
			wantValues: []float64{15.23},
			wantScalar: true,
		},
		{
			name:       "Test15",
			vecs:       makeTempVectors("15.23", types.T_text, false, types.T_float32),
			proc:       procs,
			wantValues: []float32{15.23},
			wantScalar: false,
		},
		{
			name:       "Test16",
			vecs:       makeTempVectors("15.23", types.T_text, true, types.T_float64),
			proc:       procs,
			wantValues: []float64{15.23},
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantValues, castRes.Col)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}

}

func TestCastSpecials2Float(t *testing.T) {
	//(float32/float64) -> (char / varhcar / blob)
	makeTempVectors := func(src interface{}, destType types.T, srcIsConst bool) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeVector(src, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  []byte
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors(float32(23.65), types.T_char, true),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors(float64(23.65), types.T_char, true),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors(float32(23.65), types.T_varchar, true),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors(float64(23.65), types.T_varchar, true),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: true,
		},
		{
			name:       "Test05",
			vecs:       makeTempVectors(float32(23.65), types.T_char, false),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: false,
		},
		{
			name:       "Test06",
			vecs:       makeTempVectors(float64(23.65), types.T_char, false),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: false,
		},
		{
			name:       "Test07",
			vecs:       makeTempVectors(float32(23.65), types.T_varchar, false),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: false,
		},
		{
			name:       "Test08",
			vecs:       makeTempVectors(float64(23.65), types.T_varchar, false),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: false,
		},
		{
			name:       "Test09",
			vecs:       makeTempVectors(float32(23.65), types.T_blob, false),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: false,
		},
		{
			name:       "Test10",
			vecs:       makeTempVectors(float64(23.65), types.T_blob, false),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: false,
		},
		{
			name:       "Test11",
			vecs:       makeTempVectors(float32(23.65), types.T_blob, true),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: true,
		},
		{
			name:       "Test12",
			vecs:       makeTempVectors(float64(23.65), types.T_blob, true),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: true,
		},
		{
			name:       "Test13",
			vecs:       makeTempVectors(float32(23.65), types.T_text, false),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: false,
		},
		{
			name:       "Test14",
			vecs:       makeTempVectors(float64(23.65), types.T_text, false),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: false,
		},
		{
			name:       "Test15",
			vecs:       makeTempVectors(float32(23.65), types.T_text, true),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: true,
		},
		{
			name:       "Test16",
			vecs:       makeTempVectors(float64(23.65), types.T_text, true),
			proc:       procs,
			wantBytes:  []byte("23.65"),
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, castRes.GetBytes(0))
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}
}

func TestCastSpecials3(t *testing.T) {
	// char -> char
	// char -> varhcar
	// varhcar -> char
	// varhcar -> varhcar
	// blob -> blob
	// blob -> varchar
	// blob -> char
	// varchar -> blob
	// char -> blob
	makeTempVectors := func(src string, srcType types.T, destType types.T, srcIsConst bool) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeStringVector(src, srcType, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  []byte
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors("abcsedn", types.T_char, types.T_char, true),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors("abcsedn", types.T_char, types.T_varchar, true),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors("abcsedn", types.T_varchar, types.T_char, true),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors("abcsedn", types.T_varchar, types.T_varchar, true),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: true,
		},
		{
			name:       "Test05",
			vecs:       makeTempVectors("abcsedn", types.T_char, types.T_char, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test06",
			vecs:       makeTempVectors("abcsedn", types.T_char, types.T_varchar, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test07",
			vecs:       makeTempVectors("abcsedn", types.T_varchar, types.T_char, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test08",
			vecs:       makeTempVectors("abcsedn", types.T_varchar, types.T_varchar, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test09",
			vecs:       makeTempVectors("abcsedn", types.T_blob, types.T_blob, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test10",
			vecs:       makeTempVectors("abcsedn", types.T_blob, types.T_varchar, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test11",
			vecs:       makeTempVectors("abcsedn", types.T_blob, types.T_char, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test12",
			vecs:       makeTempVectors("abcsedn", types.T_varchar, types.T_blob, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test13",
			vecs:       makeTempVectors("abcsedn", types.T_char, types.T_blob, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test14",
			vecs:       makeTempVectors("abcsedn", types.T_text, types.T_text, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test15",
			vecs:       makeTempVectors("abcsedn", types.T_text, types.T_varchar, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test16",
			vecs:       makeTempVectors("abcsedn", types.T_text, types.T_char, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test17",
			vecs:       makeTempVectors("abcsedn", types.T_varchar, types.T_text, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
		{
			name:       "Test18",
			vecs:       makeTempVectors("abcsedn", types.T_char, types.T_text, false),
			proc:       procs,
			wantBytes:  []byte("abcsedn"),
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, castRes.GetBytes(0))
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}

}

func TestCastSpecial4(t *testing.T) {
	//(int8/int16/int32/int64) to decimal128
	// (uint8/uint16/uint32/uint64) to decimal128

	makeTempVectors := func(src interface{}, destType types.T, srcIsConst bool) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeVector(src, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		vectors[1].Typ.Width = 64
		vectors[1].Typ.Scale = 0
		return vectors
	}
	resType := types.T_decimal128.ToType()
	decimal128 := types.Decimal128FromInt32(123)
	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantType   types.Type
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors(int8(123), types.T_decimal128, true),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors(int16(123), types.T_decimal128, true),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors(int32(123), types.T_decimal128, true),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors(int64(123), types.T_decimal128, true),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: true,
		},
		{
			name:       "Test05",
			vecs:       makeTempVectors(uint8(123), types.T_decimal128, true),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: true,
		},
		{
			name:       "Test06",
			vecs:       makeTempVectors(uint16(123), types.T_decimal128, true),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: true,
		},
		{
			name:       "Test07",
			vecs:       makeTempVectors(uint32(123), types.T_decimal128, true),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: true,
		},
		{
			name:       "Test08",
			vecs:       makeTempVectors(uint64(123), types.T_decimal128, true),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: true,
		},
		{
			name:       "Test09",
			vecs:       makeTempVectors(int8(123), types.T_decimal128, false),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: false,
		},
		{
			name:       "Test10",
			vecs:       makeTempVectors(int16(123), types.T_decimal128, false),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: false,
		},
		{
			name:       "Test11",
			vecs:       makeTempVectors(int32(123), types.T_decimal128, false),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: false,
		},
		{
			name:       "Test12",
			vecs:       makeTempVectors(int64(123), types.T_decimal128, false),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: false,
		},
		{
			name:       "Test13",
			vecs:       makeTempVectors(uint8(123), types.T_decimal128, false),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: false,
		},
		{
			name:       "Test14",
			vecs:       makeTempVectors(uint16(123), types.T_decimal128, false),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: false,
		},
		{
			name:       "Test15",
			vecs:       makeTempVectors(uint32(123), types.T_decimal128, false),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: false,
		},
		{
			name:       "Test16",
			vecs:       makeTempVectors(uint64(123), types.T_decimal128, false),
			proc:       procs,
			wantValues: []types.Decimal128{decimal128},
			wantType:   resType,
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			decimalres, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantValues, decimalres.Col)
			require.Equal(t, c.wantType, decimalres.Typ)
			require.Equal(t, c.wantScalar, decimalres.IsScalar())
		})
	}

}

func TestCastVarcharAsDate(t *testing.T) {
	//Cast converts varchar to date type
	convey.Convey("Cast varchar to date", t, func() {
		type kase struct {
			s    string
			want string
		}

		kases := []kase{
			{
				s:    "2004-04-03",
				want: "2004-04-03",
			},
			{
				s:    "2021-10-03",
				want: "2021-10-03",
			},
			{
				s:    "2020-08-23",
				want: "2020-08-23",
			},
			{
				s:    "2021-11-23",
				want: "2021-11-23",
			},
			{
				s:    "2014-09-23",
				want: "2014-09-23",
			},
		}

		var inStrs []string
		var wantStrs []string
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeVarcharVector(inStrs, nil)
		destVector := testutil.MakeDateVector(nil, nil)

		wantVec := testutil.MakeDateVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Cast scalar varchar to date", t, func() {
		type kase struct {
			s    string
			want string
		}

		k := kase{
			s:    "2014-09-23",
			want: "2014-09-23",
		}

		srcVector := testutil.MakeScalarVarchar(k.s, 10)
		destVector := testutil.MakeDateVector(nil, nil)
		wantVec := testutil.MakeScalarDate(k.want, 10)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	//Cast converts varchar to datetime type
	convey.Convey("Cast varchar to datetime", t, func() {
		type kase struct {
			s    string
			want string
		}

		kases := []kase{
			{
				s:    "2004-04-03 12:14:35",
				want: "2004-04-03 12:14:35",
			},
			{
				s:    "2021-10-03 11:52:21",
				want: "2021-10-03 11:52:21",
			},
			{
				s:    "2020-08-23 11:52:21",
				want: "2020-08-23 11:52:21",
			},
			{
				s:    "2021-11-23 16:12:21",
				want: "2021-11-23 16:12:21",
			},
			{
				s:    "2014-09-23 16:17:21",
				want: "2014-09-23 16:17:21",
			},
		}

		var inStrs []string
		var wantStrs []string
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeVarcharVector(inStrs, nil)
		destVector := testutil.MakeDateTimeVector(nil, nil)

		wantVec := testutil.MakeDateTimeVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Cast scalar varchar to datetime", t, func() {
		type kase struct {
			s    string
			want string
		}

		k := kase{
			s:    "2004-04-03 12:14:35",
			want: "2004-04-03 12:14:35",
		}

		srcVector := testutil.MakeScalarVarchar(k.s, 10)
		destVector := testutil.MakeDateTimeVector(nil, nil)
		wantVec := testutil.MakeScalarDateTime(k.want, 10)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	//Cast converts varchar to timestamp type
	convey.Convey("Cast varchar to timestamp", t, func() {
		type kase struct {
			s    string
			want string
		}

		kases := []kase{
			{
				s:    "2004-04-03 12:14:35",
				want: "2004-04-03 12:14:35",
			},
			{
				s:    "2021-10-03 11:52:21",
				want: "2021-10-03 11:52:21",
			},
			{
				s:    "2020-08-23 11:52:21",
				want: "2020-08-23 11:52:21",
			},
			{
				s:    "2021-11-23 16:12:21",
				want: "2021-11-23 16:12:21",
			},
			{
				s:    "2014-09-23 16:17:21",
				want: "2014-09-23 16:17:21",
			},
		}

		var inStrs []string
		var wantStrs []string
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeVarcharVector(inStrs, nil)
		destVector := testutil.MakeTimeStampVector(nil, nil)

		wantVec := testutil.MakeTimeStampVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Cast scalar varchar to timestamp", t, func() {
		type kase struct {
			s    string
			want string
		}

		k := kase{
			s:    "2021-11-23 16:12:21",
			want: "2021-11-23 16:12:21",
		}

		srcVector := testutil.MakeScalarVarchar(k.s, 10)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeScalarTimeStamp(k.want, 10)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

func TestCastFloatAsDecimal(t *testing.T) {
	makeTempVectors := func(leftVal []float32, leftType types.Type, rightType types.Type) []*vector.Vector {
		vecs := make([]*vector.Vector, 2)
		vecs[0] = vector.NewConstFixed(leftType, 1, leftVal[0], testutil.TestUtilMp)
		vecs[1] = &vector.Vector{
			Col: nil,
			Typ: rightType,
			Nsp: &nulls.Nulls{},
		}
		return vecs
	}
	leftType := types.Type{Oid: types.T_float32, Size: 8}
	rightType := types.Type{Oid: types.T_decimal64, Size: 8, Scale: 2, Width: 16}
	d, _ := types.Decimal64FromFloat64(123.0, 5, 1)

	cases := []struct {
		name      string
		vecs      []*vector.Vector
		proc      *process.Process
		wantBytes interface{}
	}{
		{
			name:      "TEST01",
			vecs:      makeTempVectors([]float32{123.0}, leftType, rightType),
			proc:      testutil.NewProc(),
			wantBytes: []types.Decimal64{d},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result, _ := Cast(c.vecs, c.proc)
			require.Equal(t, c.wantBytes, result.Col.([]types.Decimal64))
		})
	}
}

func TestCastDecimalAsString(t *testing.T) {
	makeTempVectors := func(leftVal []types.Decimal64, leftType types.Type, rightType types.Type) []*vector.Vector {
		vecs := make([]*vector.Vector, 2)
		vecs[0] = vector.NewConstFixed(leftType, 1, leftVal[0], testutil.TestUtilMp)
		vecs[1] = vector.New(rightType)
		return vecs
	}
	leftType := types.Type{Oid: types.T_decimal64, Size: 8}
	rightType := types.Type{Oid: types.T_varchar, Size: 24}

	cases := []struct {
		name      string
		vecs      []*vector.Vector
		proc      *process.Process
		wantBytes interface{}
	}{
		{
			name: "TEST01",
			vecs: makeTempVectors([]types.Decimal64{types.Decimal64FromInt32(1230)}, leftType, rightType),
			proc: testutil.NewProc(),
			wantBytes: [][]byte{
				{0x31, 0x32, 0x33, 0x30},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result, _ := Cast(c.vecs, c.proc)
			require.Equal(t, c.wantBytes, vector.GetBytesVectorValues(result))
		})
	}
}

func TestCastTimestampAsDate(t *testing.T) {
	makeTempVectors := func(leftVal []types.Timestamp, leftType types.Type, rightType types.Type) []*vector.Vector {
		vecs := make([]*vector.Vector, 2)
		vecs[0] = vector.NewConstFixed(leftType, 1, leftVal[0], testutil.TestUtilMp)
		vecs[1] = &vector.Vector{
			Col: nil,
			Typ: rightType,
			Nsp: &nulls.Nulls{},
		}
		return vecs
	}
	leftType := types.Type{Oid: types.T_timestamp, Size: 8}
	rightType := types.Type{Oid: types.T_date, Size: 4}

	cases := []struct {
		name      string
		vecs      []*vector.Vector
		proc      *process.Process
		wantBytes interface{}
	}{
		{
			name:      "TEST01",
			vecs:      makeTempVectors([]types.Timestamp{types.Timestamp(382331223)}, leftType, rightType),
			proc:      testutil.NewProc(),
			wantBytes: []types.Date{types.Date(0)},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result, _ := Cast(c.vecs, c.proc)
			require.Equal(t, c.wantBytes, result.Col.([]types.Date))
		})
	}
}

func TestCastDecimal64AsDecimal128(t *testing.T) {
	//Cast converts decimal64 to decimal128
	makeTempVector := func(left types.Decimal64, leftType types.Type, leftScalar bool, destType types.Type) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		if leftScalar {
			vectors[0] = vector.NewConstFixed(leftType, 1, left, testutil.TestUtilMp)
		} else {
			vectors[0] = vector.NewWithFixed(leftType, []types.Decimal64{left}, nil, testutil.TestUtilMp)
		}
		vectors[1] = vector.New(destType)
		return vectors
	}
	// decimal(10,5)
	leftType := types.Type{Oid: types.T_decimal64, Size: 8, Width: 10, Scale: 5}
	//decimal(20, 5)
	destType := types.Type{Oid: types.T_decimal128, Size: 16, Width: 20, Scale: 5}

	d64_33333300 := types.Decimal64FromInt32(333333000)
	d128_33333300 := types.Decimal128FromInt32(333333000)

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  interface{}
		wantType   types.T
		wantScalar bool
	}{
		{
			name:       "TEST01", //cast(333.33300 as decimal(20, 5))
			vecs:       makeTempVector(d64_33333300, leftType, true, destType),
			proc:       procs,
			wantBytes:  []types.Decimal128{d128_33333300},
			wantType:   types.T_decimal128,
			wantScalar: true,
		},
		{
			name:       "TEST01", //cast(333.33300 as decimal(20, 5))
			vecs:       makeTempVector(d64_33333300, leftType, false, destType),
			proc:       procs,
			wantBytes:  []types.Decimal128{d128_33333300},
			wantType:   types.T_decimal128,
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, castRes.Col)
			require.Equal(t, c.wantType, castRes.Typ.Oid)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}

}

func TestCastDecimal64AsDecimal64(t *testing.T) {
	//Cast converts decimal64 to decimal64
	makeTempVector := func(left types.Decimal64, leftType types.Type, leftScalar bool, destType types.Type) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		if leftScalar {
			vectors[0] = vector.NewConstFixed(leftType, 1, left, testutil.TestUtilMp)
		} else {
			vectors[0] = vector.NewWithFixed(leftType, []types.Decimal64{left}, nil, testutil.TestUtilMp)
		}

		vectors[1] = vector.New(destType)
		return vectors
	}
	// decimal(10,5)
	leftType := types.Type{Oid: types.T_decimal64, Size: 8, Width: 10, Scale: 5}
	//decimal(10, 4)
	destType := types.Type{Oid: types.T_decimal64, Size: 8, Width: 10, Scale: 4}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  interface{}
		wantType   types.Type
		wantScalar bool
	}{
		{
			name:       "TEST01", //cast(333.33300 as decimal(10, 4))
			vecs:       makeTempVector(types.Decimal64FromInt32(33333300), leftType, true, destType),
			proc:       procs,
			wantBytes:  []types.Decimal64{types.Decimal64FromInt32(33333300)},
			wantType:   destType,
			wantScalar: true,
		},
		{
			name:       "TEST02", //cast(333.33300 as decimal(10, 4))
			vecs:       makeTempVector(types.Decimal64FromInt32(33333300), leftType, false, destType),
			proc:       procs,
			wantBytes:  []types.Decimal64{types.Decimal64FromInt32(33333300)},
			wantType:   destType,
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, castRes.Col)
			require.Equal(t, c.wantType.Oid, castRes.Typ.Oid)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}
}

func TestCastDecimal128AsDecimal128(t *testing.T) {
	// Cast converts decimal128 to decimal128
	makeTempVector := func(left types.Decimal128, leftType types.Type, leftScalar bool, destType types.Type) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		if leftScalar {
			vectors[0] = vector.NewConstFixed(leftType, 1, left, testutil.TestUtilMp)
		} else {
			vectors[0] = vector.NewWithFixed(leftType, []types.Decimal128{left}, nil, testutil.TestUtilMp)
		}

		vectors[1] = vector.New(destType)
		return vectors
	}

	leftType := types.Type{Oid: types.T_decimal128, Size: 16, Width: 20, Scale: 5}
	destType := types.Type{Oid: types.T_decimal128, Size: 16, Width: 20, Scale: 5}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  interface{}
		wantType   types.Type
		wantScalar bool
	}{
		{
			name:       "TEST01", //cast(333.33300 as decimal(20, 5))
			vecs:       makeTempVector(types.Decimal128FromInt32(33333300), leftType, true, destType),
			proc:       procs,
			wantBytes:  []types.Decimal128{types.Decimal128FromInt32(33333300)},
			wantType:   destType,
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVector(types.Decimal128FromInt32(33333300), leftType, false, destType),
			proc:       procs,
			wantBytes:  []types.Decimal128{types.Decimal128FromInt32(33333300)},
			wantType:   destType,
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, castRes.Col)
			require.Equal(t, c.wantType.Oid, castRes.Typ.Oid)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}
}

/*
 * Honestly I have no idea what this is testing ...
 *
func TestCastStringAsDecimal64(t *testing.T) {

	makeDecimal64Vector := func(values []int64, nsp []uint64, width int32, scale int32) *vector.Vector {
		d64 := types.Type{
			Oid:   types.T_decimal64,
			Size:  8,
			Width: width,
			Scale: scale,
		}
		vec := vector.New(d64)
		for _, n := range nsp {
			nulls.Add(vec.Nsp, n)
		}
		ptr := (*[]types.Decimal64)(unsafe.Pointer(&values))
		vec.Col = *ptr
		return vec
	}

	makeScalarDecimal64 := func(v int64, length int, width int32, scale int32) *vector.Vector {
		d64 := types.Type{
			Oid:   types.T_decimal64,
			Size:  8,
			Width: width,
			Scale: scale,
		}
		vec := testutil.NewProc().AllocScalarVector(d64)
		vec.Length = length
		var tmp types.Decimal64
		tmp.FromInt64(v)
		vec.Col = []types.Decimal64{tmp}
		return vec
	}

	convey.Convey("TestCastStringAsDecimal64", t, func() {
		type kase struct {
			s    string
			want int64
		}

		kases := []kase{
			{
				s:    "333.333",
				want: 33333300,
			},
			{
				s:    "-1234.5",
				want: -123450000,
			},
		}

		var inStr []string
		var wantDecimal64 []int64
		for _, k := range kases {
			inStr = append(inStr, k.s)
			wantDecimal64 = append(wantDecimal64, k.want)
		}

		inVector := testutil.MakeVarcharVector(inStr, nil)
		destVector := makeDecimal64Vector(nil, nil, 10, 5)
		wantVector := makeDecimal64Vector(wantDecimal64, nil, 10, 5)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{inVector, destVector}, proc)
		//res, err := CastStringAsDecimal64(inVector, destVector, proc)
		convey.ShouldBeNil(err)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("TestCasetScalarStringAsDecimal64", t, func() {
		type kase struct {
			s    string
			want int64
		}

		k := kase{
			s:    "333.123",
			want: 33312300,
		}

		inVector := testutil.MakeScalarVarchar(k.s, 10)
		wantVector := makeScalarDecimal64(k.want, 10, 10, 5)
		destVector := makeDecimal64Vector(nil, nil, 10, 5)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{inVector, destVector}, proc)
		//res, err := CastStringAsDecimal64(inVector, destVector, proc)
		convey.ShouldBeNil(err)
		compare := testutil.CompareVectors(wantVector, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

*
*/

func TestCastTimeStampAsDatetime(t *testing.T) {
	//Cast converts timestamp to datetime
	convey.Convey("Cast timestamp to datetime", t, func() {
		type kase struct {
			s    string
			want string
		}
		kases := []kase{
			{
				s:    "2004-04-03 12:14:35",
				want: "2004-04-03 12:14:35",
			},
			{
				s:    "2021-10-03 11:52:21",
				want: "2021-10-03 11:52:21",
			},
			{
				s:    "2020-08-23 11:52:21",
				want: "2020-08-23 11:52:21",
			},
			{
				s:    "2021-11-23 16:12:21",
				want: "2021-11-23 16:12:21",
			},
			{
				s:    "2014-09-23 16:17:21",
				want: "2014-09-23 16:17:21",
			},
		}

		var inStrs []string
		var wantStrs []string
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeTimeStampVector(inStrs, nil)
		destVector := testutil.MakeDateTimeVector(nil, nil)
		wantVec := testutil.MakeDateTimeVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Cast scalar timestamp to datetime", t, func() {
		type kase struct {
			s    string
			want string
		}
		k := kase{
			s:    "2021-10-03 11:52:21",
			want: "2021-10-03 11:52:21",
		}

		srcVector := testutil.MakeScalarTimeStamp(k.s, 10)
		destVector := testutil.MakeDateTimeVector(nil, nil)
		wantVec := testutil.MakeScalarDateTime(k.want, 10)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

func TestCastDatetimeAsTimeStamp(t *testing.T) {
	//Cast converts timestamp to datetime
	convey.Convey("Cast datetime to timestamp", t, func() {
		type kase struct {
			s    string
			want string
		}
		kases := []kase{
			{
				s:    "2004-04-03 12:14:35",
				want: "2004-04-03 12:14:35",
			},
			{
				s:    "2021-10-03 11:52:21",
				want: "2021-10-03 11:52:21",
			},
			{
				s:    "2020-08-23 11:52:21",
				want: "2020-08-23 11:52:21",
			},
			{
				s:    "2021-11-23 16:12:21",
				want: "2021-11-23 16:12:21",
			},
			{
				s:    "2014-09-23 16:17:21",
				want: "2014-09-23 16:17:21",
			},
		}

		var inStrs []string
		var wantStrs []string
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeDateTimeVector(inStrs, nil)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeTimeStampVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Cast scalar datetimeto timestamp ", t, func() {
		type kase struct {
			s    string
			want string
		}
		k := kase{
			s:    "2021-10-03 11:52:21",
			want: "2021-10-03 11:52:21",
		}

		srcVector := testutil.MakeScalarDateTime(k.s, 10)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeScalarTimeStamp(k.want, 10)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

func TestCastDateAsTimeStamp(t *testing.T) {
	//Cast converts timestamp to datetime
	convey.Convey("Cast date to timeStamp", t, func() {
		type kase struct {
			s    string
			want string
		}
		kases := []kase{
			{
				s:    "2004-04-03",
				want: "2004-04-03 00:00:00",
			},
			{
				s:    "2021-10-03",
				want: "2021-10-03 00:00:00",
			},
			{
				s:    "2020-08-23",
				want: "2020-08-23 00:00:00",
			},
			{
				s:    "2021-11-23",
				want: "2021-11-23 00:00:00",
			},
			{
				s:    "2014-09-23",
				want: "2014-09-23 00:00:00",
			},
		}

		var inStrs []string
		var wantStrs []string
		for _, k := range kases {
			inStrs = append(inStrs, k.s)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeDateVector(inStrs, nil)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeTimeStampVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Cast scalar date to timestamp", t, func() {
		type kase struct {
			s    string
			want string
		}
		k := kase{
			s:    "2021-10-03",
			want: "2021-10-03 00:00:00",
		}

		srcVector := testutil.MakeScalarDate(k.s, 10)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeScalarTimeStamp(k.want, 10)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

func TestCastIntegerAsTimestamp(t *testing.T) {

	wantDatetimeFromUnix := func(ts int64) string {
		return time.Unix(ts, 0).Local().Format("2006-01-02 15:04:05")
	}
	//Cast converts int8 to timeStamp
	convey.Convey("Cast int8 to timeStamp", t, func() {
		type kase struct {
			intval int8
			want   string
		}
		kases := []kase{
			{
				intval: 23,
				want:   wantDatetimeFromUnix(23),
			},
			{
				intval: 26,
				want:   wantDatetimeFromUnix(26),
			},
		}

		var intVals []int8
		var wantStrs []string
		for _, k := range kases {
			intVals = append(intVals, k.intval)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeInt8Vector(intVals, nil)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeTimeStampVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	//Cast converts int16 to timeStamp
	convey.Convey("Cast int16 to timeStamp", t, func() {
		type kase struct {
			intval int16
			want   string
		}
		kases := []kase{
			{
				intval: 12000,
				want:   wantDatetimeFromUnix(12000),
			},
			{
				intval: 26200,
				want:   wantDatetimeFromUnix(26200),
			},
		}

		var intVals []int16
		var wantStrs []string
		for _, k := range kases {
			intVals = append(intVals, k.intval)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeInt16Vector(intVals, nil)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeTimeStampVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	//Cast converts int32 to timeStamp
	convey.Convey("Cast int32 to timeStamp", t, func() {
		type kase struct {
			intval int32
			want   string
		}
		kases := []kase{
			{
				intval: 2300000,
				want:   wantDatetimeFromUnix(2300000),
			},
			{
				intval: 2710000,
				want:   wantDatetimeFromUnix(2710000),
			},
		}

		var intVals []int32
		var wantStrs []string
		for _, k := range kases {
			intVals = append(intVals, k.intval)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeInt32Vector(intVals, nil)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeTimeStampVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	//Cast converts int64 to timeStamp
	convey.Convey("Cast int64 to timeStamp", t, func() {
		type kase struct {
			intval int64
			want   string
		}
		kases := []kase{
			{
				intval: 833453453,
				want:   wantDatetimeFromUnix(833453453),
			},
			{
				intval: 933453453,
				want:   wantDatetimeFromUnix(933453453),
			},
		}

		var intVals []int64
		var wantStrs []string
		for _, k := range kases {
			intVals = append(intVals, k.intval)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeInt64Vector(intVals, nil)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeTimeStampVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	//Cast converts uint8 to timeStamp
	convey.Convey("Cast uint8 to timeStamp", t, func() {
		type kase struct {
			intval uint8
			want   string
		}
		kases := []kase{
			{
				intval: 233,
				want:   wantDatetimeFromUnix(233),
			},
			{
				intval: 254,
				want:   wantDatetimeFromUnix(254),
			},
		}

		var intVals []uint8
		var wantStrs []string
		for _, k := range kases {
			intVals = append(intVals, k.intval)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeUint8Vector(intVals, nil)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeTimeStampVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	//Cast converts uint16 to timeStamp
	convey.Convey("Cast uint16 to timeStamp", t, func() {
		type kase struct {
			intval uint16
			want   string
		}
		kases := []kase{
			{
				intval: 33345,
				want:   wantDatetimeFromUnix(33345),
			},
			{
				intval: 43345,
				want:   wantDatetimeFromUnix(43345),
			},
		}

		var intVals []uint16
		var wantStrs []string
		for _, k := range kases {
			intVals = append(intVals, k.intval)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeUint16Vector(intVals, nil)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeTimeStampVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	//Cast converts uint32 to timeStamp
	convey.Convey("Cast uint32 to timeStamp", t, func() {
		type kase struct {
			intval uint32
			want   string
		}
		kases := []kase{
			{
				intval: 83345789,
				want:   wantDatetimeFromUnix(83345789),
			},
			{
				intval: 89345789,
				want:   wantDatetimeFromUnix(89345789),
			},
		}

		var intVals []uint32
		var wantStrs []string
		for _, k := range kases {
			intVals = append(intVals, k.intval)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeUint32Vector(intVals, nil)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeTimeStampVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	//Cast converts uint64 to timeStamp
	convey.Convey("Cast uint64 to timeStamp", t, func() {
		type kase struct {
			intval uint64
			want   string
		}
		kases := []kase{
			{
				intval: 1998933575,
				want:   wantDatetimeFromUnix(1998933575),
			},
			{
				intval: 1298933575,
				want:   wantDatetimeFromUnix(1298933575),
			},
		}

		var intVals []uint64
		var wantStrs []string
		for _, k := range kases {
			intVals = append(intVals, k.intval)
			wantStrs = append(wantStrs, k.want)
		}

		srcVector := testutil.MakeUint64Vector(intVals, nil)
		destVector := testutil.MakeTimeStampVector(nil, nil)
		wantVec := testutil.MakeTimeStampVector(wantStrs, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

func TestCastNullAsAllType(t *testing.T) {
	//Cast null as (int8/int16/int32/int64/uint8/uint16/uint32/uint64/float32/float64/date/datetime/timestamp/decimal64/decimal128/char/varchar)
	makeTempVectors := func(srcType types.T, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeScalarNullVector(srcType)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		bitMap     *roaring.Bitmap
		wantScalar bool
	}{
		{
			name:       "Test1",
			vecs:       makeTempVectors(types.T_int8, types.T_int8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test2",
			vecs:       makeTempVectors(types.T_int8, types.T_int16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test3",
			vecs:       makeTempVectors(types.T_int8, types.T_int32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test4",
			vecs:       makeTempVectors(types.T_int8, types.T_int64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test5",
			vecs:       makeTempVectors(types.T_int8, types.T_uint8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test6",
			vecs:       makeTempVectors(types.T_int8, types.T_uint16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test7",
			vecs:       makeTempVectors(types.T_int8, types.T_uint32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test8",
			vecs:       makeTempVectors(types.T_int8, types.T_uint64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test9",
			vecs:       makeTempVectors(types.T_int8, types.T_float32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test10",
			vecs:       makeTempVectors(types.T_int8, types.T_float64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test11",
			vecs:       makeTempVectors(types.T_int8, types.T_decimal128),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test12",
			vecs:       makeTempVectors(types.T_int8, types.T_char),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test13",
			vecs:       makeTempVectors(types.T_int8, types.T_varchar),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test14",
			vecs:       makeTempVectors(types.T_int16, types.T_int8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test15",
			vecs:       makeTempVectors(types.T_int16, types.T_int16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test16",
			vecs:       makeTempVectors(types.T_int16, types.T_int32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test17",
			vecs:       makeTempVectors(types.T_int16, types.T_int64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test18",
			vecs:       makeTempVectors(types.T_int16, types.T_uint8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test19",
			vecs:       makeTempVectors(types.T_int16, types.T_uint16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test20",
			vecs:       makeTempVectors(types.T_int16, types.T_uint32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test21",
			vecs:       makeTempVectors(types.T_int16, types.T_uint64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test22",
			vecs:       makeTempVectors(types.T_int16, types.T_float32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test23",
			vecs:       makeTempVectors(types.T_int16, types.T_float64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test24",
			vecs:       makeTempVectors(types.T_int16, types.T_decimal128),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test25",
			vecs:       makeTempVectors(types.T_int16, types.T_char),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test26",
			vecs:       makeTempVectors(types.T_int16, types.T_varchar),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test27",
			vecs:       makeTempVectors(types.T_int32, types.T_int8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test28",
			vecs:       makeTempVectors(types.T_int32, types.T_int16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test29",
			vecs:       makeTempVectors(types.T_int32, types.T_int32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test30",
			vecs:       makeTempVectors(types.T_int32, types.T_int64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test31",
			vecs:       makeTempVectors(types.T_int32, types.T_uint8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test32",
			vecs:       makeTempVectors(types.T_int32, types.T_uint16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test33",
			vecs:       makeTempVectors(types.T_int32, types.T_uint32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test34",
			vecs:       makeTempVectors(types.T_int32, types.T_uint64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test35",
			vecs:       makeTempVectors(types.T_int32, types.T_float32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test36",
			vecs:       makeTempVectors(types.T_int32, types.T_float64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test37",
			vecs:       makeTempVectors(types.T_int32, types.T_decimal128),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test38",
			vecs:       makeTempVectors(types.T_int32, types.T_char),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test39",
			vecs:       makeTempVectors(types.T_int32, types.T_varchar),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test40",
			vecs:       makeTempVectors(types.T_int64, types.T_int8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test41",
			vecs:       makeTempVectors(types.T_int64, types.T_int16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test42",
			vecs:       makeTempVectors(types.T_int64, types.T_int32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test43",
			vecs:       makeTempVectors(types.T_int64, types.T_int64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test44",
			vecs:       makeTempVectors(types.T_int64, types.T_uint8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test45",
			vecs:       makeTempVectors(types.T_int64, types.T_uint16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test46",
			vecs:       makeTempVectors(types.T_int64, types.T_uint32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test47",
			vecs:       makeTempVectors(types.T_int64, types.T_uint64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test48",
			vecs:       makeTempVectors(types.T_int64, types.T_float32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test49",
			vecs:       makeTempVectors(types.T_int64, types.T_float64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test50",
			vecs:       makeTempVectors(types.T_int64, types.T_decimal128),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test51",
			vecs:       makeTempVectors(types.T_int64, types.T_char),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test52",
			vecs:       makeTempVectors(types.T_int64, types.T_varchar),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test53",
			vecs:       makeTempVectors(types.T_uint8, types.T_int8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test54",
			vecs:       makeTempVectors(types.T_uint8, types.T_int16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test55",
			vecs:       makeTempVectors(types.T_uint8, types.T_int32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test56",
			vecs:       makeTempVectors(types.T_uint8, types.T_int64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test57",
			vecs:       makeTempVectors(types.T_uint8, types.T_uint8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test58",
			vecs:       makeTempVectors(types.T_uint8, types.T_uint16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test59",
			vecs:       makeTempVectors(types.T_uint8, types.T_uint32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test60",
			vecs:       makeTempVectors(types.T_uint8, types.T_uint64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test61",
			vecs:       makeTempVectors(types.T_uint8, types.T_float32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test62",
			vecs:       makeTempVectors(types.T_uint8, types.T_float64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test63",
			vecs:       makeTempVectors(types.T_uint8, types.T_decimal128),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test64",
			vecs:       makeTempVectors(types.T_uint8, types.T_char),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test65",
			vecs:       makeTempVectors(types.T_uint8, types.T_varchar),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test66",
			vecs:       makeTempVectors(types.T_uint16, types.T_int8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test67",
			vecs:       makeTempVectors(types.T_uint16, types.T_int16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test68",
			vecs:       makeTempVectors(types.T_uint16, types.T_int32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test69",
			vecs:       makeTempVectors(types.T_uint16, types.T_int64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test70",
			vecs:       makeTempVectors(types.T_uint16, types.T_uint8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test71",
			vecs:       makeTempVectors(types.T_uint16, types.T_uint16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test72",
			vecs:       makeTempVectors(types.T_uint16, types.T_uint32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test73",
			vecs:       makeTempVectors(types.T_uint16, types.T_uint64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test74",
			vecs:       makeTempVectors(types.T_uint16, types.T_float32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test75",
			vecs:       makeTempVectors(types.T_uint16, types.T_float64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test76",
			vecs:       makeTempVectors(types.T_uint16, types.T_decimal128),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test77",
			vecs:       makeTempVectors(types.T_uint16, types.T_char),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test78",
			vecs:       makeTempVectors(types.T_uint16, types.T_varchar),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test79",
			vecs:       makeTempVectors(types.T_uint32, types.T_int8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test80",
			vecs:       makeTempVectors(types.T_uint32, types.T_int16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test81",
			vecs:       makeTempVectors(types.T_uint32, types.T_int32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test82",
			vecs:       makeTempVectors(types.T_uint32, types.T_int64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test83",
			vecs:       makeTempVectors(types.T_uint32, types.T_uint8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test84",
			vecs:       makeTempVectors(types.T_uint32, types.T_uint16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test85",
			vecs:       makeTempVectors(types.T_uint32, types.T_uint32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test86",
			vecs:       makeTempVectors(types.T_uint32, types.T_uint64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test87",
			vecs:       makeTempVectors(types.T_uint32, types.T_float32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test88",
			vecs:       makeTempVectors(types.T_uint32, types.T_float64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test89",
			vecs:       makeTempVectors(types.T_uint32, types.T_decimal128),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test90",
			vecs:       makeTempVectors(types.T_uint32, types.T_char),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test91",
			vecs:       makeTempVectors(types.T_uint32, types.T_varchar),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test92",
			vecs:       makeTempVectors(types.T_uint64, types.T_int8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test93",
			vecs:       makeTempVectors(types.T_uint64, types.T_int16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test94",
			vecs:       makeTempVectors(types.T_uint64, types.T_int32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test95",
			vecs:       makeTempVectors(types.T_uint64, types.T_int64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test96",
			vecs:       makeTempVectors(types.T_uint64, types.T_uint8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test97",
			vecs:       makeTempVectors(types.T_uint64, types.T_uint16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test98",
			vecs:       makeTempVectors(types.T_uint64, types.T_uint32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test99",
			vecs:       makeTempVectors(types.T_uint64, types.T_uint64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test100",
			vecs:       makeTempVectors(types.T_uint64, types.T_float32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test101",
			vecs:       makeTempVectors(types.T_uint64, types.T_float64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test102",
			vecs:       makeTempVectors(types.T_uint64, types.T_decimal128),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test103",
			vecs:       makeTempVectors(types.T_uint64, types.T_char),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test104",
			vecs:       makeTempVectors(types.T_uint64, types.T_varchar),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test105",
			vecs:       makeTempVectors(types.T_float32, types.T_int8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test106",
			vecs:       makeTempVectors(types.T_float32, types.T_int16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test107",
			vecs:       makeTempVectors(types.T_float32, types.T_int32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test108",
			vecs:       makeTempVectors(types.T_float32, types.T_int64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test109",
			vecs:       makeTempVectors(types.T_float32, types.T_uint8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test110",
			vecs:       makeTempVectors(types.T_float32, types.T_uint16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test111",
			vecs:       makeTempVectors(types.T_float32, types.T_uint32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test112",
			vecs:       makeTempVectors(types.T_float32, types.T_uint64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test113",
			vecs:       makeTempVectors(types.T_float32, types.T_float32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test114",
			vecs:       makeTempVectors(types.T_float32, types.T_float64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test115",
			vecs:       makeTempVectors(types.T_float32, types.T_char),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test116",
			vecs:       makeTempVectors(types.T_float32, types.T_varchar),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test117",
			vecs:       makeTempVectors(types.T_float64, types.T_int8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test118",
			vecs:       makeTempVectors(types.T_float64, types.T_int16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test119",
			vecs:       makeTempVectors(types.T_float64, types.T_int32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test120",
			vecs:       makeTempVectors(types.T_float64, types.T_int64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test121",
			vecs:       makeTempVectors(types.T_float64, types.T_uint8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test122",
			vecs:       makeTempVectors(types.T_float64, types.T_uint16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test123",
			vecs:       makeTempVectors(types.T_float64, types.T_uint32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test124",
			vecs:       makeTempVectors(types.T_float64, types.T_uint64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test125",
			vecs:       makeTempVectors(types.T_float64, types.T_float32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test126",
			vecs:       makeTempVectors(types.T_float64, types.T_float64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test127",
			vecs:       makeTempVectors(types.T_float64, types.T_char),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test128",
			vecs:       makeTempVectors(types.T_float64, types.T_varchar),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0129",
			vecs:       makeTempVectors(types.T_char, types.T_int8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0130",
			vecs:       makeTempVectors(types.T_char, types.T_int16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0131",
			vecs:       makeTempVectors(types.T_char, types.T_int32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0132",
			vecs:       makeTempVectors(types.T_char, types.T_int64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0133",
			vecs:       makeTempVectors(types.T_char, types.T_uint8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0134",
			vecs:       makeTempVectors(types.T_char, types.T_uint16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0135",
			vecs:       makeTempVectors(types.T_char, types.T_uint32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0136",
			vecs:       makeTempVectors(types.T_char, types.T_uint64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0137",
			vecs:       makeTempVectors(types.T_char, types.T_float32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0138",
			vecs:       makeTempVectors(types.T_char, types.T_float64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0139",
			vecs:       makeTempVectors(types.T_char, types.T_char),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0140",
			vecs:       makeTempVectors(types.T_char, types.T_varchar),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0141",
			vecs:       makeTempVectors(types.T_varchar, types.T_int8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0142",
			vecs:       makeTempVectors(types.T_varchar, types.T_int16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0143",
			vecs:       makeTempVectors(types.T_varchar, types.T_int32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0144",
			vecs:       makeTempVectors(types.T_varchar, types.T_int64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0145",
			vecs:       makeTempVectors(types.T_varchar, types.T_uint8),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0146",
			vecs:       makeTempVectors(types.T_varchar, types.T_uint16),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0147",
			vecs:       makeTempVectors(types.T_varchar, types.T_uint32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0148",
			vecs:       makeTempVectors(types.T_varchar, types.T_uint64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0149",
			vecs:       makeTempVectors(types.T_varchar, types.T_float32),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0150",
			vecs:       makeTempVectors(types.T_varchar, types.T_float64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0151",
			vecs:       makeTempVectors(types.T_varchar, types.T_date),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0152",
			vecs:       makeTempVectors(types.T_varchar, types.T_datetime),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0153",
			vecs:       makeTempVectors(types.T_varchar, types.T_timestamp),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0154",
			vecs:       makeTempVectors(types.T_varchar, types.T_char),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0155",
			vecs:       makeTempVectors(types.T_varchar, types.T_varchar),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0156",
			vecs:       makeTempVectors(types.T_date, types.T_date),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0157",
			vecs:       makeTempVectors(types.T_datetime, types.T_datetime),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0158",
			vecs:       makeTempVectors(types.T_datetime, types.T_datetime),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0159",
			vecs:       makeTempVectors(types.T_timestamp, types.T_datetime),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0160",
			vecs:       makeTempVectors(types.T_timestamp, types.T_timestamp),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0161",
			vecs:       makeTempVectors(types.T_decimal64, types.T_decimal64),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0162",
			vecs:       makeTempVectors(types.T_decimal64, types.T_decimal128),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
		{
			name:       "Test0163",
			vecs:       makeTempVectors(types.T_decimal128, types.T_decimal128),
			proc:       procs,
			bitMap:     roaring.BitmapOf(0),
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}
}

func TestCastBoolAsString(t *testing.T) {
	//Cast converts bool to char
	//Cast converts bool to varchar
	makeTempVectors := func(src bool, srcIsConst bool, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeVector(src, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantType   types.T
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors(true, true, types.T_char),
			proc:       procs,
			wantValues: []string{"1"},
			wantType:   types.T_char,
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors(true, false, types.T_char),
			proc:       procs,
			wantValues: []string{"1"},
			wantType:   types.T_char,
			wantScalar: false,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors(false, false, types.T_varchar),
			proc:       procs,
			wantValues: []string{"0"},
			wantType:   types.T_varchar,
			wantScalar: false,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors(false, false, types.T_varchar),
			proc:       procs,
			wantValues: []string{"0"},
			wantType:   types.T_varchar,
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			got := vector.GetStrVectorValues(castRes)
			require.Equal(t, c.wantValues, got)
			require.Equal(t, c.wantType, castRes.Typ.Oid)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}

}

// date to datetime and date to string
func TestCastDateAsDatetimeAndString(t *testing.T) {
	makeTempVectors := func(src string, srcIsConst bool, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		date, _ := types.ParseDateCast(src)
		vectors[0] = makeVector(date, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantType   types.T
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors("1992-01-01", true, types.T_datetime),
			proc:       procs,
			wantValues: []types.Datetime{types.FromClock(1992, 1, 1, 0, 0, 0, 0)},
			wantType:   types.T_datetime,
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors("1992-01-01", false, types.T_datetime),
			proc:       procs,
			wantValues: []types.Datetime{types.FromClock(1992, 1, 1, 0, 0, 0, 0)},
			wantType:   types.T_datetime,
			wantScalar: false,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors("1992-01-01", true, types.T_char),
			proc:       procs,
			wantValues: []string{"1992-01-01"},
			wantType:   types.T_char,
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors("1992-02-02", true, types.T_varchar),
			proc:       procs,
			wantValues: []string{"1992-02-02"},
			wantType:   types.T_varchar,
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			if castRes.GetType().IsVarlen() {
				got := vector.GetStrVectorValues(castRes)
				require.Equal(t, c.wantValues, got)
			} else {
				require.Equal(t, c.wantValues, castRes.Col)
			}
			require.Equal(t, c.wantType, castRes.Typ.Oid)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}

}

// datetime to date and datetime to string
func TestCastDatetimeAsDateAndString(t *testing.T) {
	makeTempVectors := func(src string, srcIsConst bool, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		date, _ := types.ParseDatetime(src, 0)
		vectors[0] = makeVector(date, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantType   types.T
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors("1992-01-01 00:00:00", true, types.T_date),
			proc:       procs,
			wantValues: []types.Date{types.FromCalendar(1992, 1, 1)},
			wantType:   types.T_date,
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors("1992-01-01 00:00:00", false, types.T_date),
			proc:       procs,
			wantValues: []types.Date{types.FromCalendar(1992, 1, 1)},
			wantType:   types.T_date,
			wantScalar: false,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors("1992-01-01 00:00:00", true, types.T_char),
			proc:       procs,
			wantValues: []string{"1992-01-01 00:00:00"},
			wantType:   types.T_char,
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors("1992-02-02 00:00:00", true, types.T_varchar),
			proc:       procs,
			wantValues: []string{"1992-02-02 00:00:00"},
			wantType:   types.T_varchar,
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			if castRes.GetType().IsVarlen() {
				got := vector.GetStrVectorValues(castRes)
				require.Equal(t, c.wantValues, got)
			} else {
				require.Equal(t, c.wantValues, castRes.Col)
			}
			require.Equal(t, c.wantType, castRes.Typ.Oid)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}

}

// cast uuid to string
func TestCastUuidAsString(t *testing.T) {
	// White box test
	convey.Convey("test cast uuid to string", t, func() {
		cases := []struct {
			uuid types.Uuid
			str  string
		}{
			{
				uuid: [16]byte{13, 86, 135, 218, 42, 103, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
				str:  "0d5687da-2a67-11ed-99e0-000c29847904",
			},
			{
				uuid: [16]byte{97, 25, 223, 253, 42, 107, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
				str:  "6119dffd-2a6b-11ed-99e0-000c29847904",
			},
			{
				uuid: [16]byte{62, 53, 10, 92, 34, 42, 17, 235, 171, 239, 2, 66, 172, 17, 0, 2},
				str:  "3e350a5c-222a-11eb-abef-0242ac110002",
			},
			{
				uuid: [16]byte{62, 53, 10, 92, 34, 42, 17, 235, 171, 239, 2, 66, 172, 17, 0, 2},
				str:  "3e350a5c222a11ebabef0242ac110002",
			},
		}

		var uuids []types.Uuid
		var expects []string
		for _, c := range cases {
			uuids = append(uuids, c.uuid)
			expects = append(expects, c.str)
		}

		srcVector := testutil.MakeUuidVector(uuids, nil)
		destVector := testutil.MakeVarcharVector(nil, nil)

		wantVec := testutil.MakeVarcharVector(expects, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.ShouldBeNil(err)
		compare := testutil.CompareVectors(wantVec, res)
		convey.ShouldBeTrue(compare)
	})

}

// cast string to uuid
func TestCastStringToUuid(t *testing.T) {
	// White box test
	convey.Convey("test cast to uuid case", t, func() {
		cases := []struct {
			str  string
			uuid string
		}{
			{
				str:  "0d5687da-2a67-11ed-99e0-000c29847904",
				uuid: "0d5687da-2a67-11ed-99e0-000c29847904",
				//uuid: [16]byte{13, 86, 135, 218, 42, 103, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
			},
			{
				str:  "6119dffd-2a6b-11ed-99e0-000c29847904",
				uuid: "6119dffd-2a6b-11ed-99e0-000c29847904",
				//uuid: [16]byte{97, 25, 223, 253, 42, 107, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
			},
			{
				str:  "3e350a5c-222a-11eb-abef-0242ac110002",
				uuid: "3e350a5c-222a-11eb-abef-0242ac110002",
				//uuid: [16]byte{62, 53, 10, 92, 34, 42, 17, 235, 171, 239, 2, 66, 172, 17, 0, 2},
			},
			{
				str:  "3e350a5c222a11ebabef0242ac110002",
				uuid: "3e350a5c-222a-11eb-abef-0242ac110002",
				//uuid: [16]byte{62, 53, 10, 92, 34, 42, 17, 235, 171, 239, 2, 66, 172, 17, 0, 2},
			},
		}

		var srcstrs []string
		var expects []string
		for _, c := range cases {
			srcstrs = append(srcstrs, c.str)
			expects = append(expects, c.uuid)
		}

		srcVector := testutil.MakeVarcharVector(srcstrs, nil)
		destVector := testutil.MakeUuidVector(nil, nil)

		wantVec := testutil.MakeUuidVectorByString(expects, nil)
		proc := testutil.NewProc()
		res, err := Cast([]*vector.Vector{srcVector, destVector}, proc)
		convey.ShouldBeNil(err)
		compare := testutil.CompareVectors(wantVec, res)
		convey.ShouldBeTrue(compare)
	})

	// tolerance test
}

func TestCastTimeToString(t *testing.T) {
	makeTempVectors := func(src string, srcIsConst bool, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		time, _ := types.ParseTime(src, 6)
		vectors[0] = makeVector(time, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantType   types.T
		precision  int32
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors("2022-12-01 11:22:33", true, types.T_varchar),
			proc:       procs,
			wantValues: []string{"11:22:33"},
			wantType:   types.T_varchar,
			precision:  0,
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors("2022-12-01 11:22:33", false, types.T_varchar),
			proc:       procs,
			wantValues: []string{"11:22:33.00"},
			wantType:   types.T_varchar,
			precision:  2,
			wantScalar: false,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors("2022-12-01 11:22:33", true, types.T_varchar),
			proc:       procs,
			wantValues: []string{"11:22:33.000000"},
			wantType:   types.T_varchar,
			precision:  6,
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors("-11223", true, types.T_varchar),
			proc:       procs,
			wantValues: []string{"-01:12:23.000"},
			wantType:   types.T_varchar,
			precision:  3,
			wantScalar: true,
		},
		{
			name:       "Test05",
			vecs:       makeTempVectors("-11223.4448", false, types.T_varchar),
			proc:       procs,
			wantValues: []string{"-01:12:23.444"},
			wantType:   types.T_varchar,
			precision:  3,
			wantScalar: false,
		},
		{
			name:       "Test06",
			vecs:       makeTempVectors("-11223.4448", true, types.T_char),
			proc:       procs,
			wantValues: []string{"-01:12:23.444"},
			wantType:   types.T_char,
			precision:  3,
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// setting precision
			c.vecs[0].Typ.Precision = c.precision
			castRes, err := Cast(c.vecs, c.proc)
			require.NoError(t, err)

			if castRes.GetType().IsVarlen() {
				got := vector.GetStrVectorValues(castRes)
				require.Equal(t, c.wantValues, got)
			} else {
				require.Equal(t, c.wantValues, castRes.Col)
			}

			require.Equal(t, c.wantType, castRes.Typ.Oid)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}

}
func TestCastStringToTime(t *testing.T) {
	makeTempVectors := func(src string, srcIsConst bool, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeStringVector(src, types.T_varchar, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantType   types.T
		precision  int32
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors("2022-12-01 11:22:33", true, types.T_time),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(false, 11, 22, 33, 0)},
			wantType:   types.T_time,
			precision:  0,
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors("2022-12-01 11:22:33.125", true, types.T_time),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(false, 11, 22, 33, 130000)},
			wantType:   types.T_time,
			precision:  2,
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors("2022-12-01 11:22:33", true, types.T_time),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(false, 11, 22, 33, 0)},
			wantType:   types.T_time,
			precision:  6,
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeTempVectors("-11223", true, types.T_time),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(true, 1, 12, 23, 0)},
			wantType:   types.T_time,
			precision:  3,
			wantScalar: true,
		},
		{
			name:       "Test05",
			vecs:       makeTempVectors("11223.4444", true, types.T_time),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(false, 1, 12, 23, 444000)},
			wantType:   types.T_time,
			precision:  3,
			wantScalar: true,
		},
		{
			name:       "Test06",
			vecs:       makeTempVectors("-11223.4448", true, types.T_time),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(true, 1, 12, 23, 445000)},
			wantType:   types.T_time,
			precision:  3,
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// setting precision
			c.vecs[1].Typ.Precision = c.precision
			castRes, err := Cast(c.vecs, c.proc)
			require.NoError(t, err)
			require.Equal(t, c.wantValues, castRes.Col)
			require.Equal(t, c.wantType, castRes.Typ.Oid)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}

}

func TestCastInt64ToTime(t *testing.T) {
	makeTempVectors := func(src int64, srcIsConst bool, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeVector(src, false)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantType   types.T
		precision  int32
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors(112233, true, types.T_time),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(false, 11, 22, 33, 0)},
			wantType:   types.T_time,
			precision:  0,
			wantScalar: false,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors(-223344, true, types.T_time),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(true, 22, 33, 44, 0)},
			wantType:   types.T_time,
			precision:  2,
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// setting precision
			c.vecs[1].Typ.Precision = c.precision
			castRes, err := Cast(c.vecs, c.proc)
			require.NoError(t, err)
			require.Equal(t, c.wantValues, castRes.Col)
			require.Equal(t, c.wantType, castRes.Typ.Oid)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}
}

func TestCastDecimal128ToTime(t *testing.T) {
	makeTempVectors := func(dcmStr string, isScalar bool, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		dcm, _ := types.Decimal128_FromString(dcmStr)
		dcmType := types.Type{Oid: types.T_decimal128, Size: 8, Width: 34, Scale: 6}

		if isScalar {
			vectors[0] = vector.NewConstFixed(dcmType, 1, dcm, testutil.TestUtilMp)
		} else {
			vectors[0] = vector.NewWithFixed(dcmType, []types.Decimal128{dcm}, nil, testutil.TestUtilMp)
		}
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantType   types.T
		precision  int32
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors("112233.4444", true, types.T_time),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(false, 11, 22, 33, 444000)},
			wantType:   types.T_time,
			precision:  3,
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors("-223344.4445", true, types.T_time),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(true, 22, 33, 44, 444000)},
			wantType:   types.T_time,
			precision:  3,
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// setting precision
			c.vecs[1].Typ.Precision = c.precision
			castRes, err := Cast(c.vecs, c.proc)
			require.NoError(t, err)
			require.Equal(t, c.wantValues, castRes.Col)
			require.Equal(t, c.wantType, castRes.Typ.Oid)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}
}

func TestCastDateAndDatetimeToTime(t *testing.T) {
	makeTempVectors := func(src string, srcIsConst bool, precision int32, srcType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		switch srcType {
		case types.T_date:
			date, err := types.ParseDateCast(src)
			require.NoError(t, err)
			vectors[0] = makeVector(date, srcIsConst)
		case types.T_datetime:
			datetime, err := types.ParseDatetime(src, 6)
			require.NoError(t, err)
			vectors[0] = makeVector(datetime, srcIsConst)
		default:
			panic("wrong input test type")
		}
		vectors[1] = makeTypeVector(types.T_time)
		vectors[1].Typ.Precision = precision
		return vectors
	}

	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		precision  int32
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVectors("2022-12-01", true, 0, types.T_date),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(false, 0, 0, 0, 0)},
			precision:  0,
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVectors("2022-12-01 11:22:33", false, 0, types.T_datetime),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(false, 11, 22, 33, 0)},
			precision:  0,
			wantScalar: false,
		},
		{
			name:       "Test03",
			vecs:       makeTempVectors("2022-12-01 11:22:33.123456", true, 0, types.T_datetime),
			proc:       procs,
			wantValues: []types.Time{types.FromTimeClock(false, 11, 22, 33, 123000)},
			precision:  3,
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// setting precision
			c.vecs[1].Typ.Precision = c.precision
			castRes, err := Cast(c.vecs, c.proc)
			require.NoError(t, err)
			require.Equal(t, c.wantValues, castRes.Col)
			require.Equal(t, types.T_time, castRes.Typ.Oid)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}

}

func TestCastJsonToString(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantValues interface{}
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeJsonVectors(`"1"`, true, types.T_varchar),
			proc:       procs,
			wantValues: []string{`"1"`},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeJsonVectors(`{"a": "b"}`, false, types.T_varchar),
			proc:       procs,
			wantValues: []string{`{"a": "b"}`},
			wantScalar: false,
		},
		{
			name:       "Test03",
			vecs:       makeJsonVectors(`1`, true, types.T_varchar),
			proc:       procs,
			wantValues: []string{`1`},
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeJsonVectors(`null`, false, types.T_varchar),
			proc:       procs,
			wantValues: []string{`null`},
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			castRes, err := Cast(c.vecs, c.proc)
			require.NoError(t, err)
			got := vector.MustStrCols(castRes)
			require.Equal(t, c.wantValues, got)
			require.Equal(t, types.T_varchar, castRes.Typ.Oid)
			require.Equal(t, c.wantScalar, castRes.IsScalar())
		})
	}
}

func makeTypeVector(t types.T) *vector.Vector {
	return vector.New(t.ToType())
}

// make vector for type of int8,int16,int32,int64,uint8,uint16,uint32,uint64,date,datetime,timestamp,bool
func makeVector(src interface{}, isSrcConst bool) *vector.Vector {
	var typeOid types.T
	var vec *vector.Vector
	mp := testutil.TestUtilMp

	switch val := src.(type) {
	case int8:
		typeOid = types.T_int8
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []int8{val}, nil, mp)
		}

	case int16:
		typeOid = types.T_int16
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []int16{val}, nil, mp)
		}

	case int32:
		typeOid = types.T_int32
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []int32{val}, nil, mp)
		}

	case int64:
		typeOid = types.T_int64
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []int64{val}, nil, mp)
		}

	case uint8:
		typeOid = types.T_uint8
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []uint8{val}, nil, mp)
		}

	case uint16:
		typeOid = types.T_uint16
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []uint16{val}, nil, mp)
		}

	case uint32:
		typeOid = types.T_uint32
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []uint32{val}, nil, mp)
		}

	case uint64:
		typeOid = types.T_uint64
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, testutil.TestUtilMp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []uint64{val}, nil, testutil.TestUtilMp)
		}

	case float32:
		typeOid = types.T_float32
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []float32{val}, nil, mp)
		}

	case float64:
		typeOid = types.T_float64
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []float64{val}, nil, mp)
		}

	case types.Date:
		typeOid = types.T_date
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []types.Date{val}, nil, mp)
		}

	case types.Time:
		typeOid = types.T_time
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []types.Time{val}, nil, mp)
		}

	case types.Datetime:
		typeOid = types.T_datetime
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []types.Datetime{val}, nil, mp)
		}

	case types.Timestamp:
		typeOid = types.T_timestamp
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []types.Timestamp{val}, nil, mp)
		}

	case bool:
		typeOid = types.T_bool
		if isSrcConst {
			vec = vector.NewConstFixed(typeOid.ToType(), 1, val, mp)
		} else {
			vec = vector.NewWithFixed(typeOid.ToType(), []bool{val}, nil, mp)
		}
	}
	return vec
}

func makeScalarNullVector(srcType types.T) *vector.Vector {
	nullVector := vector.NewConstNull(srcType.ToType(), 1)
	return nullVector
}

// make vector for type of char and varchar
func makeStringVector(src string, t types.T, isConst bool) *vector.Vector {
	if isConst {
		return vector.NewConstString(t.ToType(), 1, src, testutil.TestUtilMp)
	} else {
		return vector.NewWithStrings(t.ToType(), []string{src}, nil, testutil.TestUtilMp)
	}
}
func makeJsonVectors(src string, isConst bool, t types.T) []*vector.Vector {
	dt, _ := types.ParseStringToByteJson(src)
	r, _ := dt.Marshal()
	if isConst {
		return []*vector.Vector{vector.NewConstBytes(types.T_json.ToType(), 1, r, testutil.TestUtilMp), makeTypeVector(t)}
	} else {
		return []*vector.Vector{vector.NewWithBytes(types.T_json.ToType(), [][]byte{r}, nil, testutil.TestUtilMp), makeTypeVector(t)}
	}
}
