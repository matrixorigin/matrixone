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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCastSameType(t *testing.T) {
	makeTempVectors := func(src interface{}, destType types.T, srcIsConst bool) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeVector(src, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := makeProcess()
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

	procs := makeProcess()
	//types.Date | types.Datetime | types.Timestamp
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

	procs := makeProcess()
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
	// (char / varhcar) -> (int8 / int16 / int32/ int64 / uint8 / uint16 / uint32 / uint64)

	makeTempVectors := func(src string, srcType types.T, srcIsConst bool, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeStringVector(src, srcType, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := makeProcess()
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
	// (char / varhcar) -> (float32 / float64)
	makeTempVectors := func(src string, srcType types.T, srcIsConst bool, destType types.T) []*vector.Vector {
		vectors := make([]*vector.Vector, 2)
		vectors[0] = makeStringVector(src, srcType, srcIsConst)
		vectors[1] = makeTypeVector(destType)
		return vectors
	}

	procs := makeProcess()
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

func makeTypeVector(t types.T) *vector.Vector {
	return &vector.Vector{
		Col:     nil,
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: t},
		IsConst: false,
		Length:  0,
	}
}

// make vector for type of int8,int16,int32,int64,uint8,uint16,uint32,uint64,date,datetime,timestamp
func makeVector(src interface{}, isSrcConst bool) *vector.Vector {
	var typeOid types.T
	var col interface{}
	switch val := src.(type) {
	case int8:
		typeOid = types.T_int8
		col = []int8{val}
	case int16:
		typeOid = types.T_int16
		col = []int16{val}
	case int32:
		typeOid = types.T_int32
		col = []int32{val}
	case int64:
		typeOid = types.T_int64
		col = []int64{val}
	case uint8:
		typeOid = types.T_uint8
		col = []uint8{val}
	case uint16:
		typeOid = types.T_uint16
		col = []uint16{val}
	case uint32:
		typeOid = types.T_uint32
		col = []uint32{val}
	case uint64:
		typeOid = types.T_uint64
		col = []uint64{val}
	case float32:
		typeOid = types.T_float32
		col = []float32{val}
	case float64:
		typeOid = types.T_float64
		col = []float64{val}
	case types.Date:
		typeOid = types.T_date
		col = []types.Date{val}
	case types.Datetime:
		typeOid = types.T_datetime
		col = []types.Datetime{val}
	case types.Timestamp:
		typeOid = types.T_timestamp
		col = []types.Timestamp{val}
	}

	return &vector.Vector{
		Col:     col,
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: typeOid},
		IsConst: isSrcConst,
		Length:  1,
	}
}

// make vector for type of char and varchar
func makeStringVector(src string, t types.T, isConst bool) *vector.Vector {
	srcBytes := &types.Bytes{
		Data:    []byte(src),
		Offsets: []uint32{0},
		Lengths: []uint32{uint32(len(src))},
	}
	if t == types.T_char {
		return &vector.Vector{
			Col:     srcBytes,
			Nsp:     &nulls.Nulls{},
			Typ:     types.Type{Oid: types.T_char, Size: 24},
			IsConst: isConst,
			Length:  1,
		}
	} else if t == types.T_varchar {
		return &vector.Vector{
			Col:     srcBytes,
			Nsp:     &nulls.Nulls{},
			Typ:     types.Type{Oid: types.T_varchar, Size: 24},
			IsConst: isConst,
			Length:  1,
		}
	}
	return nil
}
