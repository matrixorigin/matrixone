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
			vecs:       makeVectors(int8(-23), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{-23},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeVectors(int16(-23), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{-23},
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeVectors(int32(-23), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{-23},
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeVectors(int64(-23), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{-23},
			wantScalar: true,
		},
		{
			name:       "Test05",
			vecs:       makeVectors(uint8(23), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{23},
			wantScalar: true,
		},
		{
			name:       "Test06",
			vecs:       makeVectors(uint16(23), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{23},
			wantScalar: true,
		},
		{
			name:       "Test07",
			vecs:       makeVectors(uint32(23), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{23},
			wantScalar: true,
		},
		{
			name:       "Test08",
			vecs:       makeVectors(uint64(23), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{23},
			wantScalar: true,
		},
		{
			name:       "Test09",
			vecs:       makeVectors(float32(23.5), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{23.5},
			wantScalar: true,
		},
		{
			name:       "Test10",
			vecs:       makeVectors(float64(23.5), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{23.5},
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

func TestCastSameType2(t *testing.T) {
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
			vecs:       makeVectors(types.Date(729848), types.T_date, true),
			proc:       procs,
			wantValues: []types.Date{729848},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeVectors(types.Datetime(66122056321728512), types.T_datetime, true),
			proc:       procs,
			wantValues: []types.Datetime{66122056321728512},
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeVectors(types.Timestamp(66122026122739712), types.T_timestamp, true),
			proc:       procs,
			wantValues: []types.Timestamp{66122026122739712},
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
			vecs:       makeVectors(int8(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeVectors(int8(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeVectors(int8(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test04",
			vecs:       makeVectors(int8(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test05",
			vecs:       makeVectors(int8(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test06",
			vecs:       makeVectors(int8(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test07",
			vecs:       makeVectors(int8(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test08",
			vecs:       makeVectors(int8(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test09",
			vecs:       makeVectors(int8(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test10",
			vecs:       makeVectors(int8(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test11",
			vecs:       makeVectors(int16(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test12",
			vecs:       makeVectors(int16(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test13",
			vecs:       makeVectors(int16(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test14",
			vecs:       makeVectors(int16(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test15",
			vecs:       makeVectors(int16(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test16",
			vecs:       makeVectors(int16(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test17",
			vecs:       makeVectors(int16(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test18",
			vecs:       makeVectors(int16(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test19",
			vecs:       makeVectors(int16(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test20",
			vecs:       makeVectors(int16(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test21",
			vecs:       makeVectors(int32(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test22",
			vecs:       makeVectors(int32(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test23",
			vecs:       makeVectors(int32(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test24",
			vecs:       makeVectors(int32(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test25",
			vecs:       makeVectors(int32(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test26",
			vecs:       makeVectors(int32(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test27",
			vecs:       makeVectors(int32(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test28",
			vecs:       makeVectors(int32(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test29",
			vecs:       makeVectors(int32(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test30",
			vecs:       makeVectors(int32(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test31",
			vecs:       makeVectors(int64(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test32",
			vecs:       makeVectors(int64(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test33",
			vecs:       makeVectors(int64(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test34",
			vecs:       makeVectors(int64(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test35",
			vecs:       makeVectors(int64(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test36",
			vecs:       makeVectors(int64(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test37",
			vecs:       makeVectors(int64(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test38",
			vecs:       makeVectors(int64(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test39",
			vecs:       makeVectors(int64(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test40",
			vecs:       makeVectors(int64(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test41",
			vecs:       makeVectors(uint8(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test42",
			vecs:       makeVectors(uint8(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test43",
			vecs:       makeVectors(uint8(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test44",
			vecs:       makeVectors(uint8(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test45",
			vecs:       makeVectors(uint8(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test46",
			vecs:       makeVectors(uint8(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test47",
			vecs:       makeVectors(uint8(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test48",
			vecs:       makeVectors(uint8(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test49",
			vecs:       makeVectors(uint8(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test50",
			vecs:       makeVectors(uint8(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test51",
			vecs:       makeVectors(uint16(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test52",
			vecs:       makeVectors(uint16(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test53",
			vecs:       makeVectors(uint16(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test54",
			vecs:       makeVectors(uint16(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test55",
			vecs:       makeVectors(uint16(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test56",
			vecs:       makeVectors(uint16(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test57",
			vecs:       makeVectors(uint16(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test58",
			vecs:       makeVectors(uint16(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test59",
			vecs:       makeVectors(uint16(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test60",
			vecs:       makeVectors(uint16(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test61",
			vecs:       makeVectors(uint32(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test62",
			vecs:       makeVectors(uint32(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test63",
			vecs:       makeVectors(uint32(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test64",
			vecs:       makeVectors(uint32(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test65",
			vecs:       makeVectors(uint32(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test66",
			vecs:       makeVectors(uint32(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test67",
			vecs:       makeVectors(uint32(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test68",
			vecs:       makeVectors(uint32(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test69",
			vecs:       makeVectors(uint32(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test70",
			vecs:       makeVectors(uint32(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test71",
			vecs:       makeVectors(uint64(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test72",
			vecs:       makeVectors(uint64(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test73",
			vecs:       makeVectors(uint64(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test74",
			vecs:       makeVectors(uint64(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test75",
			vecs:       makeVectors(uint64(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test76",
			vecs:       makeVectors(uint64(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test77",
			vecs:       makeVectors(uint64(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test78",
			vecs:       makeVectors(uint64(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test79",
			vecs:       makeVectors(uint64(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test80",
			vecs:       makeVectors(uint64(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test81",
			vecs:       makeVectors(float32(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test82",
			vecs:       makeVectors(float32(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test83",
			vecs:       makeVectors(float32(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test84",
			vecs:       makeVectors(float32(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test85",
			vecs:       makeVectors(float32(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test86",
			vecs:       makeVectors(float32(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test87",
			vecs:       makeVectors(float32(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test88",
			vecs:       makeVectors(float32(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test89",
			vecs:       makeVectors(float32(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test90",
			vecs:       makeVectors(float32(125), types.T_float64, true),
			proc:       procs,
			wantValues: []float64{125},
			wantScalar: true,
		},
		{
			name:       "Test91",
			vecs:       makeVectors(float64(125), types.T_int8, true),
			proc:       procs,
			wantValues: []int8{125},
			wantScalar: true,
		},
		{
			name:       "Test92",
			vecs:       makeVectors(float64(125), types.T_int16, true),
			proc:       procs,
			wantValues: []int16{125},
			wantScalar: true,
		},
		{
			name:       "Test93",
			vecs:       makeVectors(float64(125), types.T_int32, true),
			proc:       procs,
			wantValues: []int32{125},
			wantScalar: true,
		},
		{
			name:       "Test94",
			vecs:       makeVectors(float64(125), types.T_int64, true),
			proc:       procs,
			wantValues: []int64{125},
			wantScalar: true,
		},
		{
			name:       "Test95",
			vecs:       makeVectors(float64(125), types.T_uint8, true),
			proc:       procs,
			wantValues: []uint8{125},
			wantScalar: true,
		},
		{
			name:       "Test96",
			vecs:       makeVectors(float64(125), types.T_uint16, true),
			proc:       procs,
			wantValues: []uint16{125},
			wantScalar: true,
		},
		{
			name:       "Test97",
			vecs:       makeVectors(float64(125), types.T_uint32, true),
			proc:       procs,
			wantValues: []uint32{125},
			wantScalar: true,
		},
		{
			name:       "Test98",
			vecs:       makeVectors(float64(125), types.T_uint64, true),
			proc:       procs,
			wantValues: []uint64{125},
			wantScalar: true,
		},
		{
			name:       "Test99",
			vecs:       makeVectors(float64(125), types.T_float32, true),
			proc:       procs,
			wantValues: []float32{125},
			wantScalar: true,
		},
		{
			name:       "Test100",
			vecs:       makeVectors(float64(125), types.T_float64, true),
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

func makeVectors(src interface{}, destType types.T, isSrcConst bool) []*vector.Vector {
	vectors := make([]*vector.Vector, 2)
	vectors[0] = makeVector(src, true)
	vectors[1] = makeTypeVector(destType)
	return vectors
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
		IsConst: true,
		Length:  10,
	}
}
