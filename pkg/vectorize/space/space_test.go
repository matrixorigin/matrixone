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

package space

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestCountSpacesForUnsignedInt(t *testing.T) {
	require.Equal(t, 6, CountSpacesForUnsignedInt([]uint8{0, 1, 2, 3}))
	require.Equal(t, 6, CountSpacesForUnsignedInt([]uint16{0, 1, 2, 3}))
	require.Equal(t, 6, CountSpacesForUnsignedInt([]uint32{0, 1, 2, 3}))
	require.Equal(t, 6, CountSpacesForUnsignedInt([]uint64{0, 1, 2, 3}))
}

func TestCountSpacesForSignedInt(t *testing.T) {
	require.Equal(t, 6, CountSpacesForSignedInt([]int8{0, 1, 2, 3}))
	require.Equal(t, 6, CountSpacesForSignedInt([]int16{0, 1, 2, 3}))
	require.Equal(t, 6, CountSpacesForSignedInt([]int32{0, 1, 2, 3}))
	require.Equal(t, 6, CountSpacesForSignedInt([]int64{0, 1, 2, 3}))
}

func TestCountSpacesForFloat(t *testing.T) {
	require.Equal(t, 6, CountSpacesForFloat([]float32{0, 1.1, 1.5, 3}))
	require.Equal(t, 6, CountSpacesForFloat([]float64{0, 1.1, 1.5, 3}))
}

func TestParseStringAsInt64(t *testing.T) {
	cases := map[string]int64{
		"":     0,
		"0":    0,
		"1":    1,
		"1.1":  1,
		"1.5":  1,
		"-2":   0,
		" 1":   1,
		" 1.1": 1,
		"\t1":  1,
	}

	for input, expected := range cases {
		require.Equal(t, expected, parseStringAsInt64(input), input)
	}
}

func TestCountSpacesForCharVarChar(t *testing.T) {
	ss := []string{"1", "0", "1.1", "1.5", "-2", " 1", " 1.1", " \t1"}

	require.Equal(t, 6, CountSpacesForCharVarChar(encodeStringSliceToTypeBytes(ss)))
}

func TestFillSpacesUint8(t *testing.T) {
	cases := []uint8{0, 1, 2, 3}
	spacesCount := CountSpacesForUnsignedInt(cases)
	result := &types.Bytes{
		Data:    make([]byte, spacesCount),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}

	FillSpacesUint8(cases, result)
	require.Equal(t, spacesCount, uint64(len(result.Data)))
}

func TestFillSpacesUint16(t *testing.T) {
	cases := []uint16{0, 1, 2, 3}
	spacesCount := CountSpacesForUnsignedInt(cases)
	result := &types.Bytes{
		Data:    make([]byte, spacesCount),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}

	FillSpacesUint16(cases, result)
	require.Equal(t, spacesCount, uint64(len(result.Data)))
}

func TestFillSpacesUint32(t *testing.T) {
	cases := []uint32{0, 1, 2, 3}
	spacesCount := CountSpacesForUnsignedInt(cases)
	result := &types.Bytes{
		Data:    make([]byte, spacesCount),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}

	FillSpacesUint32(cases, result)
	require.Equal(t, spacesCount, uint64(len(result.Data)))
}

func TestFillSpacesUint64(t *testing.T) {
	cases := []uint64{0, 1, 2, 3}
	spacesCount := CountSpacesForUnsignedInt(cases)
	result := &types.Bytes{
		Data:    make([]byte, spacesCount),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}

	FillSpacesUint64(cases, result)
	require.Equal(t, spacesCount, uint64(len(result.Data)))
}

func TestFillSpacesInt8(t *testing.T) {
	cases := []int8{0, 1, 2, 3}
	spacesCount := CountSpacesForSignedInt(cases)
	result := &types.Bytes{
		Data:    make([]byte, spacesCount),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}

	FillSpacesInt8(cases, result)
	require.Equal(t, spacesCount, int64(len(result.Data)))
}

func TestFillSpacesInt16(t *testing.T) {
	cases := []int16{0, 1, 2, 3}
	spacesCount := CountSpacesForSignedInt(cases)
	result := &types.Bytes{
		Data:    make([]byte, spacesCount),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}

	FillSpacesInt16(cases, result)
	require.Equal(t, spacesCount, int64(len(result.Data)))
}

func TestFillSpacesInt32(t *testing.T) {
	cases := []int32{0, 1, 2, 3}
	spacesCount := CountSpacesForSignedInt(cases)
	result := &types.Bytes{
		Data:    make([]byte, spacesCount),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}

	FillSpacesInt32(cases, result)
	require.Equal(t, spacesCount, int64(len(result.Data)))
}

func TestFillSpacesInt64(t *testing.T) {
	cases := []int64{0, 1, 2, 3}
	spacesCount := CountSpacesForSignedInt(cases)
	result := &types.Bytes{
		Data:    make([]byte, spacesCount),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}

	FillSpacesInt64(cases, result)
	require.Equal(t, spacesCount, int64(len(result.Data)))
}

func TestFillSpacesFloat32(t *testing.T) {
	cases := []float32{0, 1.1, 1.5, 3}
	spacesCount := CountSpacesForFloat(cases)
	result := &types.Bytes{
		Data:    make([]byte, spacesCount),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}

	FillSpacesFloat32(cases, result)
	require.Equal(t, spacesCount, int64(len(result.Data)))
}

func TestFillSpacesFloat64(t *testing.T) {
	cases := []float64{0, 1.1, 1.5, 3}
	spacesCount := CountSpacesForFloat(cases)
	result := &types.Bytes{
		Data:    make([]byte, spacesCount),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}

	FillSpacesFloat64(cases, result)
	require.Equal(t, spacesCount, int64(len(result.Data)))
}

func TestFillSpacesCharVarChar(t *testing.T) {
	cases := []string{"1", "0", "1.1", "1.5", "-2", " 1", " 1.1", " \t1"}
	spacesCount := CountSpacesForCharVarChar(encodeStringSliceToTypeBytes(cases))
	result := &types.Bytes{
		Data:    make([]byte, spacesCount),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}

	FillSpacesCharVarChar(encodeStringSliceToTypeBytes(cases), result)
	require.Equal(t, spacesCount, int64(len(result.Data)))
}
