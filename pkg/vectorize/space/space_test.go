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
