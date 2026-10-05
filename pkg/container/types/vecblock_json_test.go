// Copyright 2026 Matrix Origin
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

package types

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBlockScaledJSONIdentity checks that the exact text rebuilds the same cell bytes, for
// cells of many magnitudes and dimensions, and that the decoded-value text does not: it is
// quantized again and can store other values.
func TestBlockScaledJSONIdentity(t *testing.T) {
	r := rand.New(rand.NewSource(29554))
	var vectors [][]float32
	for _, dim := range []int{1, 15, 16, 17, 31, 32, 33, 70} {
		for _, scale := range []float64{1e-30, 1e-3, 1, 1e3, 1e30} {
			v := make([]float32, dim)
			for i := range v {
				v[i] = float32(r.NormFloat64() * scale)
			}
			vectors = append(vectors, v)
		}
	}
	vectors = append(vectors, make([]float32, 40)) // all zero
	review := make([]float32, 17)                  // only elements 0 and 16 nonzero
	review[0], review[16] = 8.7649145, 5.7432985
	vectors = append(vectors, review)
	for _, f := range []BlockScaledFormat{BlockScaledMXFP8, BlockScaledNVFP4} {
		for i, v := range vectors {
			cell, err := AppendBlockScaled(nil, f, v)
			require.NoError(t, err)
			text, err := BlockScaledToJSON(cell)
			require.NoError(t, err)
			require.True(t, IsBlockScaledJSON(text))
			back, err := BlockScaledFromJSON(f, text)
			require.NoError(t, err, "%s vector %d: %s", f, i, text)
			require.Equal(t, cell, back, "%s vector %d: %s", f, i, text)
			again, err := StringToBlockScaled(f, " \n"+text)
			require.NoError(t, err)
			require.Equal(t, cell, again)
		}
	}
	// the decoded-value text is quantized again: vecf4's global scale follows the decoded
	// maximum, and element 16 moves from 6.2606535 to 6.8867183
	cell, err := AppendBlockScaled(nil, BlockScaledNVFP4, review)
	require.NoError(t, err)
	decoded, err := BlockScaledToString(cell)
	require.NoError(t, err)
	requantized, err := StringToBlockScaled(BlockScaledNVFP4, decoded)
	require.NoError(t, err)
	v1, _ := BlockScaledToFloat32(cell)
	v2, _ := BlockScaledToFloat32(requantized)
	require.NotEqual(t, v1[16], v2[16])
	text, err := BlockScaledToJSON(cell)
	require.NoError(t, err)
	exact, err := StringToBlockScaled(BlockScaledNVFP4, text)
	require.NoError(t, err)
	require.Equal(t, cell, exact)
}

func TestBlockScaledJSONForm(t *testing.T) {
	cell, err := AppendBlockScaled(nil, BlockScaledNVFP4, []float32{1, -3, 0, 6, 0.5})
	require.NoError(t, err)
	text, err := BlockScaledToJSON(cell)
	require.NoError(t, err)
	// the global scale is amax / (6 * 448); the block scale 448 and the codes decode to the input
	require.Equal(t, `{"g":0.002232143,"b":[{"s":448,"v":[1,-3,0,6,0.5]}]}`, text)
	cell, err = AppendBlockScaled(nil, BlockScaledMXFP8, []float32{1, 2})
	require.NoError(t, err)
	text, err = BlockScaledToJSON(cell)
	require.NoError(t, err)
	require.Equal(t, `{"b":[{"s":0.0078125,"v":[128,256]}]}`, text)
}

func TestBlockScaledJSONRejects(t *testing.T) {
	block32 := `[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]`
	for _, tc := range []struct {
		f    BlockScaledFormat
		text string
	}{
		{BlockScaledNVFP4, `{"b":[{"s":1,"v":[1]}]}`},                                 // no global
		{BlockScaledMXFP8, `{"g":2,"b":[{"s":1,"v":[1]}]}`},                           // vecf8 global is 1
		{BlockScaledMXFP8, `{"b":[]}`},                                                // no block
		{BlockScaledMXFP8, `{"b":[{"v":[1]}]}`},                                       // no scale
		{BlockScaledMXFP8, `{"b":[{"s":3,"v":[1]}]}`},                                 // scale not a power of two
		{BlockScaledNVFP4, `{"g":1,"b":[{"s":-1,"v":[1]}]}`},                          // negative scale
		{BlockScaledNVFP4, `{"g":1,"b":[{"s":1,"v":[2.5]}]}`},                         // not an E2M1 value
		{BlockScaledMXFP8, `{"b":[{"s":1,"v":[1.0625]}]}`},                            // not an E4M3 value
		{BlockScaledMXFP8, `{"b":[{"s":1,"v":[1]},{"s":1,"v":[1]}]}`},                 // a short block before the last
		{BlockScaledMXFP8, `{"b":[{"s":1,"v":` + block32[:len(block32)-1] + `,1]}]}`}, // 33 values
		{BlockScaledMXFP8, `{"b":[{"s":1,"v":[]}]}`},                                  // empty block
		{BlockScaledMXFP8, `{"b":[{"s":1,"v":[1]}],"x":1}`},                           // unknown key
		{BlockScaledMXFP8, `{"b":[{"s":1,"v":[1]}]} x`},                               // trailing text
		{BlockScaledMXFP8, `{"b":[{"s":1,"v":["1"]}]}`},                               // not a number
		{BlockScaledNVFP4, `{"g":1e38,"b":[{"s":448,"v":[6]}]}`},                      // decodes to infinity
		{BlockScaledNVFP4, `{"g":1e39,"b":[{"s":1,"v":[1]}]}`},                        // not a finite float32
	} {
		_, err := BlockScaledFromJSON(tc.f, tc.text)
		require.Error(t, err, "%s %s", tc.f, tc.text)
	}
	// -0 is stored as code 0
	a, err := BlockScaledFromJSON(BlockScaledMXFP8, `{"b":[{"s":1,"v":[-0,2]}]}`)
	require.NoError(t, err)
	b, err := BlockScaledFromJSON(BlockScaledMXFP8, `{"b":[{"s":1,"v":[0,2]}]}`)
	require.NoError(t, err)
	require.Equal(t, a, b)
}
