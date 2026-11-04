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

package ivfflat

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
	"github.com/stretchr/testify/require"
)

func requireCode(t *testing.T, code uint16, err error) {
	t.Helper()
	if code == 0 {
		require.NoError(t, err)
		return
	}
	require.True(t, moerr.IsMoErrCode(err, code), "%v", err)
}

func TestStorageCheck(t *testing.T) {
	f32 := func(v ...float32) []byte { return types.ArrayToBytes(v) }
	f64 := func(v ...float64) []byte { return types.ArrayToBytes(v) }
	bf16 := func(v ...float32) []byte { return types.ArrayToBytes(types.Float32ToBF16Slice(v)) }
	big := float32(math.Sqrt(MaxSquaredNormF32)) // its square is the norm limit
	for _, c := range []struct {
		name  string
		s     Storage
		vec   []byte
		code  uint16
		check string
	}{
		{"f32 plain", NewStorage(types.T_array_float32, "", 0, 0, false), f32(1, 2, 3), 0, ""},
		{"f32 empty", NewStorage(types.T_array_float32, "", 0, 0, false), f32(), moerr.ErrIndexVectorDimension, ""},
		{"f32 at the norm limit", NewStorage(types.T_array_float32, "", 0, 0, false), f32(big * 0.99), 0, ""},
		{"f32 past the norm limit", NewStorage(types.T_array_float32, "", 0, 0, false), f32(big, big), moerr.ErrIndexVectorOutOfRange, "squared L2 norm"},
		{"f32 to float16 in range", NewStorage(types.T_array_float32, "float16", 0, 0, false), f32(65504), 0, ""},
		{"f32 to float16 past range", NewStorage(types.T_array_float32, "float16", 0, 0, false), f32(65520), moerr.ErrIndexVectorOutOfRange, "vecf16"},
		{"f32 to float16 negative", NewStorage(types.T_array_float32, "float16", 0, 0, false), f32(-70000), moerr.ErrIndexVectorOutOfRange, "vecf16"},
		{"f32 to bf16 past range", NewStorage(types.T_array_float32, "bf16", 0, 0, false), f32(3.4e38), moerr.ErrIndexVectorOutOfRange, "vecbf16"},
		{"bf16 to float16 past range", NewStorage(types.T_array_bf16, "float16", 0, 0, false), bf16(1e6), moerr.ErrIndexVectorOutOfRange, "vecf16"},
		{"f64 plain index keeps f64 distances", NewStorage(types.T_array_float64, "", 0, 0, false), f64(1e30, 1e30), 0, ""},
		{"f64 plain index past the f64 norm", NewStorage(types.T_array_float64, "", 0, 0, false), f64(1e154, 1e154), moerr.ErrIndexVectorOutOfRange, "squared L2 norm"},
		{"f64 quantized narrows to f32", NewStorage(types.T_array_float64, "float32", 0, 0, false), f64(1e39), moerr.ErrIndexVectorOutOfRange, "float32"},
		{"f64 to int8 in range", NewStorage(types.T_array_float64, "int8", -1, 1, true), f64(0.5), 0, ""},
		{"int8 scale overflows", NewStorage(types.T_array_float32, "int8", 0, 1e-37, true), f32(1e30), moerr.ErrIndexVectorOutOfRange, "vecint8"},
		{"uint8 scale overflows", NewStorage(types.T_array_float32, "uint8", 0, 1e-37, true), f32(1e30), moerr.ErrIndexVectorOutOfRange, "vecuint8"},
		{"f32 NaN", NewStorage(types.T_array_float32, "", 0, 0, false), f32(1, float32(math.NaN())), moerr.ErrIndexVectorNonFinite, ""},
		{"f32 infinity", NewStorage(types.T_array_float32, "float16", 0, 0, false), f32(float32(math.Inf(-1))), moerr.ErrIndexVectorNonFinite, ""},
		{"f64 NaN", NewStorage(types.T_array_float64, "", 0, 0, false), f64(math.NaN()), moerr.ErrIndexVectorNonFinite, ""},
		{"f64 infinity narrowed", NewStorage(types.T_array_float64, "float32", 0, 0, false), f64(math.Inf(1)), moerr.ErrIndexVectorNonFinite, ""},
		{"int8 base is stored as it is", NewStorage(types.T_array_int8, "", 0, 0, false), types.ArrayToBytes([]int8{127, -128}), 0, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := c.s.Check(c.vec)
			requireCode(t, c.code, err)
			if c.check != "" {
				require.Contains(t, err.Error(), c.check)
			}
		})
	}
}

// The int8 rule rejects exactly the elements whose entry map, as the entry SQL computes it,
// is not finite.
func TestStorageCheckMatchesInt8Map(t *testing.T) {
	for _, q := range []string{"int8", "uint8"} {
		mul, add := quantizer.Int8Params(0, 1e-37)
		if q == "uint8" {
			mul, add = quantizer.Uint8Params(0, 1e-37)
		}
		s := NewStorage(types.T_array_float32, q, 0, 1e-37, true)
		for _, x := range []float32{0, 1, 1e-30, 1e10, 1e20, 1e30, -1e30} {
			code := float32(x*float32(mul)) + float32(add)
			finite := !math.IsInf(float64(code), 0) && !math.IsNaN(float64(code))
			err := s.Check(types.ArrayToBytes([]float32{x}))
			require.Equal(t, finite, err == nil, "%s x=%v code=%v err=%v", q, x, code, err)
		}
	}
}

func TestNewStorage(t *testing.T) {
	s := NewStorage(types.T_array_float64, "", 0, 0, false)
	require.False(t, s.F32)
	require.Equal(t, types.T_array_float64, s.Entry)
	s = NewStorage(types.T_array_float64, "float16", 0, 0, false)
	require.True(t, s.F32)
	require.Equal(t, types.T_array_float16, s.Entry)
	s = NewStorage(types.T_array_float32, "int8", 0, 0, false)
	require.Equal(t, float64(1), s.Mul, "no bounds: identity map")
	mul, add := quantizer.Uint8Params(-2, 2)
	s = NewStorage(types.T_array_float32, "uint8", -2, 2, true)
	require.Equal(t, mul, s.Mul)
	require.Equal(t, add, s.Add)
}

func TestCheckQuery(t *testing.T) {
	s := Storage{F32: true, Entry: types.T_array_float16, Mul: 1}
	require.NoError(t, CheckQuery(s, []float32{1, 2}))
	requireCode(t, moerr.ErrIndexVectorOutOfRange, CheckQuery(s, []float32{70000}))
	s = Storage{Entry: types.T_array_float64, Mul: 1}
	require.NoError(t, CheckQuery(s, []float64{1e30}))
	requireCode(t, moerr.ErrIndexVectorOutOfRange, CheckQuery(s, []float64{1e154, 1e154}))
}
