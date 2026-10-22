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
	"bytes"
	"math"
	"sort"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestTupleLowPrecFloatBits checks that a bf16, float16, float8 or float4 tuple element
// keeps the stored bits through Unpack, UnpackNthElement and StringifyTuple, and that the
// encoded bytes order as the values do.
func TestTupleLowPrecFloatBits(t *testing.T) {
	type elem struct {
		value any
		f     float32
	}
	var cases [][]elem
	var f8, f4, b16, h16 []elem
	for c := 0; c < 256; c++ {
		if v := Float8(c); !math.IsNaN(float64(v.ToFloat32())) && c != 0x80 {
			f8 = append(f8, elem{v, v.ToFloat32()})
		}
	}
	for c := 0; c < 16; c++ {
		if c != 0x08 {
			f4 = append(f4, elem{Float4(c), Float4(c).ToFloat32()})
		}
	}
	for _, v := range []float32{-BF16MaxFinite, -65504, -1.5, -0.30078125, 0, 1e-38, 0.5, 3, BF16MaxFinite} {
		b16 = append(b16, elem{BF16FromFloat32(v), BF16FromFloat32(v).ToFloat32()})
	}
	for _, v := range []float32{-65504, -2, -6.1e-5, 0, 5.96e-8, 0.099975586, 1, 65504} {
		h16 = append(h16, elem{Float16FromFloat32(v), Float16FromFloat32(v).ToFloat32()})
	}
	cases = append(cases, f8, f4, b16, h16)
	pack := func(v any) []byte {
		p := NewPacker()
		defer p.Close()
		switch v := v.(type) {
		case BF16:
			p.EncodeBF16(v)
		case Float16:
			p.EncodeFloat16(v)
		case Float8:
			p.EncodeFloat8(v)
		case Float4:
			p.EncodeFloat4(v)
		}
		p.EncodeInt64(7)
		return append([]byte(nil), p.GetBuf()...)
	}
	for _, elems := range cases {
		encoded := make([][]byte, len(elems))
		for i, e := range elems {
			b := pack(e.value)
			encoded[i] = b
			tuple, schema, err := UnpackWithSchema(b)
			require.NoError(t, err)
			require.Equal(t, e.value, tuple[0])
			require.Equal(t, int64(7), tuple[1])
			nth, typ, err := UnpackNthElement(b, 0)
			require.NoError(t, err)
			require.Equal(t, e.value, nth)
			require.Equal(t, schema[0], typ)
			require.Equal(t, lowPrecTupleElemType(e.value), typ)
			strs, err := StringifyTuple(b, []plan.Type{{Id: int32(typ)}, {Id: int32(T_int64)}})
			require.NoError(t, err)
			require.Equal(t, strconv.FormatFloat(float64(e.f), 'g', -1, 32), strs[0])
			require.Equal(t, "7", strs[1])
		}
		order := make([]int, len(elems))
		for i := range order {
			order[i] = i
		}
		sort.SliceStable(order, func(a, b int) bool { return bytes.Compare(encoded[order[a]], encoded[order[b]]) < 0 })
		for i := 1; i < len(order); i++ {
			require.Less(t, elems[order[i-1]].f, elems[order[i]].f, "%v before %v", elems[order[i-1]].value, elems[order[i]].value)
		}
	}
}

func lowPrecTupleElemType(v any) T {
	switch v.(type) {
	case BF16:
		return T_bf16
	case Float16:
		return T_float16
	case Float8:
		return T_float8
	}
	return T_float4
}
