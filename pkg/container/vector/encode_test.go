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

package vector

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// encodeRowViaPerRowAPI encodes one row through the single-row entry point.
func encodeRowViaPerRowAPI(t *testing.T, vec *Vector, row int) []byte {
	p := types.NewPacker()
	defer p.Close()
	require.NoError(t, EncodeRowToPacker(p, vec, row))
	return append([]byte(nil), p.Bytes()...)
}

// TestNewRowEncoderMatchesEncodeRowToPacker pins the vectorization contract:
// a bound RowEncoder must produce byte-identical output to the per-row
// dispatch of EncodeRowToPacker for every type arm, so existing encoded keys
// (data-branch hash keys, mo_data_branch_update_link rows) stay comparable.
func TestNewRowEncoderMatchesEncodeRowToPacker(t *testing.T) {
	mp := mpool.MustNewZero()

	var tsVal types.TS
	for i := range tsVal {
		tsVal[i] = byte(i + 1)
	}
	var uuidVal types.Uuid
	for i := range uuidVal {
		uuidVal[i] = byte(0xa0 + i)
	}

	cases := []struct {
		name  string
		build func(t *testing.T) *Vector
	}{
		{"bool", func(t *testing.T) *Vector {
			v := NewVec(types.T_bool.ToType())
			require.NoError(t, AppendFixedList(v, []bool{true, false, true}, nil, mp))
			return v
		}},
		{"int8", func(t *testing.T) *Vector {
			v := NewVec(types.T_int8.ToType())
			require.NoError(t, AppendFixedList(v, []int8{-8, 0, 8}, nil, mp))
			return v
		}},
		{"int16", func(t *testing.T) *Vector {
			v := NewVec(types.T_int16.ToType())
			require.NoError(t, AppendFixedList(v, []int16{-16, 0, 16}, nil, mp))
			return v
		}},
		{"int32", func(t *testing.T) *Vector {
			v := NewVec(types.T_int32.ToType())
			require.NoError(t, AppendFixedList(v, []int32{-32, 0, 32}, nil, mp))
			return v
		}},
		{"int64", func(t *testing.T) *Vector {
			v := NewVec(types.T_int64.ToType())
			require.NoError(t, AppendFixedList(v, []int64{-64, 0, 64}, nil, mp))
			return v
		}},
		{"uint8", func(t *testing.T) *Vector {
			v := NewVec(types.T_uint8.ToType())
			require.NoError(t, AppendFixedList(v, []uint8{0, 8, 255}, nil, mp))
			return v
		}},
		{"uint16", func(t *testing.T) *Vector {
			v := NewVec(types.T_uint16.ToType())
			require.NoError(t, AppendFixedList(v, []uint16{0, 16, 65535}, nil, mp))
			return v
		}},
		{"uint32", func(t *testing.T) *Vector {
			v := NewVec(types.T_uint32.ToType())
			require.NoError(t, AppendFixedList(v, []uint32{0, 32, 1 << 31}, nil, mp))
			return v
		}},
		{"uint64", func(t *testing.T) *Vector {
			v := NewVec(types.T_uint64.ToType())
			require.NoError(t, AppendFixedList(v, []uint64{0, 64, 1 << 63}, nil, mp))
			return v
		}},
		{"float32", func(t *testing.T) *Vector {
			v := NewVec(types.T_float32.ToType())
			require.NoError(t, AppendFixedList(v, []float32{-1.5, 0, 3.25}, nil, mp))
			return v
		}},
		{"float64", func(t *testing.T) *Vector {
			v := NewVec(types.T_float64.ToType())
			require.NoError(t, AppendFixedList(v, []float64{-1.5, 0, 3.25}, nil, mp))
			return v
		}},
		{"date", func(t *testing.T) *Vector {
			v := NewVec(types.T_date.ToType())
			require.NoError(t, AppendFixedList(v, []types.Date{0, 365, 365242}, nil, mp))
			return v
		}},
		{"time", func(t *testing.T) *Vector {
			v := NewVec(types.T_time.ToType())
			require.NoError(t, AppendFixedList(v, []types.Time{0, 123456789, -42}, nil, mp))
			return v
		}},
		{"datetime", func(t *testing.T) *Vector {
			v := NewVec(types.T_datetime.ToType())
			require.NoError(t, AppendFixedList(v, []types.Datetime{0, 9876543210, -7}, nil, mp))
			return v
		}},
		{"timestamp", func(t *testing.T) *Vector {
			v := NewVec(types.T_timestamp.ToType())
			require.NoError(t, AppendFixedList(v, []types.Timestamp{0, 1234567890, -9}, nil, mp))
			return v
		}},
		{"year", func(t *testing.T) *Vector {
			v := NewVec(types.T_year.ToType())
			require.NoError(t, AppendFixedList(v, []types.MoYear{1901, 2000, 2155}, nil, mp))
			return v
		}},
		{"decimal64", func(t *testing.T) *Vector {
			v := NewVec(types.T_decimal64.ToType())
			require.NoError(t, AppendFixedList(v,
				[]types.Decimal64{0, 123456789012345, 99}, nil, mp))
			return v
		}},
		{"decimal128", func(t *testing.T) *Vector {
			v := NewVec(types.T_decimal128.ToType())
			require.NoError(t, AppendFixedList(v, []types.Decimal128{
				{B0_63: 1, B64_127: 2},
				{B0_63: ^uint64(0), B64_127: 0},
				{},
			}, nil, mp))
			return v
		}},
		{"decimal256", func(t *testing.T) *Vector {
			v := NewVec(types.T_decimal256.ToType())
			require.NoError(t, AppendFixedList(v, []types.Decimal256{
				{B0_63: 1, B64_127: 2, B128_191: 3, B192_255: 4},
				{B0_63: ^uint64(0)},
				{},
			}, nil, mp))
			return v
		}},
		{"uuid", func(t *testing.T) *Vector {
			v := NewVec(types.T_uuid.ToType())
			var u2 types.Uuid
			copy(u2[:], uuidVal[:])
			u2[0] ^= 0xff
			require.NoError(t, AppendFixedList(v, []types.Uuid{uuidVal, u2, {}}, nil, mp))
			return v
		}},
		{"bit", func(t *testing.T) *Vector {
			v := NewVec(types.New(types.T_bit, 10, 0))
			require.NoError(t, AppendFixedList(v, []uint64{0, 5, 1023}, nil, mp))
			return v
		}},
		{"enum", func(t *testing.T) *Vector {
			v := NewVec(types.New(types.T_enum, 0, 0))
			require.NoError(t, AppendFixedList(v, []types.Enum{0, 1, 65535}, nil, mp))
			return v
		}},
		{"varchar", func(t *testing.T) *Vector {
			v := NewVec(types.T_varchar.ToType())
			require.NoError(t, AppendBytesList(v,
				[][]byte{[]byte(""), []byte("alpha"), []byte("a longer key value")}, nil, mp))
			return v
		}},
		{"varbinary", func(t *testing.T) *Vector {
			v := NewVec(types.T_varbinary.ToType())
			require.NoError(t, AppendBytesList(v,
				[][]byte{{0x00}, {0xde, 0xad, 0xbe, 0xef}, {}}, nil, mp))
			return v
		}},
		{"ts", func(t *testing.T) *Vector {
			v := NewVec(types.T_TS.ToType())
			var zero types.TS
			require.NoError(t, AppendFixedList(v, []types.TS{tsVal, zero, tsVal}, nil, mp))
			return v
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vec := tc.build(t)
			defer vec.Free(mp)

			enc := NewRowEncoder(vec)
			p := types.NewPacker()
			defer p.Close()
			for row := 0; row < vec.Length(); row++ {
				want := encodeRowViaPerRowAPI(t, vec, row)
				p.Reset()
				enc(p, row)
				require.Equal(t, want, append([]byte(nil), p.Bytes()...),
					"row %d: bound encoder diverged from EncodeRowToPacker", row)
			}
		})
	}
}

// TestNewRowEncoderConstVector: a constant vector repeats row 0's payload at
// every logical row, and the bound encoder must agree with the per-row API
// (which clamps to row 0) instead of indexing past the length-1 backing.
func TestNewRowEncoderConstVector(t *testing.T) {
	mp := mpool.MustNewZero()

	vec, err := NewConstFixed(types.T_int64.ToType(), int64(42), 8, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	want := encodeRowViaPerRowAPI(t, vec, 0)
	enc := NewRowEncoder(vec)
	p := types.NewPacker()
	defer p.Close()
	for _, row := range []int{0, 3, 7} {
		p.Reset()
		enc(p, row)
		require.Equal(t, want, append([]byte(nil), p.Bytes()...),
			"const row %d must encode row 0's payload", row)
	}

	svec, err := NewConstBytes(types.T_varchar.ToType(), []byte("const key"), 4, mp)
	require.NoError(t, err)
	defer svec.Free(mp)

	swant := encodeRowViaPerRowAPI(t, svec, 0)
	senc := NewRowEncoder(svec)
	for _, row := range []int{0, 2, 3} {
		p.Reset()
		senc(p, row)
		require.Equal(t, swant, append([]byte(nil), p.Bytes()...),
			"const varlena row %d must encode row 0's payload", row)
	}
}

// BenchmarkRowEncoderBoundVsPerRow measures the dispatch hoisting the
// vectorized API provides: one type resolution per vector against one per row.
func BenchmarkRowEncoderBoundVsPerRow(b *testing.B) {
	mp := mpool.MustNewZero()
	const rows = 8192
	vec := NewVec(types.T_int64.ToType())
	defer vec.Free(mp)
	vals := make([]int64, rows)
	for i := range vals {
		vals[i] = int64(i)*31 - 17
	}
	require.NoError(b, AppendFixedList(vec, vals, nil, mp))

	b.Run("bound-encoder", func(b *testing.B) {
		p := types.NewPacker()
		defer p.Close()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			enc := NewRowEncoder(vec)
			for row := 0; row < rows; row++ {
				p.Reset()
				enc(p, row)
			}
		}
	})

	b.Run("per-row-dispatch", func(b *testing.B) {
		p := types.NewPacker()
		defer p.Close()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for row := 0; row < rows; row++ {
				p.Reset()
				_ = EncodeRowToPacker(p, vec, row)
			}
		}
	})
}
