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

package fulltext2

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// recordingMembershipFilter makes the loaded zero-copy probe observable without
// relying on a probabilistic Bloom-filter hit as a byte-equivalence oracle.
type recordingMembershipFilter struct {
	probes [][]byte
}

func (f *recordingMembershipFilter) Test(data []byte) bool {
	probe := make([]byte, len(data))
	copy(probe, data)
	f.probes = append(f.probes, probe)
	return true
}

func (f *recordingMembershipFilter) TestVector(*vector.Vector, func(bool, bool, int)) []uint8 {
	return nil
}

func (f *recordingMembershipFilter) Valid() bool { return true }

func (f *recordingMembershipFilter) Exact() bool { return true }

func (f *recordingMembershipFilter) Free() {}

func (f *recordingMembershipFilter) Share() docfilter.MembershipFilter { return f }

func sourcePkBytes(v *vector.Vector, typ types.Type) []byte {
	if typ.IsFixedLen() {
		return v.GetData()[:typ.TypeSize()]
	}
	return v.GetBytesAt(0)
}

// TestContainsPkTypes exercises the fast per-PK-type encode branches of docFilterMembership.Contains
// (int64 is covered elsewhere): the uint64 / int32 / uint32 arms must encode byte-identically to the
// docfilter build so even pks pass and odd pks reject, and an out-of-range ord returns false.
func TestContainsPkTypes(t *testing.T) {
	mp := mpool.MustNewZero()

	// uint64
	t.Run("uint64", func(t *testing.T) {
		b := NewBuilder("u64", int32(types.T_uint64))
		for i := 0; i < 6; i++ {
			feed(t, b, uint64(i), "x")
		}
		seg, err := b.Finish()
		require.NoError(t, err)
		vec := vector.NewVec(types.New(types.T_uint64, 8, 0))
		for i := 0; i < 6; i += 2 {
			require.NoError(t, vector.AppendFixed(vec, uint64(i), false, mp))
		}
		fb, err := docfilter.Build(vec)
		require.NoError(t, err)
		f, err := docfilter.New(fb)
		require.NoError(t, err)
		defer f.Free()
		dfm := &docFilterMembership{seg: seg, f: f}
		for i := int64(0); i < 6; i++ {
			require.Equalf(t, i%2 == 0, dfm.Contains(i), "ord %d", i)
		}
		require.False(t, dfm.Contains(-1))           // out of range low
		require.False(t, dfm.Contains(int64(1<<40))) // out of range high
	})

	// int32
	t.Run("int32", func(t *testing.T) {
		b := NewBuilder("i32", int32(types.T_int32))
		for i := 0; i < 6; i++ {
			feed(t, b, int32(i), "x")
		}
		seg, err := b.Finish()
		require.NoError(t, err)
		vec := vector.NewVec(types.New(types.T_int32, 4, 0))
		for i := 0; i < 6; i += 2 {
			require.NoError(t, vector.AppendFixed(vec, int32(i), false, mp))
		}
		fb, err := docfilter.Build(vec)
		require.NoError(t, err)
		f, err := docfilter.New(fb)
		require.NoError(t, err)
		defer f.Free()
		dfm := &docFilterMembership{seg: seg, f: f}
		for i := int64(0); i < 6; i++ {
			require.Equalf(t, i%2 == 0, dfm.Contains(i), "ord %d", i)
		}
	})

	// uint32
	t.Run("uint32", func(t *testing.T) {
		b := NewBuilder("u32", int32(types.T_uint32))
		for i := 0; i < 6; i++ {
			feed(t, b, uint32(i), "x")
		}
		seg, err := b.Finish()
		require.NoError(t, err)
		vec := vector.NewVec(types.New(types.T_uint32, 4, 0))
		for i := 0; i < 6; i += 2 {
			require.NoError(t, vector.AppendFixed(vec, uint32(i), false, mp))
		}
		fb, err := docfilter.Build(vec)
		require.NoError(t, err)
		f, err := docfilter.New(fb)
		require.NoError(t, err)
		defer f.Free()
		dfm := &docFilterMembership{seg: seg, f: f}
		for i := int64(0); i < 6; i++ {
			require.Equalf(t, i%2 == 0, dfm.Contains(i), "ord %d", i)
		}
	})

	// uuid: probed as the RAW 16 bytes (not the canonical string) — the dedicated arm.
	t.Run("uuid", func(t *testing.T) {
		b := NewBuilder("uuid", int32(types.T_uuid))
		mkUuid := func(i int) types.Uuid { var u types.Uuid; u[0] = byte(i); return u }
		for i := 0; i < 6; i++ {
			feed(t, b, mkUuid(i), "x")
		}
		seg, err := b.Finish()
		require.NoError(t, err)
		vec := vector.NewVec(types.New(types.T_uuid, 16, 0))
		for i := 0; i < 6; i += 2 {
			require.NoError(t, vector.AppendFixed(vec, mkUuid(i), false, mp))
		}
		fb, err := docfilter.Build(vec)
		require.NoError(t, err)
		f, err := docfilter.New(fb)
		require.NoError(t, err)
		defer f.Free()
		dfm := &docFilterMembership{seg: seg, f: f}
		for i := int64(0); i < 6; i++ {
			require.Equalf(t, i%2 == 0, dfm.Contains(i), "ord %d", i)
		}
	})

	// varchar: the default arm (encodePk), not one of the fast integer/uuid cases.
	t.Run("varchar", func(t *testing.T) {
		b := NewBuilder("vc", int32(types.T_varchar))
		keys := []string{"k0", "k1", "k2", "k3", "k4", "k5"}
		for _, k := range keys {
			feed(t, b, k, "x")
		}
		seg, err := b.Finish()
		require.NoError(t, err)
		vec := vector.NewVec(types.New(types.T_varchar, 64, 0))
		for i := 0; i < 6; i += 2 {
			require.NoError(t, vector.AppendBytes(vec, []byte(keys[i]), false, mp))
		}
		fb, err := docfilter.Build(vec)
		require.NoError(t, err)
		f, err := docfilter.New(fb)
		require.NoError(t, err)
		defer f.Free()
		dfm := &docFilterMembership{seg: seg, f: f}
		for i := int64(0); i < 6; i++ {
			require.Equalf(t, i%2 == 0, dfm.Contains(i), "ord %d", i)
		}
	})
}

// TestLoadedContainsPkTypes proves the loaded-docmap fast path probes bytes
// identical to docfilter.Build's source-vector representation. UUID deliberately
// uses the typed fallback because its docmap stores the canonical string.
func TestLoadedContainsPkTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	u, err := types.ParseUuid("12345678-1234-1234-1234-1234567890ab")
	require.NoError(t, err)
	cases := []struct {
		name string
		typ  types.T
		val  any
	}{
		{"int8", types.T_int8, int8(-8)},
		{"int16", types.T_int16, int16(-1600)},
		{"int32", types.T_int32, int32(-320000)},
		{"int64", types.T_int64, int64(-64000000)},
		{"uint8", types.T_uint8, uint8(8)},
		{"uint16", types.T_uint16, uint16(1600)},
		{"uint32", types.T_uint32, uint32(320000)},
		{"uint64", types.T_uint64, uint64(64000000)},
		{"bit", types.T_bit, uint64(7)},
		{"date", types.T_date, types.Date(20260)},
		{"datetime", types.T_datetime, types.Datetime(1234567)},
		{"time", types.T_time, types.Time(7654321)},
		{"timestamp", types.T_timestamp, types.Timestamp(9876543)},
		{"decimal64", types.T_decimal64, types.Decimal64(12345)},
		{"decimal128", types.T_decimal128, types.Decimal128{B0_63: 12, B64_127: 34}},
		{"char", types.T_char, []byte("char-key")},
		{"varchar", types.T_varchar, []byte("varchar-key")},
		{"text", types.T_text, []byte("text-key")},
		{"binary", types.T_binary, []byte{0, 1, 2}},
		{"varbinary", types.T_varbinary, []byte{3, 4, 5}},
		{"blob", types.T_blob, []byte{6, 7, 8}},
		{"json", types.T_json, []byte(`{"k":1}`)},
		{"datalink", types.T_datalink, []byte("file://pk")},
		{"uuid", types.T_uuid, u},
		{"varchar-empty", types.T_varchar, []byte{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBuilder("loaded-membership", int32(tc.typ))
			feed(t, b, tc.val, "x")
			seg, err := b.Finish()
			require.NoError(t, err)
			blob, err := seg.Serialize()
			require.NoError(t, err)
			loaded, err := Deserialize("loaded-membership", bytes.NewReader(blob))
			require.NoError(t, err)
			t.Cleanup(func() { _ = loaded.dict.Close() })

			vec := vector.NewVec(tc.typ.ToType())
			require.NoError(t, vector.AppendAny(vec, tc.val, false, mp))
			t.Cleanup(func() { vec.Free(mp) })
			payload, err := docfilter.Build(vec)
			require.NoError(t, err)
			f, err := docfilter.New(payload)
			require.NoError(t, err)
			t.Cleanup(f.Free)

			allow := &docFilterMembership{seg: loaded, f: f}
			require.True(t, allow.Contains(0))
			require.False(t, allow.Contains(-1))
			require.False(t, allow.Contains(1))

			// The real docfilter remains the end-to-end membership check above. For
			// non-integer PKs it is a Bloom filter, so a successful probe alone cannot
			// prove that the zero-copy path passed the exact source bytes. Capture the
			// production probe and compare it byte-for-byte with the source vector;
			// UUID must receive raw 16-byte vector data, not canonical docmap text.
			capture := &recordingMembershipFilter{}
			captured := &docFilterMembership{seg: loaded, f: capture}
			require.True(t, captured.Contains(0))
			require.Len(t, capture.probes, 1)
			expected := sourcePkBytes(vec, tc.typ.ToType())
			require.Len(t, capture.probes[0], len(expected))
			require.True(t, bytes.Equal(expected, capture.probes[0]))
		})
	}
}

// TestIsJSONParser pins the json-family predicate.
func TestIsJSONParser(t *testing.T) {
	require.True(t, IsJSONParser(ParserJSON))
	require.True(t, IsJSONParser(ParserJSONValue))
	require.False(t, IsJSONParser(ParserDefault))
	require.False(t, IsJSONParser("ngram"))
	require.True(t, IsJSONValueParser(ParserJSONValue))
	require.False(t, IsJSONValueParser(ParserJSON))
}

// TestFlattenJSONColumns covers the input-type dispatch of the two CDC-side json flatteners for
// each accepted form (ByteJson, []byte, string) plus the default (non-json input → nil, nil).
func TestFlattenJSONColumns(t *testing.T) {
	raw := `{"a":"hello world","b":42}`
	bj, err := bytejson.ParseFromString(raw)
	require.NoError(t, err)

	for _, fn := range []func(any) ([]byte, error){FlattenJSONColumn, FlattenJSONValueColumn} {
		// ByteJson
		out, err := fn(bj)
		require.NoError(t, err)
		require.NotEmpty(t, out)
		// []byte
		out, err = fn([]byte(raw))
		require.NoError(t, err)
		require.NotEmpty(t, out)
		// string
		out, err = fn(raw)
		require.NoError(t, err)
		require.NotEmpty(t, out)
		// default (unsupported type) → nil, nil
		out, err = fn(12345)
		require.NoError(t, err)
		require.Nil(t, out)
	}
}

// TestCdcTokenizer covers the three arms: json_value (whole-value tokens), a normal parser (ngram
// words), and an unknown parser (DocTokenizer error propagated).
func TestCdcTokenizer(t *testing.T) {
	jv, err := CdcTokenizer(ParserJSONValue)
	require.NoError(t, err)
	require.NotNil(t, jv)

	def, err := CdcTokenizer(ParserDefault)
	require.NoError(t, err)
	require.NotNil(t, def)
	require.NotEmpty(t, def("hello world")) // tokenizes into words

	_, err = CdcTokenizer("no-such-parser")
	require.Error(t, err)
}
