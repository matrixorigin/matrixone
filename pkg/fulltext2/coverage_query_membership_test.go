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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

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
