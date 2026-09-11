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
	"encoding/binary"
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
	probes     [][]byte
	probeViews [][]byte
}

// noAllocMembershipFilter makes the loaded probe path measurable without the
// recording copy used by recordingMembershipFilter.  The production filter is
// called through the interface, so this also catches an accidental fallback to
// a boxed decode/re-encode that allocates per candidate.  It intentionally says
// nothing about allocations inside the real C/Bloom filter; those belong to the
// integration/performance measurement.
type noAllocMembershipFilter struct {
	target []byte
}

func (f noAllocMembershipFilter) Test(data []byte) bool {
	if len(data) != len(f.target) {
		return false
	}
	for i := range data {
		if data[i] != f.target[i] {
			return false
		}
	}
	return true
}

func (noAllocMembershipFilter) TestVector(*vector.Vector, func(bool, bool, int)) []uint8 {
	return nil
}

func (noAllocMembershipFilter) Valid() bool { return true }

func (noAllocMembershipFilter) Exact() bool { return true }

func (noAllocMembershipFilter) Free() {}

func (f noAllocMembershipFilter) Share() docfilter.MembershipFilter { return f }

type loadedPkCase struct {
	name  string
	typ   types.T
	value any
	other any
}

func loadedPkCases() []loadedPkCase {
	u0, _ := types.ParseUuid("12345678-1234-1234-1234-1234567890ab")
	u1, _ := types.ParseUuid("abcdefab-cdef-abcd-efab-cdefabcdef01")
	return []loadedPkCase{
		{name: "int8", typ: types.T_int8, value: int8(-8), other: int8(-7)},
		{name: "int16", typ: types.T_int16, value: int16(-1600), other: int16(-1599)},
		{name: "int32", typ: types.T_int32, value: int32(-320000), other: int32(-319999)},
		{name: "int64", typ: types.T_int64, value: int64(-64000000), other: int64(-63999999)},
		{name: "int64-large", typ: types.T_int64, value: int64(1 << 40), other: int64(1<<40 + 1)},
		{name: "uint8", typ: types.T_uint8, value: uint8(8), other: uint8(9)},
		{name: "uint16", typ: types.T_uint16, value: uint16(1600), other: uint16(1601)},
		{name: "uint32", typ: types.T_uint32, value: uint32(320000), other: uint32(320001)},
		{name: "uint64", typ: types.T_uint64, value: uint64(64000000), other: uint64(64000001)},
		{name: "bit", typ: types.T_bit, value: uint64(7), other: uint64(8)},
		{name: "date", typ: types.T_date, value: types.Date(20260), other: types.Date(20261)},
		{name: "datetime", typ: types.T_datetime, value: types.Datetime(1234567), other: types.Datetime(1234568)},
		{name: "time", typ: types.T_time, value: types.Time(7654321), other: types.Time(7654322)},
		{name: "timestamp", typ: types.T_timestamp, value: types.Timestamp(9876543), other: types.Timestamp(9876544)},
		{name: "decimal64", typ: types.T_decimal64, value: types.Decimal64(12345), other: types.Decimal64(12346)},
		{name: "decimal128", typ: types.T_decimal128, value: types.Decimal128{B0_63: 12, B64_127: 34}, other: types.Decimal128{B0_63: 13, B64_127: 34}},
		{name: "char", typ: types.T_char, value: []byte("char-key"), other: []byte("char-other")},
		{name: "varchar", typ: types.T_varchar, value: []byte("varchar-key"), other: []byte("varchar-other")},
		{name: "text", typ: types.T_text, value: []byte("text-key"), other: []byte("text-other")},
		{name: "binary", typ: types.T_binary, value: []byte{0, 1, 2}, other: []byte{0, 1, 3}},
		{name: "varbinary", typ: types.T_varbinary, value: []byte{3, 4, 5}, other: []byte{3, 4, 6}},
		{name: "blob", typ: types.T_blob, value: []byte{6, 7, 8}, other: []byte{6, 7, 9}},
		{name: "json", typ: types.T_json, value: []byte(`{"k":1}`), other: []byte(`{"k":2}`)},
		{name: "datalink", typ: types.T_datalink, value: []byte("file://pk"), other: []byte("file://other")},
		{name: "uuid", typ: types.T_uuid, value: u0, other: u1},
		{name: "varchar-empty", typ: types.T_varchar, value: []byte{}, other: []byte("non-empty")},
	}
}

func (f *recordingMembershipFilter) Test(data []byte) bool {
	f.probeViews = append(f.probeViews, data)
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

// oldLoadedMembershipProbe is a controlled pre-optimization counterexample. It
// deliberately follows the former loaded.pk -> encodePk -> Test sequence so the
// allocation assertion below measures a real boxed/re-encoded probe, rather than
// assuming that every interface conversion allocates. It is test-only and does not
// represent the production implementation.
func oldLoadedMembershipProbe(seg *Segment, f docfilter.MembershipFilter, ord int64) bool {
	v := seg.pk(ord)
	raw, err := encodePk(seg.PkType, v)
	if err != nil {
		return false
	}
	return f.Test(raw)
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
	for _, tc := range loadedPkCases() {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBuilder("loaded-membership", int32(tc.typ))
			feed(t, b, tc.value, "x")
			feed(t, b, tc.other, "x")
			seg, err := b.Finish()
			require.NoError(t, err)
			require.NotNil(t, seg.pks)
			blob, err := seg.Serialize()
			require.NoError(t, err)
			loaded, err := Deserialize("loaded-membership", bytes.NewReader(blob))
			require.NoError(t, err)
			require.Nil(t, loaded.pks)
			t.Cleanup(func() { _ = loaded.dict.Close() })

			vec := vector.NewVec(tc.typ.ToType())
			require.NoError(t, vector.AppendAny(vec, tc.value, false, mp))
			t.Cleanup(func() { vec.Free(mp) })
			payload, err := docfilter.Build(vec)
			require.NoError(t, err)
			f, err := docfilter.New(payload)
			require.NoError(t, err)
			t.Cleanup(f.Free)

			allow := &docFilterMembership{seg: loaded, f: f}
			require.True(t, allow.Contains(0))
			require.False(t, allow.Contains(-1))
			require.False(t, allow.Contains(2))

			// The real docfilter remains the end-to-end membership check above. For
			// non-integer PKs it is a Bloom filter, so a successful probe alone cannot
			// prove that the zero-copy path passed the exact source bytes. Capture the
			// production probe and compare it byte-for-byte with the source vector;
			// UUID must receive raw 16-byte vector data, not canonical docmap text.
			capture := &recordingMembershipFilter{}
			captured := &docFilterMembership{seg: loaded, f: capture}
			require.True(t, captured.Contains(0))
			require.Len(t, capture.probes, 1)
			require.Len(t, capture.probeViews, 1)
			expected := sourcePkBytes(vec, tc.typ.ToType())
			require.Len(t, capture.probes[0], len(expected))
			require.True(t, bytes.Equal(expected, capture.probes[0]))

			// A real Bloom filter may legally return a false positive for a valid
			// non-member (decimal128 does so with this tiny fixture). Use the same
			// production probe boundary with a deterministic byte filter to assert a
			// valid hit and miss for every supported type, including UUID's fallback.
			exact := &docFilterMembership{seg: loaded, f: noAllocMembershipFilter{target: expected}}
			require.True(t, exact.Contains(0))
			require.False(t, exact.Contains(1))

			// Byte equality alone would still pass if loaded keys fell back to
			// decode-plus-reencode. Prove the non-UUID probe is the exact borrowed
			// docmap view. UUID is the intentional control: its stored 36-byte
			// canonical text must be converted to an independent raw 16-byte probe.
			stored, err := loaded.pkContent(0)
			require.NoError(t, err)
			view := capture.probeViews[0]
			if tc.typ == types.T_uuid {
				require.Len(t, stored, 36)
				require.Len(t, view, 16)
				require.False(t, &stored[0] == &view[0])
			} else {
				require.Len(t, view, len(stored))
				if len(stored) > 0 {
					require.True(t, &stored[0] == &view[0])
				}
			}
		})
	}
}

// TestLoadedContainsPkNoAlloc keeps the allocation contract at the production
// boundary: after the loaded segment and probe are warm, a non-UUID PK probe
// must borrow immutable docmap bytes instead of decoding a boxed value and
// encoding it again.  UUID is intentionally excluded because its loaded
// representation is canonical text while the runtime-filter source is raw 16
// bytes and therefore uses the typed fallback.
func TestLoadedContainsPkNoAlloc(t *testing.T) {
	for _, tc := range loadedPkCases() {
		if tc.typ == types.T_uuid {
			// UUID is intentionally excluded: the loaded docmap keeps canonical text
			// while the runtime filter source uses raw 16-byte values, so its typed
			// fallback necessarily decodes and re-encodes the probe.
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			// The int64 fixture uses 1<<40 (rather than a small integer) so a
			// regression that boxes and decodes the PK cannot accidentally benefit
			// from a runtime small-integer cache.
			b := NewBuilder("loaded-membership-alloc", int32(tc.typ))
			feed(t, b, tc.value, "x")
			feed(t, b, tc.other, "x")
			seg, err := b.Finish()
			require.NoError(t, err)
			blob, err := seg.Serialize()
			require.NoError(t, err)
			loaded, err := Deserialize("loaded-membership-alloc", bytes.NewReader(blob))
			require.NoError(t, err)
			t.Cleanup(func() { _ = loaded.dict.Close() })

			// Keep the filter input independent from loaded.pkContent: this models
			// the runtime-filter source vector and catches a loaded/source encoding
			// mismatch instead of comparing the loaded representation with itself.
			mp := mpool.MustNewZero()
			vec := vector.NewVec(tc.typ.ToType())
			require.NoError(t, vector.AppendAny(vec, tc.value, false, mp))
			t.Cleanup(func() { vec.Free(mp) })
			target := sourcePkBytes(vec, tc.typ.ToType())
			filter := noAllocMembershipFilter{target: target}
			membership := &docFilterMembership{seg: loaded, f: filter}
			require.True(t, membership.Contains(0))  // warm the loaded path
			require.False(t, membership.Contains(1)) // exercise a real miss as well
			var allowed, rejected bool
			allocs := testing.AllocsPerRun(100, func() {
				allowed = membership.Contains(0)
				rejected = membership.Contains(1)
			})
			require.True(t, allowed, "loaded %s hit was rejected", tc.name)
			require.False(t, rejected, "loaded %s miss was accepted", tc.name)
			require.Zero(t, allocs,
				"loaded %s Contains wrapper must borrow docmap bytes without per-probe allocations", tc.name)
		})
	}
}

// TestLoadedContainsPkBoxingCounterexample proves that the allocation test has
// detection power: with a large int64 that is outside the runtime's small-value
// cache, the controlled old loaded.pk -> encodePk probe allocates, while the
// production loaded byte-view Contains path remains allocation-free. The filter
// itself is a deterministic Go stub; this scopes the result to the Contains
// wrapper and excludes C/Bloom internals from the claim.
func TestLoadedContainsPkBoxingCounterexample(t *testing.T) {
	var tc loadedPkCase
	for _, candidate := range loadedPkCases() {
		if candidate.name == "int64-large" {
			tc = candidate
			break
		}
	}
	require.Equal(t, "int64-large", tc.name)

	b := NewBuilder("loaded-membership-boxing", int32(tc.typ))
	feed(t, b, tc.value, "x")
	feed(t, b, tc.other, "x")
	seg, err := b.Finish()
	require.NoError(t, err)
	blob, err := seg.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("loaded-membership-boxing", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	// Build the probe from an independent runtime-filter source vector, as in the
	// no-allocation matrix, so both paths test the same membership decision.
	mp := mpool.MustNewZero()
	vec := vector.NewVec(tc.typ.ToType())
	require.NoError(t, vector.AppendAny(vec, tc.value, false, mp))
	t.Cleanup(func() { vec.Free(mp) })
	filter := noAllocMembershipFilter{target: sourcePkBytes(vec, tc.typ.ToType())}
	membership := &docFilterMembership{seg: loaded, f: filter}
	require.True(t, membership.Contains(0))
	require.True(t, oldLoadedMembershipProbe(loaded, filter, 0))

	var newHit, oldHit bool
	newAllocs := testing.AllocsPerRun(100, func() {
		newHit = membership.Contains(0)
	})
	oldAllocs := testing.AllocsPerRun(100, func() {
		oldHit = oldLoadedMembershipProbe(loaded, filter, 0)
	})
	t.Logf("large int64 loaded membership allocations: new Contains=%.0f, controlled old boxed probe=%.0f", newAllocs, oldAllocs)
	require.True(t, newHit)
	require.True(t, oldHit)
	require.Zero(t, newAllocs,
		"loaded large-int64 Contains wrapper must not allocate per probe")
	require.Greater(t, oldAllocs, float64(0),
		"controlled boxed loaded.pk -> encodePk probe must allocate for the large-int64 counterexample")
}

func TestPkContentRejectsInvalidAccess(t *testing.T) {
	b := NewBuilder("loaded-membership-errors", int32(types.T_int64))
	feed(t, b, int64(1), "x")
	seg, err := b.Finish()
	require.NoError(t, err)

	_, err = seg.pkContent(0)
	require.Error(t, err)

	blob, err := seg.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("loaded-membership-errors", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })
	_, err = loaded.pkContent(0)
	require.NoError(t, err)

	_, err = loaded.pkContent(-1)
	require.Error(t, err)
	_, err = loaded.pkContent(loaded.N)
	require.Error(t, err)

	missingOffset := *loaded
	missingOffset.pkOffsets = nil
	_, err = missingOffset.pkContent(0)
	require.Error(t, err)

	truncated := *loaded
	truncated.pkRaw = truncated.pkRaw[:2]
	_, err = truncated.pkContent(0)
	require.Error(t, err)
	capture := &recordingMembershipFilter{}
	require.False(t, (&docFilterMembership{seg: &truncated, f: capture}).Contains(0))
	require.Empty(t, capture.probes)

	badLength := *loaded
	badLength.pkRaw = append([]byte(nil), loaded.pkRaw...)
	off := int(badLength.pkOffsets[0])
	binary.LittleEndian.PutUint32(badLength.pkRaw[off:], uint32(len(badLength.pkRaw)))
	_, err = badLength.pkContent(0)
	require.Error(t, err)
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
