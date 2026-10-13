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
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

type uuidMembershipDoc struct {
	pk    any
	words []string
}

func loadedUUIDSegment(tb testing.TB, docs ...uuidMembershipDoc) *Segment {
	tb.Helper()
	b := NewBuilder("uuid-membership", int32(types.T_uuid))
	for _, doc := range docs {
		pos := int32(0)
		for _, word := range doc.words {
			require.NoError(tb, b.Add(word, pos, doc.pk))
			pos += int32(len(word)) + 1
		}
	}
	seg, err := b.Finish()
	require.NoError(tb, err)
	blob, err := seg.Serialize()
	require.NoError(tb, err)
	loaded, err := Deserialize(seg.Id, bytes.NewReader(blob))
	require.NoError(tb, err)
	require.Nil(tb, loaded.pks)
	tb.Cleanup(func() { _ = loaded.dict.Close() })
	return loaded
}

func mustTestUUID(tb testing.TB, s string) types.Uuid {
	tb.Helper()
	u, err := types.ParseUuid(s)
	require.NoError(tb, err)
	return u
}

func loadedUUIDFilter(tb testing.TB, values ...types.Uuid) docfilter.MembershipFilter {
	tb.Helper()
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.New(types.T_uuid, 16, 0))
	for _, value := range values {
		require.NoError(tb, vector.AppendFixed(vec, value, false, mp))
	}
	payload, err := docfilter.Build(vec)
	require.NoError(tb, err)
	filter, err := docfilter.New(payload)
	require.NoError(tb, err)
	tb.Cleanup(func() { vec.Free(mp) })
	tb.Cleanup(filter.Free)
	return filter
}

func cloneLoadedSegment(s *Segment) Segment {
	clone := *s
	clone.pkRaw = append([]byte(nil), s.pkRaw...)
	clone.pkOffsets = append([]int32(nil), s.pkOffsets...)
	return clone
}

func uuidResultScoreBits(tb testing.TB, results []Result) map[types.Uuid]uint32 {
	tb.Helper()
	out := make(map[types.Uuid]uint32, len(results))
	for _, result := range results {
		u, ok := result.Pk.(types.Uuid)
		require.Truef(tb, ok, "result pk has type %T", result.Pk)
		out[u] = math.Float32bits(result.Score)
	}
	return out
}

type uuidSetMembershipFilter struct {
	allowed map[types.Uuid]struct{}
}

func newUUIDSetMembershipFilter(values ...types.Uuid) uuidSetMembershipFilter {
	allowed := make(map[types.Uuid]struct{}, len(values))
	for _, value := range values {
		allowed[value] = struct{}{}
	}
	return uuidSetMembershipFilter{allowed: allowed}
}

func (f uuidSetMembershipFilter) Test(data []byte) bool {
	if len(data) != 16 {
		return false
	}
	var value types.Uuid
	copy(value[:], data)
	_, ok := f.allowed[value]
	return ok
}

func (uuidSetMembershipFilter) TestVector(*vector.Vector, func(bool, bool, int)) []uint8 {
	return nil
}

func (uuidSetMembershipFilter) Valid() bool { return true }

func (uuidSetMembershipFilter) Exact() bool { return true }

func (uuidSetMembershipFilter) Free() {}

func (f uuidSetMembershipFilter) Share() docfilter.MembershipFilter { return f }

// TestLoadedUUIDMembershipRepresentations proves that the loaded docmap keeps
// each historical UUID string representation readable while the membership probe
// is always the source vector's independent raw 16-byte value.
func TestLoadedUUIDMembershipRepresentations(t *testing.T) {
	const canonical = "12345678-1234-1234-1234-1234567890ab"
	other := mustTestUUID(t, "abcdefab-cdef-abcd-efab-cdefabcdef01")
	cases := []struct {
		name string
		pk   string
	}{
		{name: "32-hex", pk: "123456781234123412341234567890ab"},
		{name: "36-canonical", pk: canonical},
		{name: "38-braced", pk: "{" + canonical + "}"},
		{name: "45-urn", pk: "urn:uuid:" + canonical},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := mustTestUUID(t, tc.pk)
			loaded := loadedUUIDSegment(t,
				uuidMembershipDoc{pk: tc.pk, words: []string{"alpha"}},
				uuidMembershipDoc{pk: other.String(), words: []string{"alpha"}},
			)
			require.Equal(t, int64(2), loaded.N)

			// Keep the real C/Bloom filter hit in this representation matrix; use an
			// exact independent filter for the miss because a one-element Bloom filter
			// may legally return a false positive for the other UUID.
			realFilter := loadedUUIDFilter(t, u)
			realAllow := &docFilterMembership{seg: loaded, f: realFilter}
			require.True(t, realAllow.Contains(0))
			exactAllow := &docFilterMembership{seg: loaded, f: newUUIDSetMembershipFilter(u)}
			require.False(t, exactAllow.Contains(1))
			require.False(t, exactAllow.Contains(-1))
			require.False(t, exactAllow.Contains(loaded.N))

			capture := &recordingMembershipFilter{}
			captured := &docFilterMembership{seg: loaded, f: capture}
			require.True(t, captured.Contains(0))
			require.Len(t, capture.probes, 1)
			require.Len(t, capture.probeViews, 1)
			expected := [16]byte(u)
			require.Equal(t, expected[:], capture.probes[0])
			require.Len(t, capture.probeViews[0], 16)
			stored, err := loaded.pkContent(0)
			require.NoError(t, err)
			require.Len(t, stored, len(tc.pk))
			require.False(t, &stored[0] == &capture.probeViews[0][0],
				"UUID probe must not alias the docmap text")
		})
	}
}

// TestLoadedUUIDMembershipMixed proves a filter built from an independent UUID
// source vector admits a mixed hit/miss set across loaded ordinals.
func TestLoadedUUIDMembershipMixed(t *testing.T) {
	u0 := mustTestUUID(t, "00000000-0000-0000-0000-000000000001")
	u1 := mustTestUUID(t, "00000000-0000-0000-0000-000000000002")
	u2 := mustTestUUID(t, "00000000-0000-0000-0000-000000000003")
	u3 := mustTestUUID(t, "00000000-0000-0000-0000-000000000004")
	loaded := loadedUUIDSegment(t,
		uuidMembershipDoc{pk: u0.String(), words: []string{"alpha"}},
		uuidMembershipDoc{pk: u1.String(), words: []string{"alpha"}},
		uuidMembershipDoc{pk: u2.String(), words: []string{"alpha"}},
		uuidMembershipDoc{pk: u3.String(), words: []string{"alpha"}},
	)
	filter := newUUIDSetMembershipFilter(u0, u2)
	allow := &docFilterMembership{seg: loaded, f: filter}
	for ord, want := range []bool{true, false, true, false} {
		require.Equalf(t, want, allow.Contains(int64(ord)), "ord %d", ord)
	}

	// A deterministic exact filter captures the exact bytes passed to Test while
	// keeping the expected set independent of the loaded docmap representation.
	capture := &recordingMembershipFilter{}
	captured := &docFilterMembership{seg: loaded, f: capture}
	for ord := int64(0); ord < loaded.N; ord++ {
		require.True(t, captured.Contains(ord))
	}
	require.Len(t, capture.probes, 4)
	for i, want := range []types.Uuid{u0, u1, u2, u3} {
		wantBytes := [16]byte(want)
		require.Equalf(t, wantBytes[:], capture.probes[i], "ord %d", i)
	}
}

// TestLoadedUUIDMembershipRejectsMalformedRows proves malformed first and later
// UUID content fails closed without calling the filter or panicking. The valid
// first row makes later-row corruption reachable after Deserialize accepts the
// length and type metadata.
func TestLoadedUUIDMembershipRejectsMalformedRows(t *testing.T) {
	u0 := mustTestUUID(t, "00000000-0000-0000-0000-000000000001")
	u1 := mustTestUUID(t, "00000000-0000-0000-0000-000000000002")
	u2 := mustTestUUID(t, "00000000-0000-0000-0000-000000000003")
	u3 := mustTestUUID(t, "00000000-0000-0000-0000-000000000004")
	loaded := loadedUUIDSegment(t,
		uuidMembershipDoc{pk: u0.String(), words: []string{"alpha"}},
		uuidMembershipDoc{pk: u1.String(), words: []string{"alpha"}},
		uuidMembershipDoc{pk: u2.String(), words: []string{"alpha"}},
		uuidMembershipDoc{pk: u3.String(), words: []string{"alpha"}},
	)

	t.Run("later-invalid-hex", func(t *testing.T) {
		bad := cloneLoadedSegment(loaded)
		off := int(bad.pkOffsets[1]) + 4
		bad.pkRaw[off] = 'z'
		capture := &recordingMembershipFilter{}
		allow := &docFilterMembership{seg: &bad, f: capture}
		require.True(t, allow.Contains(0))
		require.Len(t, capture.probes, 1)
		require.NotPanics(t, func() { require.False(t, allow.Contains(1)) })
		require.Len(t, capture.probes, 1, "malformed UUID must not reach Test")
		require.True(t, allow.Contains(2))
	})

	t.Run("first-invalid-hyphen", func(t *testing.T) {
		bad := cloneLoadedSegment(loaded)
		off := int(bad.pkOffsets[0]) + 4 + 8
		bad.pkRaw[off] = '_'
		capture := &recordingMembershipFilter{}
		allow := &docFilterMembership{seg: &bad, f: capture}
		require.NotPanics(t, func() { require.False(t, allow.Contains(0)) })
		require.Empty(t, capture.probes)
	})

	for _, width := range []int{35, 37} {
		t.Run("later-invalid-length-"+strconv.Itoa(width), func(t *testing.T) {
			bad := cloneLoadedSegment(loaded)
			off := int(bad.pkOffsets[1])
			binary.LittleEndian.PutUint32(bad.pkRaw[off:], uint32(width))
			capture := &recordingMembershipFilter{}
			allow := &docFilterMembership{seg: &bad, f: capture}
			require.NotPanics(t, func() { require.False(t, allow.Contains(1)) })
			require.Empty(t, capture.probes)
		})
	}

	// The stale-scratch sequence is intentional: a parse error must not reuse the
	// previous hit's raw bytes, and a later valid row must still parse normally.
	bad := cloneLoadedSegment(loaded)
	off := int(bad.pkOffsets[1]) + 4
	bad.pkRaw[off] = 'z'
	allow := &docFilterMembership{seg: &bad, f: newUUIDSetMembershipFilter(u0, u2)}
	require.True(t, allow.Contains(0))
	require.False(t, allow.Contains(1))
	require.False(t, allow.Contains(3))
	require.True(t, allow.Contains(2))
}

// TestLoadedUUIDMembershipRejectsInvalidAccess covers the defensive pkContent
// boundary used before UUID parsing: ordinal, offset, header, span, and payload
// truncation must all return false and leave the filter untouched.
func TestLoadedUUIDMembershipRejectsInvalidAccess(t *testing.T) {
	u0 := mustTestUUID(t, "00000000-0000-0000-0000-000000000001")
	u1 := mustTestUUID(t, "00000000-0000-0000-0000-000000000002")
	loaded := loadedUUIDSegment(t,
		uuidMembershipDoc{pk: u0.String(), words: []string{"alpha"}},
		uuidMembershipDoc{pk: u1.String(), words: []string{"alpha"}},
	)

	tests := []struct {
		name string
		ord  int64
		mut  func(*Segment)
	}{
		{name: "negative-ordinal", ord: -1, mut: func(*Segment) {}},
		{name: "ordinal-at-end", ord: loaded.N, mut: func(*Segment) {}},
		{name: "missing-offsets", ord: 0, mut: func(s *Segment) { s.pkOffsets = nil }},
		{name: "negative-offset", ord: 0, mut: func(s *Segment) { s.pkOffsets[0] = -1 }},
		{name: "short-header", ord: 0, mut: func(s *Segment) { s.pkRaw = s.pkRaw[:2] }},
		{name: "truncated-payload", ord: 1, mut: func(s *Segment) {
			off := int(s.pkOffsets[1])
			s.pkRaw = s.pkRaw[:off+4+35]
		}},
		{name: "length-out-of-span", ord: 0, mut: func(s *Segment) {
			off := int(s.pkOffsets[0])
			binary.LittleEndian.PutUint32(s.pkRaw[off:], uint32(len(s.pkRaw)))
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bad := cloneLoadedSegment(loaded)
			tc.mut(&bad)
			capture := &recordingMembershipFilter{}
			allow := &docFilterMembership{seg: &bad, f: capture}
			require.NotPanics(t, func() { require.False(t, allow.Contains(tc.ord)) })
			require.Empty(t, capture.probes)
		})
	}
}

// TestLoadedUUIDMembershipLivenessScoreAndStream compares loaded membership
// against an unfiltered result oracle. It covers dead-copy suppression, exact
// float32 score bits for admitted rows, the no-LIMIT stream path, and repeated
// query owners sharing immutable loaded segments.
func TestLoadedUUIDMembershipLivenessScoreAndStream(t *testing.T) {
	u0 := mustTestUUID(t, "00000000-0000-0000-0000-000000000001")
	u1 := mustTestUUID(t, "00000000-0000-0000-0000-000000000002")
	u2 := mustTestUUID(t, "00000000-0000-0000-0000-000000000003")
	u3 := mustTestUUID(t, "00000000-0000-0000-0000-000000000004")
	base := loadedUUIDSegment(t,
		uuidMembershipDoc{pk: u0.String(), words: []string{"alpha", "alpha"}},
		uuidMembershipDoc{pk: u1.String(), words: []string{"alpha"}},
		uuidMembershipDoc{pk: u2.String(), words: []string{"alpha"}},
	)
	base.Recency = 0
	tail := loadedUUIDSegment(t,
		uuidMembershipDoc{pk: u0.String(), words: []string{"alpha"}},
		uuidMembershipDoc{pk: u3.String(), words: []string{"alpha"}},
	)
	tail.Recency = 1
	idx := NewIndex([]*Segment{base, tail}, nil)

	unfiltered, err := idx.SearchQuery([]byte("alpha"), true, ParserDefault, BM25, 100, nil)
	require.NoError(t, err)
	require.Len(t, unfiltered, 4)
	allScores := uuidResultScoreBits(t, unfiltered)
	require.Equal(t, map[types.Uuid]struct{}{u0: {}, u1: {}, u2: {}, u3: {}},
		resultUUIDSet(unfiltered))

	filter := newUUIDSetMembershipFilter(u0, u1, u3)
	pref := &prefilter{docFilter: filter}
	filtered, err := idx.SearchQuery([]byte("alpha"), true, ParserDefault, BM25, 100, pref)
	require.NoError(t, err)
	require.Len(t, filtered, 3)
	filteredScores := uuidResultScoreBits(t, filtered)
	require.Equal(t, map[types.Uuid]struct{}{u0: {}, u1: {}, u3: {}}, resultUUIDSet(filtered))
	for value, score := range filteredScores {
		require.Equalf(t, allScores[value], score, "score bits changed for %s", value)
	}

	// The same loaded segment can create independent query-local owners. The prefilter
	// admits the base copy of u0, but the liveness conjunction rejects that stale ord;
	// only the newer tail copy survives the complete search.
	first := mkAllow(base, pref)
	second := mkAllow(base, pref)
	require.NotSame(t, first, second)
	require.NotSame(t, first.(*docFilterMembership), second.(*docFilterMembership))
	require.True(t, allowed(first, 0))
	require.True(t, allowed(second, 1))
	require.False(t, allowed(andAllow(first, &livenessMembership{idx: idx, si: 0}), 0))
	require.True(t, allowed(andAllow(second, &livenessMembership{idx: idx, si: 0}), 1))

	collectStream := func(boolean bool) map[types.Uuid]uint32 {
		got := make(map[types.Uuid]uint32)
		emit := func(out *vectorindex.SearchOutput) error {
			require.Equal(t, types.T_uuid, out.Keys.Type)
			cursor := 0
			for i := 0; i < out.Keys.N; i++ {
				require.GreaterOrEqual(t, len(out.Keys.Data)-cursor, 4)
				width := int(binary.LittleEndian.Uint32(out.Keys.Data[cursor:]))
				cursor += 4
				require.Equal(t, 36, width)
				require.GreaterOrEqual(t, len(out.Keys.Data)-cursor, width)
				value, err := types.ParseUuid(string(out.Keys.Data[cursor : cursor+width]))
				require.NoError(t, err)
				cursor += width
				_, duplicate := got[value]
				require.Falsef(t, duplicate, "stream emitted duplicate UUID %s", value)
				got[value] = math.Float32bits(out.Dists[i])
			}
			require.Equal(t, len(out.Keys.Data), cursor)
			PutColumnBuffer(out.Keys)
			return nil
		}
		err := idx.StreamQuery([]byte("alpha"), boolean, ParserDefault, BM25, pref, false, emit)
		require.NoError(t, err)
		return got
	}
	require.Equal(t, filteredScores, collectStream(true), "boolean stream changed membership or score")
	require.Equal(t, filteredScores, collectStream(false), "phrase stream changed membership or score")

	// Repeat the filtered search after the stream owners have been released. This
	// catches stale query-local state crossing the normal query lifecycle boundary.
	for i := 0; i < 3; i++ {
		repeated, repeatErr := idx.SearchQuery([]byte("alpha"), true, ParserDefault, BM25, 100, &prefilter{docFilter: filter})
		require.NoError(t, repeatErr)
		require.Equal(t, filteredScores, uuidResultScoreBits(t, repeated))
	}
}

func resultUUIDSet(results []Result) map[types.Uuid]struct{} {
	set := make(map[types.Uuid]struct{}, len(results))
	for _, result := range results {
		if value, ok := result.Pk.(types.Uuid); ok {
			set[value] = struct{}{}
		}
	}
	return set
}

// TestLoadedUUIDMembershipParallelOwners gives each worker a distinct
// docFilterMembership and interleaves probes against one immutable loaded segment.
func TestLoadedUUIDMembershipParallelOwners(t *testing.T) {
	u0 := mustTestUUID(t, "00000000-0000-0000-0000-000000000001")
	u1 := mustTestUUID(t, "00000000-0000-0000-0000-000000000002")
	loaded := loadedUUIDSegment(t,
		uuidMembershipDoc{pk: u0.String(), words: []string{"alpha"}},
		uuidMembershipDoc{pk: u1.String(), words: []string{"alpha"}},
	)
	filter := newUUIDSetMembershipFilter(u0)
	const workers = 16
	const iterations = 200
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			owner := &docFilterMembership{seg: loaded, f: filter}
			for i := 0; i < iterations; i++ {
				if !owner.Contains(0) {
					errs <- errors.New("parallel loaded UUID hit rejected")
					return
				}
				if owner.Contains(1) {
					errs <- errors.New("parallel loaded UUID miss accepted")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
