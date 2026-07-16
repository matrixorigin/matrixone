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
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Membership is a doc-ord allow-set consulted during search for WHERE-clause
// prefiltering — the fulltext2 analogue of bm25/wand.Membership and cuVS filtered
// search. A nil Membership means "allow all" (no prefilter). It is built PER QUERY
// (the loaded Index is shared/cached across concurrent queries, so per-query filter
// state must never live on the Index) and threaded as a parameter into the segment
// searches, where it prunes candidates BEFORE they enter the top-k — so a pushed
// LIMIT bounds the FILTERED result, not the raw match set.
type Membership interface {
	Contains(ord int64) bool
}

// docFilterMembership resolves a pk-based docfilter.MembershipFilter (built in C from
// the pushed-down WHERE pks) against ONE segment's ord→pk dictionary: doc ord is
// allowed iff its pk bytes pass the filter. Mirrors bm25's docFilterMembership;
// fulltext2 shares the byte-identical pk codec, so the encoded probe bytes match what
// docfilter.Build hashed on the source side. For integer PKs this is the C int64
// cbitmap (exact); for other PKs a bloom (its false positives are removed by the
// downstream join to the filtered source).
type docFilterMembership struct {
	seg     *Segment
	f       docfilter.MembershipFilter
	scratch [8]byte // reused encode buffer for the hot integer-PK path (Test copies out)
}

func (d *docFilterMembership) Contains(ord int64) bool {
	if ord < 0 || ord >= int64(len(d.seg.pks)) {
		return false
	}
	v := d.seg.pks[ord]
	// Contains runs once per WAND candidate on the block-max walk hot path. For the
	// common integer PKs, encode straight into the reused scratch buffer (byte-identical
	// to packUint*) rather than allocating via encodePk per call; Test reads the bytes
	// synchronously and does not retain them, so reuse is safe.
	var raw []byte
	switch types.T(d.seg.PkType) {
	case types.T_int64:
		binary.LittleEndian.PutUint64(d.scratch[:], uint64(v.(int64)))
		raw = d.scratch[:8]
	case types.T_uint64:
		binary.LittleEndian.PutUint64(d.scratch[:], v.(uint64))
		raw = d.scratch[:8]
	case types.T_int32:
		binary.LittleEndian.PutUint32(d.scratch[:4], uint32(v.(int32)))
		raw = d.scratch[:4]
	case types.T_uint32:
		binary.LittleEndian.PutUint32(d.scratch[:4], v.(uint32))
		raw = d.scratch[:4]
	case types.T_uuid:
		// The membership filter hashes each source uuid as its RAW 16 bytes; probe with
		// the same raw bytes, NOT encodePk (the 36-char canonical string), which would
		// never hit the same bloom cell and reject every candidate.
		u := v.(types.Uuid)
		raw = u[:]
	default:
		var err error
		if raw, err = encodePk(d.seg.PkType, v); err != nil {
			return false
		}
	}
	return d.f.Test(raw)
}

// mkAllow returns a per-segment WHERE-prefilter Membership over seg's ords, or nil
// when there is no filter (the "allow all" fast path).
func mkAllow(seg *Segment, f docfilter.MembershipFilter) Membership {
	if f == nil {
		return nil
	}
	return &docFilterMembership{seg: seg, f: f}
}

// allowed reports whether ord passes the membership (nil = allow all) — the one-liner
// the segment searches use at each admit point.
func allowed(m Membership, ord int64) bool { return m == nil || m.Contains(ord) }

// livenessMembership admits ord in segment si iff it is the LIVE copy of its pk
// (highest-Recency, not delete-shadowed). Threaded into the per-segment boolean walk
// so a segment's top-k is k LIVE docs — a dead copy (superseded by a later CDC
// segment, or deleted) never enters the heap, so the cross-segment merge can't
// under-fill by later dropping it. Mirrors bm25's per-segment liveness Membership.
type livenessMembership struct {
	idx *Index
	si  int
}

func (l *livenessMembership) Contains(ord int64) bool {
	b := l.idx.liveOrd[l.si]
	if b == nil {
		return true // fully-live segment (no dead copies) — the fast path
	}
	return ord >= 0 && ord < int64(len(b)) && b[ord]
}

// andMembership is the conjunction of two memberships (either may be nil = allow all).
type andMembership struct{ a, b Membership }

func (m andMembership) Contains(ord int64) bool {
	return (m.a == nil || m.a.Contains(ord)) && (m.b == nil || m.b.Contains(ord))
}

// andAllow ANDs two memberships, collapsing nils (used to AND the WHERE prefilter with
// per-segment liveness). nil AND x = x.
func andAllow(a, b Membership) Membership {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	default:
		return andMembership{a, b}
	}
}
