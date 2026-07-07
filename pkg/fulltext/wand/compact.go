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

package wand

import (
	"fmt"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// compact.go — model-level primitives for tiered merge-compaction (Stage 2):
// densify a finalized model to its live ords (FilterLive) and split a finalized
// model into capacity-bounded sub-models (Split). Both operate on a model's
// already-finalized (ascending-ord) postings — no re-tokenize — and produce fresh
// Go-heap models, re-finalized. They are the pieces the CompactSegments
// orchestrator chains between ComputeLiveness and Merge.

// FilterLive returns a new densified model holding only the ords allow.Contains
// reports live, with ords compacted to a fresh 0..M-1 range and every term's
// postings filtered + remapped. allow == nil means "all ords live" (the
// ComputeLiveness fast path) — the receiver is returned unchanged.
//
// Postings are ascending by ord; the live-ord remap is monotonic, so the filtered
// postings stay ascending (finalizeScoring's invariant holds). After
// ComputeLiveness each pk has exactly one owning segment, so FilterLive'ing every
// segment yields pk-DISJOINT models — the precondition Merge requires.
func (m *WandModel) FilterLive(allow Membership) *WandModel {
	if allow == nil {
		return m
	}
	remap := make([]int64, m.N) // old ord -> new ord, or -1 if dead
	out := NewWandModel(m.Id, m.PkType)
	out.overflow = m.overflow // dict shared read-only; Merge reconciles it
	var newOrd int64
	for ord := int64(0); ord < m.N; ord++ {
		if allow.Contains(ord) {
			remap[ord] = newOrd
			out.pks = append(out.pks, m.pks[ord])
			out.docLen = append(out.docLen, m.docLen[ord])
			newOrd++
		} else {
			remap[ord] = -1
		}
	}
	out.N = newOrd
	for wid, tp := range m.terms {
		var stp *termPostings
		for i, ord := range tp.docIDs {
			no := remap[ord]
			if no < 0 {
				continue
			}
			if stp == nil {
				stp = &termPostings{}
			}
			stp.docIDs = append(stp.docIDs, no)
			stp.tfs = append(stp.tfs, tp.tfs[i])
		}
		if stp != nil {
			out.terms[wid] = stp
		}
	}
	out.finalizeScoring()
	return out
}

// Split partitions a finalized model into capacity-bounded sub-models by doc-ord
// range (each ≤ capacity docs), mirroring Builder.FinishSegments but on an
// already-built model. capacity <= 0 or N <= capacity returns the receiver
// unchanged. Each sub-model is self-contained (local 0-based ords, copied
// pks/docLen, its own remapped postings) and re-finalized; the overflow dict is
// shared. Used to keep a Merge result ≤ max_index_capacity.
//
// Requires ascending-ord postings (the model invariant after finalizeScoring /
// Merge / FilterLive). Does NOT sort in place — that would mutate off-heap
// C-buffer postings of a loaded model; callers pass Go-heap Merge/FilterLive
// output.
func (m *WandModel) Split(capacity int64) []*WandModel {
	n := m.N
	if capacity <= 0 || n <= capacity {
		return []*WandModel{m}
	}
	nseg := int((n + capacity - 1) / capacity)
	segs := make([]*WandModel, nseg)
	for s := 0; s < nseg; s++ {
		lo := int64(s) * capacity
		hi := lo + capacity
		if hi > n {
			hi = n
		}
		seg := NewWandModel(m.Id, m.PkType)
		seg.pks = append([]any(nil), m.pks[lo:hi]...)
		seg.docLen = append([]int32(nil), m.docLen[lo:hi]...)
		seg.overflow = m.overflow
		seg.N = hi - lo
		segs[s] = seg
	}
	for wid, tp := range m.terms {
		i, df := 0, len(tp.docIDs)
		for s := 0; s < nseg && i < df; s++ {
			hi := int64(s+1) * capacity
			start := i
			for i < df && tp.docIDs[i] < hi {
				i++
			}
			if i == start {
				continue
			}
			lo := int64(s) * capacity
			stp := &termPostings{
				docIDs: make([]int64, i-start),
				tfs:    append([]uint8(nil), tp.tfs[start:i]...),
			}
			for j := start; j < i; j++ {
				stp.docIDs[j-start] = tp.docIDs[j] - lo // global -> local ord
			}
			segs[s].terms[wid] = stp
		}
	}
	for _, seg := range segs {
		seg.finalizeScoring()
	}
	return segs
}

// CompactSegments folds the visible tag=1 CdcTail into the tag=0 base WITHOUT
// re-tokenizing from source and WITHOUT rewriting the existing base sub-indexes —
// the O(tail) "fold" step of the recency LSM. It runs in the caller's transaction
// (the fulltext_wand_compact TVF, reached by `ALTER … REINDEX … FULLTEXT MERGE`).
// Snapshot isolation makes it atomic: K = MAX(chunk_id) is read within the txn, so
// concurrent sinker appends (chunk_id > K) are invisible and survive the tail delete.
//
// Only the (threshold-bounded) tail is loaded — never the base — so memory is O(tail),
// not O(corpus). Steps:
//  1. Load the tag=1 tail: insert segments + folded delete map + the pk type.
//  2. Live-filter the tail inserts among themselves (dedup by chunk_id, drop those a
//     later tail delete supersedes) → Merge into new capacity-split tag=0 sub(s) at
//     recency K (metadata.chunk_id = K, above every existing base at recency < K).
//  3. Surviving deletes = tail deletes whose pk is NOT a live tail insert. They must
//     still shadow stale copies in the untouched OLD bases (recency < K), so re-frame
//     them as ONE tail delete frame at NextTailChunkId (> K). Deletes resolved inside
//     the tail (pk re-inserted live) are dropped.
//  4. Delete the folded tail (chunk_id ≤ K). Old base subs are left in place; their
//     stale/deleted copies are shadowed by the new sub (recency K) and the surviving
//     deletes (recency > K) at query time. A later tiered merge (2b) reclaims the space.
//
// Returns the number of new tag=0 sub-indexes written (0 when the tail held only
// resolved churn / nothing to fold).
func CompactSegments(sqlproc *sqlexec.SqlProcess, cfg TableConfig, capacity int64) (int, error) {
	// K = MAX tail chunk_id in this snapshot (the prefix we fold + delete).
	_, k, emptyTail, err := tailChunkBounds(sqlproc, cfg)
	if err != nil {
		return 0, err
	}
	if emptyTail {
		return 0, nil // no tail → nothing to fold
	}

	tailSegs, deletes, pkType, err := loadTailSegments(sqlproc, cfg)
	if err != nil {
		return 0, err
	}
	defer freeSegs(tailSegs) // free off-heap loaded inputs; Merge copies what it keeps

	// Live-filter the tail inserts (dedup by chunk_id + drop tail-deleted); collect
	// the surviving pks. After ComputeLiveness each pk has one owner, so the filtered
	// models are pk-disjoint — Merge's precondition.
	live := ComputeLiveness(tailSegs, deletes)
	filtered := make([]*WandModel, 0, len(tailSegs))
	livePks := make(map[any]struct{})
	for i, s := range tailSegs {
		f := s.FilterLive(live[i])
		if f.N == 0 {
			continue // segment fully dead/superseded within the tail
		}
		filtered = append(filtered, f)
		for _, pk := range f.pks {
			livePks[pk] = struct{}{}
		}
	}
	if pkType == 0 && len(filtered) > 0 {
		pkType = filtered[0].PkType
	}

	// Fold the live tail inserts → new base sub(s) at recency K. The id is timestamp-
	// unique (disjoint from existing base ids); recency is carried by ChunkId, not id.
	ts := time.Now().UnixMicro()
	uid := fmt.Sprintf("%s:%d", cfg.IndexTable, ts)
	var segs []*WandModel
	if len(filtered) > 0 {
		merged := Merge(uid, filtered...)
		segs = merged.Split(capacity)
		for _, s := range segs {
			s.ChunkId = k
		}
	}

	// Surviving deletes: tail deletes not resolved by a live re-insert. They shadow
	// stale copies in the untouched old bases (recency < K).
	var surviving []DeleteRecord
	for pk := range deletes {
		if _, ok := livePks[pk]; !ok {
			surviving = append(surviving, DeleteRecord{Pk: pk})
		}
	}

	// Write the new base sub(s) at recency K.
	for i, m := range segs {
		m.Id = SubIndexId(uid, i)
		sqls, cleanup, e := m.ToInsertSqls(cfg, ts, int(0)) // tag=0 base
		if e != nil {
			return 0, e
		}
		if e := runSqlsWithCleanup(sqlproc, sqls, cleanup); e != nil {
			return 0, e
		}
	}

	// Re-frame surviving deletes as ONE tail delete frame. Runs AFTER writing the base
	// at recency K, so NextTailChunkId = K+1 (still ≤ K tail present) → the frame lands
	// above the new base and every old base; the tail delete below then spares it.
	if len(surviving) > 0 {
		if pkType == 0 {
			return 0, moerr.NewInternalError(sqlproc.GetContext(),
				"wand compact: surviving deletes but unknown pk type")
		}
		if e := appendDeleteFrame(sqlproc, cfg, pkType, surviving); e != nil {
			return 0, e
		}
	}

	// Delete the folded tail prefix (≤ K). Old base subs are left untouched.
	for _, s := range DeleteTailChunksByMaxId(cfg, k) {
		res, e := sqlexec.RunSql(sqlproc, s)
		if e != nil {
			return 0, e
		}
		res.Close()
	}
	return len(segs), nil
}

// runSqlsWithCleanup runs a group of statements, calling cleanup (temp-file removal)
// after — even on error — so a failed base write never leaks its serialized blob.
func runSqlsWithCleanup(sqlproc *sqlexec.SqlProcess, sqls []string, cleanup func()) error {
	if cleanup != nil {
		defer cleanup()
	}
	for _, s := range sqls {
		res, e := sqlexec.RunSql(sqlproc, s)
		if e != nil {
			return e
		}
		res.Close()
	}
	return nil
}

// appendDeleteFrame persists one tag=1 delete frame (the compaction's surviving
// deletes) at NextTailChunkId — the same file→chunk-rows path the CDC sinker uses,
// so it re-loads as an ordinary tail delete frame.
func appendDeleteFrame(sqlproc *sqlexec.SqlProcess, cfg TableConfig, pkType int32, recs []DeleteRecord) error {
	framed, err := FrameDeletes(pkType, recs)
	if err != nil {
		return err
	}
	fp, err := os.CreateTemp("", "wanddel")
	if err != nil {
		return err
	}
	path := fp.Name()
	defer func() { fp.Close(); os.Remove(path) }()
	if _, err = fp.Write(framed); err != nil {
		return err
	}
	if err = fp.Sync(); err != nil { // durable before load_file reads it
		return err
	}
	start, err := nextTailChunkId(sqlproc, cfg)
	if err != nil {
		return err
	}
	for _, s := range TailFileInsertSqls(cfg, start, path, len(framed)) {
		res, e := sqlexec.RunSql(sqlproc, s)
		if e != nil {
			return e
		}
		res.Close()
	}
	return nil
}

// nextTailChunkId runs NextTailChunkIdSql and returns the next free tag=1 append
// position (GREATEST(max tail chunk_id, max base recency)+1).
func nextTailChunkId(sqlproc *sqlexec.SqlProcess, cfg TableConfig) (int64, error) {
	res, err := sqlexec.RunSql(sqlproc, NextTailChunkIdSql(cfg))
	if err != nil {
		return 0, err
	}
	defer res.Close()
	for _, bat := range res.Batches {
		if bat != nil && bat.RowCount() > 0 {
			return vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0), nil
		}
	}
	return 0, nil
}
