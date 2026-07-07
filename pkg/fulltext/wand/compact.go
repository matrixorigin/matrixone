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
	"time"

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

// CompactSegments folds the tag=0 base sub-indexes + the entire visible tag=1
// CdcTail into a fresh, capacity-split tag=0 base — WITHOUT re-tokenizing from
// source — then deletes the old inputs, all in the caller's transaction. This is
// the Stage-2 "merge-all" body run by the fulltext_wand_compact TVF (which idxcron
// reaches via `ALTER … REINDEX … FULLTEXT MERGE`). Snapshot isolation makes it
// atomic: K = MAX(chunk_id) is read within the txn, so concurrent sinker appends
// (chunk_id > K) are invisible here and survive the `DELETE … chunk_id <= K`.
//
// Correctness: ComputeLiveness runs over ALL loaded segments (tag=0 + tag=1),
// resolving owner-by-chunk_id + deletes globally; FilterLive drops the
// dead/superseded ords so the merged base holds exactly the live corpus. The new
// base loads at ChunkId=-1 (below any future tail), so a pk later re-appended at
// chunk_id > K still supersedes the base copy at query time.
//
// Returns the number of new tag=0 sub-indexes written. A no-op (0) when there is
// nothing to compact. (Stage 2b will replace the merge-ALL with a tiered
// similar-size subset selection; the TVF/command/idxcron wiring is unchanged.)
func CompactSegments(sqlproc *sqlexec.SqlProcess, cfg TableConfig, capacity int64) (int, error) {
	// K = MAX tail chunk_id in this snapshot (the prefix we will fold + delete).
	_, k, emptyTail, err := tailChunkBounds(sqlproc, cfg)
	if err != nil {
		return 0, err
	}

	bases, err := LoadAllBases(sqlproc, cfg)
	if err != nil {
		return 0, err
	}
	tailSegs, deletes, err := loadTailSegments(sqlproc, cfg)
	if err != nil {
		freeSegs(bases)
		return 0, err
	}
	all := append(bases, tailSegs...)
	if len(all) == 0 {
		return 0, nil // empty index — nothing to compact
	}
	defer freeSegs(all) // free the off-heap loaded inputs; Merge copies what it keeps

	// Global liveness over tag=0 + tag=1, then densify each to its live ords. After
	// ComputeLiveness each pk has one owner, so the filtered models are pk-disjoint.
	live := ComputeLiveness(all, deletes)
	filtered := make([]*WandModel, 0, len(all))
	for i, s := range all {
		f := s.FilterLive(live[i])
		if f.N == 0 {
			continue // segment fully dead/superseded
		}
		filtered = append(filtered, f)
	}

	ts := time.Now().UnixMicro()
	uid := fmt.Sprintf("%s:%d", cfg.IndexTable, ts)

	// Build the new capped tag=0 sub-models (empty when the whole corpus is dead).
	var segs []*WandModel
	if len(filtered) > 0 {
		segs = Merge(uid, filtered...).Split(capacity)
	}

	// Replace: drop all old tag=0 bases + the folded tail prefix, then write the new
	// base. New sub-ids (uid:i) differ from the old ids, so order is immaterial; do
	// deletes first (mirrors the create TVF) to keep the metadata table minimal.
	clear := DeleteAllBasesSqls(cfg)
	if !emptyTail {
		clear = append(clear, DeleteTailChunksByMaxId(cfg, k)...)
	}
	for _, s := range clear {
		res, e := sqlexec.RunSql(sqlproc, s)
		if e != nil {
			return 0, e
		}
		res.Close()
	}

	for i, m := range segs {
		m.Id = SubIndexId(uid, i)
		sqls, cleanup, e := m.ToInsertSqls(cfg, ts, int(0)) // tag=0 base
		if e != nil {
			return 0, e
		}
		runErr := func() error {
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
		}()
		if runErr != nil {
			return 0, runErr
		}
	}
	return len(segs), nil
}
