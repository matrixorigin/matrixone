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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// CompactSegments folds the tag=1 CdcTail into the tag=0 base and reclaims dead
// space: it loads base + tail + deletes, reconstructs the LIVE docs from their
// positional postings (no re-tokenize), rebuilds capacity-bounded fresh tag=0
// bases, and atomically replaces ALL prior bases + the whole tail — within the
// caller's (TVF statement) transaction. This is a FULL compaction, simpler than
// bm25's incremental tail-fold; an incremental variant is a later optimization.
// Returns the live-doc count. No tail delta → nothing to reclaim (returns 0).
func CompactSegments(sqlproc *sqlexec.SqlProcess, cfg TableConfig, capacity, postingCap int64) (int, error) {
	bases, err := LoadAllBases(sqlproc, cfg)
	if err != nil {
		return 0, err
	}
	tails, deletes, err := LoadTailSegments(sqlproc, cfg)
	if err != nil {
		freeSegs(bases)
		return 0, err
	}
	// Free the loaded (off-heap) base + tail buffers on every return path; the fresh
	// rebuilt segments below are build-side (Go heap, no deallocators — Free no-op).
	defer freeSegs(bases)
	defer freeSegs(tails)
	if len(tails) == 0 && len(deletes) == 0 {
		return 0, nil // no CDC delta since the last build/compaction
	}

	// Recency for the fresh base: above every existing base + tail chunk_id, so a
	// later tail append (NextTailChunkId = base+1) stays monotonic and newer.
	recency, err := NextTailChunkId(sqlproc, cfg)
	if err != nil {
		return 0, err
	}

	segs := append(bases, tails...)
	pkType := int32(0)
	if len(segs) > 0 {
		pkType = segs[0].PkType
	}
	idx := NewIndex(segs, deletes)
	if capacity <= 0 {
		capacity = DefaultBuildCapacity // floor so the fresh base is sealed+spilled per ~1M docs
	}
	if postingCap <= 0 {
		postingCap = DefaultPostingCapacity // floor so a long-doc corpus's rebuild stays memory-bounded
	}

	ts := time.Now().UnixMicro()
	uid := fmt.Sprintf("%s:%d", cfg.IndexTable, ts)
	var bopts []BuildOpt
	if cfg.PositionFree {
		bopts = append(bopts, WithPositionFree())
	}

	// Atomic replace, all in the caller's (TVF statement) txn: drop every prior base +
	// the whole tail FIRST, then STREAM the fresh dead-doc-free base(s). Deleting first
	// is required because DeleteAllBases is indiscriminate over tag=0 — writing the fresh
	// bases first would let it wipe them. It is safe because ReconstructLiveDocs reads the
	// already-loaded in-memory idx (not the storage rows), and a mid-stream failure rolls
	// the whole statement back.
	for _, s := range DeleteAllBasesSqls(cfg) {
		if e := runCompactSql(sqlproc, s); e != nil {
			return 0, e
		}
	}
	for _, s := range DeleteTailSqls(cfg) {
		if e := runCompactSql(sqlproc, s); e != nil {
			return 0, e
		}
	}

	// Stream the reconstructed live docs into a capacity-bounded builder, sealing +
	// persisting + freeing each fresh base segment as it fills — so peak build memory is
	// one open segment, not the whole rebuilt corpus.
	var cur *Builder
	segIdx := 0
	nlive := 0
	sealFresh := func() error {
		if cur == nil || cur.NumDocs() == 0 {
			cur = nil
			return nil
		}
		seg, err := cur.Finish()
		cur = nil
		if err != nil {
			return err
		}
		seg.Id = SubIndexId(uid, segIdx)
		seg.Recency = recency
		segIdx++
		sqls, cleanup, e := seg.ToInsertSqls(cfg, ts, 0 /* tag=0 base */)
		if e != nil {
			return e
		}
		defer cleanup()
		return runCompactSqls(sqlproc, sqls)
	}
	for d, derr := range idx.ReconstructLiveDocs(cfg.PositionFree) {
		if derr != nil {
			return 0, derr
		}
		if cur == nil {
			cur = NewBuilder(uid, pkType, bopts...)
		}
		for i, w := range d.Terms {
			if e := cur.Add(w, d.Positions[i], d.Pk); e != nil {
				return 0, e
			}
		}
		nlive++
		if ReachedSegmentCap(cur, capacity, postingCap) {
			if e := sealFresh(); e != nil {
				return 0, e
			}
		}
	}
	if e := sealFresh(); e != nil {
		return 0, e
	}
	return nlive, nil
}

func runCompactSql(sqlproc *sqlexec.SqlProcess, sql string) error {
	res, err := sqlexec.RunSql(sqlproc, sql)
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func runCompactSqls(sqlproc *sqlexec.SqlProcess, sqls []string) error {
	for _, s := range sqls {
		if err := runCompactSql(sqlproc, s); err != nil {
			return err
		}
	}
	return nil
}
