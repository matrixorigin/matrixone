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

// Package idxcron is fulltext's idxcron hook. A retrieval (WAND) index
// participates in scheduled rebuilds (SyncDescriptor.IdxcronAction =
// "fulltext_reindex"); the cron executor runs
// `ALTER … REINDEX … FULLTEXT FORCE_SYNC` when Updatable returns true, which
// rebuilds tag=0 from source and wipes the tag=1 CdcTail. A postings/ngram
// index never registers a task (the compile hook gates on parser).
package idxcron

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext/wand"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// TailChunkThreshold gates the scheduled rebuild on tag=1 CdcTail growth: fire only
// once the tail has accumulated at least this many chunk rows since the last reindex.
// A chunk is ≤ MaxChunkSize (64 KB), so this bounds the delta the search path must
// load+reconcile on top of tag=0 before a compaction folds it in. Default 1024; a
// dev/test deploy can lower it via env MO_IDXCRON_WAND_TAIL_THRESHOLD (e.g. 1) to
// observe a rebuild without generating ~64 MB of tail. (Stage 1 is a full reindex from
// source; Stage 2 will swap the reindex body for tiered merge without changing this gate.)
var TailChunkThreshold = func() int64 {
	if v := os.Getenv("MO_IDXCRON_WAND_TAIL_THRESHOLD"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 1024
}()

type Hooks struct{}

var _ idxcronplugin.Hooks = Hooks{}
var _ idxcronplugin.ReindexOptioner = Hooks{}

// Updatable fires the scheduled reindex when the tag=1 CdcTail has grown past
// TailChunkThreshold. The framework has already applied the auto_update / hour /
// cadence gates; this adds the WAND-specific "is there enough tail to bother"
// check. A non-retrieval index has no WAND store, so it is skipped here too (a
// defensive backstop — such an index should never have a task registered).
func (Hooks) Updatable(in idxcronplugin.UpdatableInput) (bool, string, error) {
	// Cadence backstop (mirrors CuvsUpdatable): skip if rebuilt within the interval.
	if in.LastUpdateAt != nil {
		last := time.Unix(in.LastUpdateAt.Unix(), 0)
		if last.Add(in.Interval).After(time.Now()) {
			return false, fmt.Sprintf("within reindex interval (last %s + %v)",
				last.Format("2006-01-02 15:04:05"), in.Interval), nil
		}
	}

	// Locate the retrieval index's WAND chunk-store table. Absent ⇒ not a
	// retrieval index (postings/ngram) ⇒ nothing to compact.
	var storageTbl string
	for _, idx := range in.TableDef.Indexes {
		if idx.IndexName == in.IndexName &&
			idx.IndexAlgoTableType == catalog.FullTextIndex_TblType_Storage {
			storageTbl = idx.IndexTableName
			break
		}
	}
	if storageTbl == "" {
		return false, "not a retrieval index (no WAND store)", nil
	}

	cfg := wand.TableConfig{DbName: in.TableDef.DbName, IndexTable: storageTbl}
	count, err := wand.CountTailChunks(in.Sqlproc, cfg)
	if err != nil {
		return false, "", err
	}
	logutil.Infof("[idxcron][wand] Updatable index=%s: tag=1 tail chunks=%d threshold=%d",
		in.IndexName, count, TailChunkThreshold)
	if count < TailChunkThreshold {
		return false, fmt.Sprintf("tag=1 tail chunks %d < threshold %d", count, TailChunkThreshold), nil
	}
	return true, "", nil
}

// RebuildDeadPct: when the percentage of dead (deleted-but-not-yet-reclaimed) docs in the
// base exceeds this, the scheduled compaction fires a full REBUILD instead of an incremental
// MERGE — a rebuild reclaims all dead docs AND their tombstones at once, cheaper net than
// folding forever. Hardcoded 30: the alternative (a global merge to reclaim incrementally)
// would load the whole base = O(corpus) resident = OOM, so REBUILD is the accepted reclaim path.
const RebuildDeadPct = 30

// ReindexOption picks the scheduled reindex mode (overriding the descriptor's fixed "MERGE"):
// "MERGE" (incremental fold + tiered compaction) normally, or "" (full REBUILD) once the
// dead-doc fraction — 1 - liveSourceRows/baseDocs — exceeds RebuildDeadPct. Cheap: one
// COUNT(*) on the source table + SUM(nrow) on the metadata table (no postings loaded).
func (Hooks) ReindexOption(in idxcronplugin.UpdatableInput) (string, error) {
	var metaTbl string
	for _, idx := range in.TableDef.Indexes {
		if idx.IndexName == in.IndexName &&
			idx.IndexAlgoTableType == catalog.FullTextIndex_TblType_Metadata {
			metaTbl = idx.IndexTableName
			break
		}
	}
	if metaTbl == "" {
		return "MERGE", nil // not a retrieval index / no metadata table — MERGE is a harmless default
	}
	cfg := wand.TableConfig{DbName: in.TableDef.DbName, MetadataTable: metaTbl}
	baseDocs, err := wand.SumBaseNrow(in.Sqlproc, cfg)
	if err != nil {
		return "", err
	}
	if baseDocs == 0 {
		return "MERGE", nil // no tag=0 base yet (corpus still in the tail) — fold it in
	}
	liveRows, err := countSourceRows(in.Sqlproc, in.TableDef.DbName, in.TableDef.Name)
	if err != nil {
		return "", err
	}
	opt := reindexOptionForCounts(liveRows, baseDocs)
	deadPct := int64(0)
	if baseDocs > 0 {
		deadPct = 100 - liveRows*100/baseDocs
	}
	logutil.Infof("[idxcron][wand] ReindexOption index=%s: liveRows=%d baseDocs=%d dead=%d%% threshold=%d%% -> %s",
		in.IndexName, liveRows, baseDocs, deadPct, RebuildDeadPct, optLabel(opt))
	return opt, nil
}

// reindexOptionForCounts is the pure decision: "" (full REBUILD) once the dead-doc percentage
// exceeds RebuildDeadPct, else "MERGE" (incremental). Integer arithmetic so the boundary is
// exact (dead% > 30 ⟺ live% < 70 ⟺ liveRows*100 < baseDocs*(100-30)); float 1-live/base tips
// at the exact boundary due to rounding.
func reindexOptionForCounts(liveRows, baseDocs int64) string {
	if baseDocs <= 0 {
		return "MERGE"
	}
	if liveRows*100 < baseDocs*(100-RebuildDeadPct) {
		return "" // dead % > RebuildDeadPct → full REBUILD
	}
	return "MERGE"
}

func optLabel(opt string) string {
	if opt == "" {
		return "REBUILD"
	}
	return opt
}

// countSourceRows returns the source table's live row count (= live docs, since a DELETE
// removes the source row). Compared to SumBaseNrow to estimate the base's dead-doc fraction.
func countSourceRows(sqlproc *sqlexec.SqlProcess, db, table string) (int64, error) {
	res, err := sqlexec.RunSql(sqlproc, fmt.Sprintf("SELECT COUNT(*) FROM %s", sqlquote.QualifiedIdent(db, table)))
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
