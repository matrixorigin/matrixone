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

// Package idxcron is the fulltext2 index's scheduled-compaction hook (mirrors
// bm25's). fulltext2 participates in scheduled reindex
// (SyncDescriptor.IdxcronAction = "fulltext2_reindex"); the cron executor runs
// `ALTER … REINDEX … FULLTEXT2 [MERGE] FORCE_SYNC` when Updatable fires — MERGE
// folds the tag=1 CdcTail into the tag=0 base, or a full REBUILD once the dead-doc
// fraction is high (ReindexOption).
package idxcron

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// TailChunkThreshold gates the scheduled compaction on tag=1 CdcTail growth: fire
// only once the tail has at least this many chunk rows since the last reindex. A
// dev/test deploy can lower it via MO_IDXCRON_FULLTEXT2_TAIL_THRESHOLD (e.g. 1).
// Mirrors bm25's TailChunkThreshold.
var TailChunkThreshold = func() int64 {
	if v := os.Getenv("MO_IDXCRON_FULLTEXT2_TAIL_THRESHOLD"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 1024
}()

type Hooks struct{}

var _ idxcronplugin.Hooks = Hooks{}
var _ idxcronplugin.ReindexOptioner = Hooks{}

// Updatable fires the scheduled compaction once the tag=1 CdcTail has grown past
// TailChunkThreshold (the framework already applied the auto_update/hour/cadence
// gates). Mirrors bm25's Updatable.
func (Hooks) Updatable(in idxcronplugin.UpdatableInput) (bool, string, error) {
	if in.LastUpdateAt != nil {
		last := time.Unix(in.LastUpdateAt.Unix(), 0)
		if last.Add(in.Interval).After(time.Now()) {
			return false, fmt.Sprintf("within reindex interval (last %s + %v)",
				last.Format("2006-01-02 15:04:05"), in.Interval), nil
		}
	}

	var storageTbl string
	for _, idx := range in.TableDef.Indexes {
		if idx.IndexName == in.IndexName && idx.IndexAlgoTableType == catalog.FullText2Index_TblType_Storage {
			storageTbl = idx.IndexTableName
			break
		}
	}
	if storageTbl == "" {
		return false, "not a fulltext2 index (no storage table)", nil
	}

	cfg := fulltext2.TableConfig{DbName: in.TableDef.DbName, IndexTable: storageTbl}
	count, err := fulltext2.CountTailChunks(in.Sqlproc, cfg)
	if err != nil {
		return false, "", err
	}
	logutil.Infof("[idxcron][ftv2] Updatable index=%s: tag=1 tail chunks=%d threshold=%d",
		in.IndexName, count, TailChunkThreshold)
	if count < TailChunkThreshold {
		return false, fmt.Sprintf("tag=1 tail chunks %d < threshold %d", count, TailChunkThreshold), nil
	}
	return true, "", nil
}

// RebuildDeadPct: once the dead-doc percentage in the base exceeds this, the
// scheduled compaction fires a full REBUILD instead of an incremental MERGE.
const RebuildDeadPct = 30

// ReindexOption picks the scheduled reindex mode: "MERGE" normally, "" (full
// REBUILD) once the dead-doc fraction (1 - liveSourceRows/baseDocs) exceeds
// RebuildDeadPct. Mirrors bm25's ReindexOption.
func (Hooks) ReindexOption(in idxcronplugin.UpdatableInput) (string, error) {
	var metaTbl string
	for _, idx := range in.TableDef.Indexes {
		if idx.IndexName == in.IndexName && idx.IndexAlgoTableType == catalog.FullText2Index_TblType_Metadata {
			metaTbl = idx.IndexTableName
			break
		}
	}
	if metaTbl == "" {
		return "MERGE", nil
	}
	cfg := fulltext2.TableConfig{DbName: in.TableDef.DbName, MetadataTable: metaTbl}
	baseDocs, err := fulltext2.SumBaseNrow(in.Sqlproc, cfg)
	if err != nil {
		return "", err
	}
	if baseDocs == 0 {
		return "MERGE", nil
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
	logutil.Infof("[idxcron][ftv2] ReindexOption index=%s: liveRows=%d baseDocs=%d dead=%d%% threshold=%d%% -> %s",
		in.IndexName, liveRows, baseDocs, deadPct, RebuildDeadPct, optLabel(opt))
	return opt, nil
}

// reindexOptionForCounts is the pure decision: "" (REBUILD) once dead% >
// RebuildDeadPct, else "MERGE". Integer arithmetic for an exact boundary.
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

// countSourceRows returns the source table's live row count (= live docs).
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
