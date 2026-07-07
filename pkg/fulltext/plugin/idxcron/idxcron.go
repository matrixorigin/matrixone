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
	"github.com/matrixorigin/matrixone/pkg/fulltext/wand"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
