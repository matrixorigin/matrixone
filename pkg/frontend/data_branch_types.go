// Copyright 2025 Matrix Origin
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

package frontend

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/panjf2000/ants/v2"
)

const (
	fakeKind = iota
	normalKind
	compositeKind
)

const (
	diffInsert = "INSERT"
	diffDelete = "DELETE"
	diffUpdate = "UPDATE"
)

const (
	diffSideUnknown = iota
	diffSideTarget
	diffSideBase
)

const (
	dataBranchApplyModeOnlineMerge dataBranchApplyMode = iota
	dataBranchApplyModeOnlinePKOnly
	dataBranchApplyModePortableSQL
)

const dataBranchHashmapLimitRate = 0.8

type dataBranchApplyMode int

type branchHashmapAllocator struct {
	upstream  malloc.Allocator
	throttler rscthrottler.RSCThrottler
}

type branchHashmapDeallocator struct {
	upstream  malloc.Deallocator
	throttler rscthrottler.RSCThrottler
	size      uint64
}

var diffTempTableSeq uint64

const (
	maxSqlBatchCnt   = objectio.BlockMaxRows * 10
	maxSqlBatchSize  = mpool.MB * 32
	defaultRowBytes  = int64(128)
	tombstoneRowMult = int64(3)
	tombstoneRowDiv  = int64(2)
)

type collectRange struct {
	from []types.TS
	end  []types.TS
	rel  []engine.Relation
}

// branchMetaInfo describes the lineage relationship between the two
// endpoints of a `data branch diff` request, expressed entirely as
// the DAG paths from the LCA (lowest common ancestor) down to each
// endpoint.
//
// All downstream consumers (decideCollectRange, hashDiff,
// findDeleteAndUpdateBat, …) derive whatever they need from these
// paths through the helper methods on this struct, so the path
// fields are the single source of truth for diff lineage.
//
// Path encoding. Index 0 is the LCA itself; the last index is the
// endpoint. pathFromLCAToXxxTS[i] is the CloneTS of node i (i.e. the
// moment node i was forked off node i-1). Index 0 holds the LCA's
// own CloneTS, which is unused for diff purposes — the LCA's own
// mutation window always starts at its creation commit TS instead.
//
// lcaTableId == 0 means "no LCA" (two unrelated tables, or
// snapshot-only diff on a single table without branch lineage).
//
// Running example. Tree (depth 4, 19 nodes, 10 leaves):
//
//	          t0
//	       /   |   \
//	     t1   t2   t3
//	    /  \   |   /  \
//	  t4   t5 t6  t7   t8
//	 / \   /\ /\  /\   /\
//	t9 t10 ... ... ... t18
//
// For `data branch diff t9 against t0`:
//
//	pathFromLCAToTar  = [t0, t1, t4, t9]
//	pathFromLCAToBase = [t0]
//
// For `data branch diff t9 against t11` (t11 sits under t5):
//
//	pathFromLCAToTar  = [t1, t4, t9]
//	pathFromLCAToBase = [t1, t5, t11]
//
// For `data branch diff t9 against t13` (t13 sits under t2→t6):
//
//	pathFromLCAToTar  = [t0, t1, t4, t9]
//	pathFromLCAToBase = [t0, t2, t6, t13]
type branchMetaInfo struct {
	lcaTableId uint64

	pathFromLCAToTar    []uint64
	pathFromLCAToTarTS  []types.TS
	pathFromLCAToBase   []uint64
	pathFromLCAToBaseTS []types.TS
}

// hasLCA reports whether tar and base share a common ancestor in the
// branch DAG.
func (m *branchMetaInfo) hasLCA() bool {
	return m.lcaTableId != 0
}

// tarLCASnapshot is the snapshot of the LCA at which tar's view of
// the LCA's state is anchored. It feeds tombstone resolution on the
// tar side: tar's tombstones must be resolved against the LCA at
// this moment.
//
// When tar forked off the LCA (len(pathFromLCAToTar) > 1), the
// anchor is tar's own first-child CloneTS — the moment tar diverged.
// When tar IS the LCA (length-1 path), tar inherits the meeting
// point from base: base's first-child CloneTS, or baseSP if base is
// also the LCA (case-0 same-table diff).
func (m *branchMetaInfo) tarLCASnapshot(baseSP types.TS) types.TS {
	if len(m.pathFromLCAToTar) > 1 {
		return m.pathFromLCAToTarTS[1]
	}
	if len(m.pathFromLCAToBase) > 1 {
		return m.pathFromLCAToBaseTS[1]
	}
	return baseSP
}

// baseLCASnapshot is the mirror of tarLCASnapshot for the base side.
func (m *branchMetaInfo) baseLCASnapshot(tarSP types.TS) types.TS {
	if len(m.pathFromLCAToBase) > 1 {
		return m.pathFromLCAToBaseTS[1]
	}
	if len(m.pathFromLCAToTar) > 1 {
		return m.pathFromLCAToTarTS[1]
	}
	return tarSP
}

// lcaProbeSnapshot is the snapshot used by diffOnBase to fetch the
// LCA relation handle when the LCA is a third-party node (neither
// tar nor base). It is the earlier of the two per-side snapshots so
// the relation handle is valid on both sides' views of the LCA.
func (m *branchMetaInfo) lcaProbeSnapshot(tarSP, baseSP types.TS) types.TS {
	t := m.tarLCASnapshot(baseSP)
	b := m.baseLCASnapshot(tarSP)
	if t.LT(&b) {
		return t
	}
	return b
}

type tableStuff struct {
	tarRel   engine.Relation
	baseRel  engine.Relation
	tarSnap  *plan.Snapshot
	baseSnap *plan.Snapshot

	lcaRel engine.Relation

	def struct {
		colNames     []string     // all columns
		colTypes     []types.Type // all columns
		visibleIdxes []int
		pkColIdx     int
		pkSeqnum     int   // physical column seqnum for PK (for ZoneMap lookup)
		pkColIdxes   []int // expanded pk columns
		pkKind       int
	}

	worker               *ants.Pool
	hashmapAllocator     *branchHashmapAllocator
	maxTombstoneBatchCnt int
	// lcaReaderProbeMode is shared across copies of tableStuff in a single diff
	// request. When enabled, LCA probing skips SQL and directly uses reader
	// fallback.
	lcaReaderProbeMode *atomic.Bool

	retPool *retBatchList

	bufPool *sync.Pool
}

// resolvedSnapshots returns the effective per-side snapshot timestamps:
// the snapshot bound to {tar,base}Snap when present, otherwise the
// session's transaction snapshot. Centralized here so every call site
// (decideCollectRange, diffOnBase, hashDiffIfHasLCA) uses the exact
// same derivation.
func (t *tableStuff) resolvedSnapshots(ses *Session) (tarSP, baseSP types.TS) {
	tarSP = types.TimestampToTS(ses.GetTxnHandler().GetTxn().SnapshotTS())
	baseSP = tarSP
	if t.tarSnap != nil && t.tarSnap.TS != nil {
		tarSP = types.TimestampToTS(*t.tarSnap.TS)
	}
	if t.baseSnap != nil && t.baseSnap.TS != nil {
		baseSP = types.TimestampToTS(*t.baseSnap.TS)
	}
	return
}

type batchWithKind struct {
	name       string
	kind       string
	side       int
	fromUpdate bool
	batch      *batch.Batch
}

type emitFunc func(batchWithKind) (stop bool, err error)

type retBatchList struct {
	mu sync.Mutex
	// 0: data
	// 1: tombstone
	dList []*batch.Batch
	tList []*batch.Batch

	pinned map[*batch.Batch]struct{}

	dataVecCnt    int
	tombVecCnt    int
	dataTypes     []types.Type
	tombstoneType types.Type
	tombRowIDType types.Type
	tombKeyType   types.Type
}

type compositeOption struct {
	conflictOpt           *tree.ConflictOpt
	outputSQL             bool
	expandUpdate          bool
	preservePickConflicts bool
}
