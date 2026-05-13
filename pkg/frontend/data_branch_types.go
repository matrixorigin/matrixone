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

const dataBranchHashmapLimitRate = 0.8

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
	lcaEmpty = iota
	lcaOther
	lcaLeft
	lcaRight
)

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

type branchMetaInfo struct {
	lcaType      int
	lcaTableId   uint64
	tarBranchTS  types.TS
	baseBranchTS types.TS

	// pathFromLCAToTar / pathFromLCAToBase carry the full DAG chain
	// from the LCA (lowest common ancestor of tar and base) down to
	// each endpoint. Index 0 is the LCA itself; the last index is
	// the endpoint. pathFromLCAToXxxTS[i] holds the CloneTS of the
	// i-th node (the i-th node was cloned off the (i-1)-th node at
	// that timestamp; pathFromLCAToXxxTS[0] is the LCA's own
	// CloneTS, which is unused for diff purposes — the LCA's own
	// mutation window starts at its creation commit TS instead).
	//
	// Running example. Tree (depth 4, 19 nodes, 10 leaves):
	//
	//                          t0
	//                       /   |   \
	//                     t1   t2   t3
	//                    /  \   |   /  \
	//                  t4   t5 t6  t7   t8
	//                 / \   /\ /\  /\   /\
	//                t9 t10 ... ... ... t18
	//
	// For `data branch diff t9 against t0`:
	//   pathFromLCAToTar  = [t0, t1, t4, t9]
	//   pathFromLCAToBase = [t0]
	// For `data branch diff t9 against t11` (t11 sits under t5):
	//   pathFromLCAToTar  = [t1, t4, t9]
	//   pathFromLCAToBase = [t1, t5, t11]
	// For `data branch diff t9 against t13` (t13 sits under t2→t6):
	//   pathFromLCAToTar  = [t0, t1, t4, t9]
	//   pathFromLCAToBase = [t0, t2, t6, t13]
	//
	// These chains let decideCollectRange iterate every intermediate
	// ancestor (not just the direct child of LCA) and emit one
	// collect segment per ancestor, surfacing inherited mutations
	// that would otherwise be invisible to a single-relation scan.
	pathFromLCAToTar    []uint64
	pathFromLCAToTarTS  []types.TS
	pathFromLCAToBase   []uint64
	pathFromLCAToBaseTS []types.TS
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
