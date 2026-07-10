// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/bm25/wand"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// bm25CompactState runs a WAND retrieval-index tiered merge-compaction as
// a standalone table function: `SELECT * FROM bm25_compact(db, store, meta)`
// (no CROSS APPLY / no driving table). It reads the three identifying args in
// start() and, in end(), folds the tag=0 base + the tag=1 CdcTail into a fresh,
// capacity-split tag=0 base and deletes the inputs — via wand.CompactSegments, in
// the statement's transaction. Reached from idxcron / ALTER … REINDEX … FULLTEXT
// MERGE. Its output is a single discarded status row (mirrors bm25_create).
type bm25CompactState struct {
	inited   bool
	tblcfg   wand.TableConfig
	capacity int64
	batch    *batch.Batch
}

func (u *bm25CompactState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *bm25CompactState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
	return vm.CancelResult, nil
}

func (u *bm25CompactState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

// start reads the four varchar args once — [0]=db, [1]=store table, [2]=metadata
// table, [3]=max_index_capacity — into the TableConfig + capacity the compaction runs
// against. Capacity is passed explicitly (resolved by the compile layer from the index's
// persisted algo_params) rather than resolved here, so a manual MERGE and the background
// idxcron MERGE always use the SAME build-time capacity regardless of session.
func (u *bm25CompactState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
	if u.inited {
		return nil
	}
	for i := 0; i < 4; i++ {
		v := tf.ctr.argVecs[i]
		if v.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "bm25_compact: args (db, store, meta, capacity) must be strings")
		}
		if !v.IsConst() {
			return moerr.NewInternalError(proc.Ctx, "bm25_compact: args must be string constants")
		}
	}
	u.tblcfg = wand.TableConfig{
		DbName:        tf.ctr.argVecs[0].UnsafeGetStringAt(0),
		IndexTable:    tf.ctr.argVecs[1].UnsafeGetStringAt(0),
		MetadataTable: tf.ctr.argVecs[2].UnsafeGetStringAt(0),
	}
	if u.tblcfg.DbName == "" || u.tblcfg.IndexTable == "" || u.tblcfg.MetadataTable == "" {
		return moerr.NewInternalError(proc.Ctx, "bm25_compact: db/store/meta must be non-empty")
	}
	cap, err := strconv.ParseInt(tf.ctr.argVecs[3].UnsafeGetStringAt(0), 10, 64)
	if err != nil {
		return moerr.NewInvalidInput(proc.Ctx, "bm25_compact: capacity must be an integer")
	}
	u.capacity = cap
	u.batch = tf.createResultBatch()
	u.inited = true
	return nil
}

// end runs the tiered merge-compaction in the statement transaction.
func (u *bm25CompactState) end(tf *TableFunction, proc *process.Process) error {
	if !u.inited {
		return nil
	}
	sqlproc := sqlexec.NewSqlProcess(proc)

	// capacity was resolved by the compile layer from the index's persisted algo_params
	// (the immutable max_index_capacity flat param) and passed in as arg[3], so fold-split
	// and tiered-merge fullness always match what the base was built with.
	if _, err := wand.CompactSegments(sqlproc, u.tblcfg, u.capacity); err != nil {
		return err
	}
	// The tag=0 base changed — evict any cached search index so the next query
	// reloads the merged base instead of the stale one held until the TTL.
	veccache.Cache.Remove(u.tblcfg.IndexTable)
	return nil
}

func bm25CompactPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	if len(arg.Args) != 4 {
		return nil, moerr.NewInvalidInput(proc.Ctx, "bm25_compact: expects 4 args (db, store, meta, capacity)")
	}
	var err error
	st := &bm25CompactState{}
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))
	return st, err
}
