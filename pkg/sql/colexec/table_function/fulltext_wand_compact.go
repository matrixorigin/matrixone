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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext/wand"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// fulltextWandCompactState runs a WAND retrieval-index tiered merge-compaction as
// a standalone table function: `SELECT * FROM fulltext_wand_compact(db, store, meta)`
// (no CROSS APPLY / no driving table). It reads the three identifying args in
// start() and, in end(), folds the tag=0 base + the tag=1 CdcTail into a fresh,
// capacity-split tag=0 base and deletes the inputs — via wand.CompactSegments, in
// the statement's transaction. Reached from idxcron / ALTER … REINDEX … FULLTEXT
// MERGE. Its output is a single discarded status row (mirrors fulltext_wand_create).
type fulltextWandCompactState struct {
	inited bool
	tblcfg wand.TableConfig
	batch  *batch.Batch
}

func (u *fulltextWandCompactState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *fulltextWandCompactState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
	return vm.CancelResult, nil
}

func (u *fulltextWandCompactState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

// start reads the three varchar args once — [0]=db, [1]=store table, [2]=metadata
// table — into the TableConfig the compaction runs against.
func (u *fulltextWandCompactState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
	if u.inited {
		return nil
	}
	for i := 0; i < 3; i++ {
		v := tf.ctr.argVecs[i]
		if v.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "fulltext_wand_compact: args (db, store, meta) must be strings")
		}
		if !v.IsConst() {
			return moerr.NewInternalError(proc.Ctx, "fulltext_wand_compact: args must be string constants")
		}
	}
	u.tblcfg = wand.TableConfig{
		DbName:        tf.ctr.argVecs[0].UnsafeGetStringAt(0),
		IndexTable:    tf.ctr.argVecs[1].UnsafeGetStringAt(0),
		MetadataTable: tf.ctr.argVecs[2].UnsafeGetStringAt(0),
	}
	if u.tblcfg.DbName == "" || u.tblcfg.IndexTable == "" || u.tblcfg.MetadataTable == "" {
		return moerr.NewInternalError(proc.Ctx, "fulltext_wand_compact: db/store/meta must be non-empty")
	}
	u.batch = tf.createResultBatch()
	u.inited = true
	return nil
}

// end runs the tiered merge-compaction in the statement transaction.
func (u *fulltextWandCompactState) end(tf *TableFunction, proc *process.Process) error {
	if !u.inited {
		return nil
	}
	sqlproc := sqlexec.NewSqlProcess(proc)

	// max_index_capacity via the resolver — the live session var for a manual MERGE,
	// or the captured session_vars overlay for the background idxcron reindex (same
	// path as fulltext_wand_create). Unresolved / 0 => a single unbounded base.
	capacity := int64(0)
	if rf := sqlproc.GetResolveVariableFunc(); rf != nil {
		if v, verr := rf("fulltext_max_index_capacity", true, false); verr == nil {
			if c, ok := v.(int64); ok {
				capacity = c
			}
		}
	}

	if _, err := wand.CompactSegments(sqlproc, u.tblcfg, capacity); err != nil {
		return err
	}
	// The tag=0 base changed — evict any cached search index so the next query
	// reloads the merged base instead of the stale one held until the TTL.
	veccache.Cache.Remove(u.tblcfg.IndexTable)
	return nil
}

func fulltextWandCompactPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	var err error
	st := &fulltextWandCompactState{}
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))
	if len(arg.Args) != 3 {
		return nil, moerr.NewInvalidInput(proc.Ctx, "fulltext_wand_compact: expects 3 args (db, store, meta)")
	}
	return st, err
}
