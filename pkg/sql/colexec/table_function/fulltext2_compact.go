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

// fulltext2_compact is the standalone MERGE-compaction TVF (mirrors bm25_compact):
// SELECT * FROM fulltext2_compact(db, store, meta, capacity). It folds the tag=1
// CdcTail into the tag=0 base and drops dead docs in one txn, driving
// fulltext2.CompactSegments. No input rows; its own output is discarded.
package table_function

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type fulltext2CompactState struct {
	inited   bool
	tblcfg   fulltext2.TableConfig
	capacity int64
	batch    *batch.Batch
}

func (u *fulltext2CompactState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *fulltext2CompactState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
	return vm.CancelResult, nil
}

func (u *fulltext2CompactState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}
}

func (u *fulltext2CompactState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
	if u.inited {
		return nil
	}
	for i := 0; i < 4; i++ {
		v := tf.ctr.argVecs[i]
		if v.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "fulltext2_compact: args (db, store, meta, capacity) must be strings")
		}
		if !v.IsConst() {
			return moerr.NewInternalError(proc.Ctx, "fulltext2_compact: args must be string constants")
		}
	}
	u.tblcfg = fulltext2.TableConfig{
		DbName:        tf.ctr.argVecs[0].UnsafeGetStringAt(0),
		IndexTable:    tf.ctr.argVecs[1].UnsafeGetStringAt(0),
		MetadataTable: tf.ctr.argVecs[2].UnsafeGetStringAt(0),
	}
	if u.tblcfg.DbName == "" || u.tblcfg.IndexTable == "" || u.tblcfg.MetadataTable == "" {
		return moerr.NewInternalError(proc.Ctx, "fulltext2_compact: db/store/meta must be non-empty")
	}
	c, err := strconv.ParseInt(tf.ctr.argVecs[3].UnsafeGetStringAt(0), 10, 64)
	if err != nil {
		return moerr.NewInvalidInput(proc.Ctx, "fulltext2_compact: capacity must be an integer")
	}
	u.capacity = c
	u.batch = tf.createResultBatch()
	u.inited = true
	return nil
}

// end runs the compaction after the (empty) input is consumed — one txn folds the
// tail into the base and drops dead docs.
func (u *fulltext2CompactState) end(tf *TableFunction, proc *process.Process) error {
	if !u.inited {
		return nil
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	if _, err := fulltext2.CompactSegments(sqlproc, u.tblcfg, u.capacity); err != nil {
		return err
	}
	// The tag=0 base changed (tail folded into a fresh merged base) — evict any cached
	// search index so the next query reloads the merged base instead of the stale one
	// held until the TTL.
	veccache.Cache.Remove(u.tblcfg.IndexTable)
	return nil
}

func fulltext2CompactPrepare(proc *process.Process, arg *TableFunction) (tvfState, error) {
	if len(arg.Args) != 4 {
		return nil, moerr.NewInvalidInput(proc.Ctx, "fulltext2_compact: expects 4 args (db, store, meta, capacity)")
	}
	var err error
	st := &fulltext2CompactState{}
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.argVecs = make([]*vector.Vector, len(arg.Args))
	return st, err
}
