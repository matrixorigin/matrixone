// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exercise SQL -> TAE Flush/transfer -> logtail -> reader with four versions of
// one compound PK. Holding three Flush commits is controlled task scheduling,
// not a claim about the automatic flusher's admission order or frequency.
func TestExactPrimaryKeyPendingFlushes(t *testing.T) {
	p := testutil.InitEnginePack(testutil.TestOptions{
		TaeEngineOptions: config.WithLongScanAndCKPOpts(nil), Timeout: time.Minute,
	}, t)
	t.Cleanup(p.Close)
	ctx := p.Ctx
	newTxn := func() client.TxnOperator {
		op := p.StartCNTxn(client.WithTxnIsolation(pbtxn.TxnIsolation_SI))
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = op.Rollback(cleanup)
		})
		return op
	}
	value, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)
	exec := value.(executor.SQLExecutor)
	execWrite := func(sql string) {
		t.Helper()
		op := newTxn()
		res, err := exec.Exec(ctx, sql, executor.Options{}.WithTxn(op))
		defer res.Close()
		require.NoError(t, err)
		require.Equal(t, uint64(1), res.AffectedRows)
		require.NoError(t, op.Commit(ctx))
	}

	schema := catalog.NewEmptySchema("offsets")
	for _, col := range []struct {
		name string
		typ  types.Type
	}{
		{"table_id", types.T_uint64.ToType()},
		{"col_name", types.New(types.T_varchar, 64, 0)},
		{"offset", types.T_uint64.ToType()},
		{"step", types.T_uint64.ToType()},
	} {
		require.NoError(t, schema.AppendCol(col.name, col.typ))
		schema.ColDefs[len(schema.ColDefs)-1].NullAbility = false
	}
	require.NoError(t, schema.AppendPKCol(pkgcatalog.CPrimaryKeyColName, types.T_varchar.ToType(), 0))
	schema.ColDefs[len(schema.ColDefs)-1].Hidden = true
	constraint := &engine.ConstraintDef{Cts: []engine.Constraint{&engine.PrimaryKeyDef{Pkey: &plan.PrimaryKeyDef{
		Names: []string{"table_id", "col_name"}, PkeyColName: pkgcatalog.CPrimaryKeyColName,
		CompPkeyCol: &plan.ColDef{Name: pkgcatalog.CPrimaryKeyColName, Typ: plan.Type{Id: int32(types.T_varchar)}, Primary: true, Hidden: true},
	}}}}
	var err error
	schema.Constraint, err = constraint.MarshalBinary()
	require.NoError(t, err)
	schema.Extra.BlockMaxRows, schema.Extra.ObjectMaxBlocks = 1, 1
	require.NoError(t, schema.Finalize(false))
	create := newTxn()
	p.CreateDBAndTable(create, "exact_transfer", schema)
	require.NoError(t, create.Commit(ctx))
	execWrite("insert into exact_transfer.offsets values(399944,'a',0,1)")
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeUint64(399944)
	packer.EncodeStringType([]byte("a"))
	filter := handle.NewEQFilter(packer.GetBuf())
	flushes := make([]txnif.AsyncTxn, 0, 3)
	committed := make([]bool, 3)
	sources := make(map[objectio.ObjectId]struct{})
	for round := 0; round < 3; round++ {
		flush, err := p.T.StartTxn()
		require.NoError(t, err)
		t.Cleanup(func() {
			if !committed[round] {
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = flush.Rollback(cleanup)
			}
		})
		db, err := flush.GetDatabase("exact_transfer")
		require.NoError(t, err)
		rel, err := db.GetRelationByName(schema.Name)
		require.NoError(t, err)
		id, _, err := rel.GetByFilter(ctx, filter)
		require.NoError(t, err)
		obj, err := rel.GetObject(id.ObjectID(), false)
		require.NoError(t, err)
		meta := obj.GetMeta().(*catalog.ObjectEntry)
		require.NoError(t, obj.Close())
		_, seen := sources[*meta.ID()]
		require.False(t, seen)
		sources[*meta.ID()] = struct{}{}
		require.True(t, meta.GetObjectData().PrepareCompact())
		// No tombstone objects are passed; transferred deletes still originate
		// from actual updates and Flush commit, not injected logtail entries.
		task, err := jobs.NewFlushTableTailTask(nil, flush, []*catalog.ObjectEntry{meta}, nil, p.T.GetDB().Runtime)
		require.NoError(t, err)
		require.NoError(t, task.OnExec(ctx))
		flushes = append(flushes, flush)
		execWrite(fmt.Sprintf("update exact_transfer.offsets set offset=%d where table_id=399944 and col_name='a'", round+1))
	}
	queries := []string{
		"select offset,step from exact_transfer.offsets where table_id=399944 and col_name='a' for update",
		"select offset,step from exact_transfer.offsets where table_id=399944 and col_name='a'",
		"select offset,step from exact_transfer.offsets where table_id=399944",
		"select offset,step from exact_transfer.offsets",
	}
	check := func(phase string) {
		t.Helper()
		op := newTxn()
		before := op.SnapshotTS()
		err := exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			for _, sql := range queries {
				res, err := tx.Exec(sql, executor.StatementOption{})
				if err != nil {
					res.Close()
					return err
				}
				var rows [][2]uint64
				for _, bat := range res.Batches {
					for i := 0; i < bat.RowCount(); i++ {
						rows = append(rows, [2]uint64{vector.GetFixedAtNoTypeCheck[uint64](bat.Vecs[0], i), vector.GetFixedAtNoTypeCheck[uint64](bat.Vecs[1], i)})
					}
				}
				res.Close()
				t.Logf("%s: %s => %v", phase, sql, rows)
				assert.Equal(t, [][2]uint64{{3, 1}}, rows, "%s: %s", phase, sql)
				assert.Equal(t, before, op.SnapshotTS())
			}
			return nil
		}, executor.Options{}.WithTxn(op))
		require.NoError(t, err)
		require.NoError(t, op.Rollback(ctx))
	}
	check("before flush commits")
	for i, flush := range flushes {
		require.NoError(t, flush.Commit(ctx))
		committed[i] = true
		_, err := p.T.GetDB().LogtailMgr.ReadBarrier(ctx)
		require.NoError(t, err)
		check(fmt.Sprintf("after flush %d", i))
	}
}
