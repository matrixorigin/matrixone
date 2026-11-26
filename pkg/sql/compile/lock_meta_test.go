// Copyright 2021 - 2026 Matrix Origin
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

package compile

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestLockMetaKeyBatches(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	keys := vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { keys.Free(mp) })
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, vector.AppendBytes(keys, []byte(key), false, mp))
	}
	collect := func(stopAt string) ([]string, error) {
		var batches []string
		err := lockMetaKeyBatches(keys, 2, mp, func(window *vector.Vector) error {
			var batch string
			for i := 0; i < window.Length(); i++ {
				batch += string(window.GetBytesAt(i))
			}
			batches = append(batches, batch)
			if batch == stopAt {
				return context.Canceled
			}
			return nil
		})
		return batches, err
	}
	batches, err := collect("")
	require.NoError(t, err)
	require.Equal(t, []string{"ab", "cd", "e"}, batches)
	batches, err = collect("cd")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{"ab", "cd"}, batches)
}

// Optimistic LockRows is inert: this fixture isolates key ownership and catalog
// handle rebinding. Public pessimistic tests prove actual lock admission.
func newLockMetaTestOwner(t testing.TB, account uint32) (*LockMeta, *process.Process, engine.Engine, *int, *error) {
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	proc := process.NewTopProcess(defines.AttachAccountId(context.Background(), account), mpool.MustNewZero(), nil, op, nil, nil, nil, nil, nil, nil, nil)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, gomock.Any()).Return(db, nil).AnyTimes()
	calls := new(int)
	resetErr := new(error)
	for i, name := range []string{catalog.MO_DATABASE, catalog.MO_TABLES} {
		rel := mock_frontend.NewMockRelation(ctrl)
		db.EXPECT().Relation(gomock.Any(), name, gomock.Any()).Return(rel, nil).AnyTimes()
		rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(i + 1)).AnyTimes()
		rel.EXPECT().Reset(gomock.Any()).DoAndReturn(func(client.TxnOperator) error { *calls++; return *resetErr }).AnyTimes()
		rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{DbName: catalog.MO_CATALOG}).AnyTimes()
		rel.EXPECT().GetTableName().Return(name).AnyTimes()
	}
	l := NewLockMeta()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(proc.Mp())
		l.clear(proc)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	return l, proc, eng, calls, resetErr
}

func unpackLockMetaKeys(t *testing.T, vec *vector.Vector) [][]interface{} {
	t.Helper()
	keys := make([][]interface{}, 0, vec.Length())
	for i := 0; i < vec.Length(); i++ {
		key, err := types.Unpack(vec.GetBytesAt(i))
		require.NoError(t, err)
		row := make([]interface{}, len(key))
		for j, value := range key {
			if text, ok := value.([]byte); ok {
				row[j] = string(text)
			} else {
				row[j] = value
			}
		}
		keys = append(keys, row)
	}
	return keys
}

func TestLockReadPrePipelineCatalogTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		stmt tree.Statement
		typ  plan.Query_StatementType
	}{
		{name: "locking select", stmt: &tree.Select{}, typ: plan.Query_SELECT},
		{name: "update", stmt: &tree.Update{}, typ: plan.Query_UPDATE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta, proc, eng, _, _ := newLockMetaTestOwner(t, 7)
			meta.appendMetaTables(&plan.ObjectRef{SchemaName: "db", ObjName: "t"})
			root := resource.NewRoot(resource.ConnExternal)
			root.EnableLockRead()
			proc.ReplaceTopCtx(resource.ContextWithRoot(proc.GetTopContext(), root))

			ctrl := gomock.NewController(t)
			op := mock_frontend.NewMockTxnOperator(ctrl)
			txnID := []byte{1}
			op.EXPECT().Txn().Return(txn.TxnMeta{Mode: txn.TxnMode_Pessimistic, ID: txnID}).AnyTimes()
			op.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
			op.EXPECT().NextSequence().Return(uint64(1))
			op.EXPECT().CreateTS().Return(timestamp.Timestamp{})
			op.EXPECT().AddWaitLock(uint64(2), gomock.Any(), gomock.Any()).Return(uint64(1))
			op.EXPECT().RemoveWaitLock(uint64(1))
			proc.Base.TxnOperator = op

			service := mock_lock.NewMockLockService(ctrl)
			service.EXPECT().GetConfig().Return(lockservice.Config{MaxLockRowCount: 16}).AnyTimes()
			service.EXPECT().GetServiceID().Return("")
			holder := []byte("catalog-lock-txn")
			service.EXPECT().Lock(gomock.Any(), uint64(2), gomock.Any(), txnID, gomock.Any()).
				DoAndReturn(func(ctx context.Context, _ uint64, keys [][]byte, _ []byte, opts lock.LockOptions) (lock.Result, error) {
					require.Equal(t, lock.LockMode_Shared, opts.Mode)
					require.Len(t, keys, 1)
					resource.ObserveLocalLockConflict(ctx, [][]byte{holder})
					return lock.Result{}, lockservice.ErrLockTimeout
				})
			proc.Base.LockService = service

			c := &Compile{
				proc: proc, e: eng, lockMeta: meta, stmt: tc.stmt, counterSet: &perfcounter.CounterSet{},
				pn: &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
					StmtType: tc.typ, Nodes: []*plan.Node{{NodeType: plan.Node_LOCK_OP}},
				}}},
				resourceAttemptOwnerEligible: true,
			}
			c.InitPipelineContextToExecuteQuery()
			require.ErrorIs(t, c.prePipelineInitializer(), lockservice.ErrLockTimeout)

			_, snapshot := root.SealWithLockRead(uint64(100 * time.Millisecond))
			require.Equal(t, tc.typ == plan.Query_SELECT, snapshot.Qualifies(100*time.Millisecond, time.Second))
			d := snapshot.Diagnostic([16]byte{}, 1, true)
			if tc.typ == plan.Query_SELECT {
				require.Positive(t, d.AcquisitionNS)
				require.Equal(t, "acquisition", d.LastIncomplete)
				require.True(t, d.Conflict)
				require.Equal(t, fmt.Sprintf("%x", holder), d.ObservedConflictTxnID)
				require.Zero(t, d.LogtailNS)
				require.Zero(t, d.PKCheckNS)
			} else {
				require.Zero(t, d.Seen)
			}
		})
	}
}

type failingLockMetaExpression struct {
	colexec.ExpressionExecutor
	fail bool
}

func (e *failingLockMetaExpression) Eval(p *process.Process, b []*batch.Batch, s []bool) (*vector.Vector, error) {
	if e.fail {
		e.fail = false
		return nil, moerr.NewInternalErrorNoCtx("injected serial failure")
	}
	return e.ExpressionExecutor.Eval(p, b, s)
}

func TestLockMetaKeyReuseAndFailure(t *testing.T) {
	for _, account := range []uint32{0, 7} {
		t.Run(fmt.Sprint(account), func(t *testing.T) {
			l, p, e, resets, resetErr := newLockMetaTestOwner(t, account)
			require.NoError(t, l.doLock(e, p))
			require.Nil(t, l.lockTableVec)
			l.appendMetaTables(&plan.ObjectRef{SchemaName: "a", ObjName: "t"})
			require.NoError(t, l.doLock(e, p))
			require.Equal(t, [][]interface{}{{account, "a", "t"}}, unpackLockMetaKeys(t, l.lockTableVec))
			require.Zero(t, l.lockDbVec.Length())
			savedTable, savedDb := l.lockTableVec, l.lockDbVec
			l.appendMetaTables(&plan.ObjectRef{SchemaName: "a", ObjName: "t"})
			l.appendMetaTables(&plan.ObjectRef{SchemaName: catalog.MO_CATALOG, ObjName: catalog.MO_TABLES})
			require.Same(t, savedTable, l.lockTableVec)
			require.Same(t, savedDb, l.lockDbVec)
			tableExe, dbExe := l.lockTableExe, l.lockDbExe
			tableProbe := &failingLockMetaExpression{tableExe, true}
			dbProbe := &failingLockMetaExpression{dbExe, true}
			l.lockTableExe, l.lockDbExe = tableProbe, dbProbe
			l.reset(p)
			require.NoError(t, l.doLock(e, p))
			require.True(t, tableProbe.fail && dbProbe.fail, "warm admission must not evaluate either key expression")
			l.lockTableExe, l.lockDbExe = tableExe, dbExe
			require.Equal(t, 2, *resets)
			*resetErr = moerr.NewInternalErrorNoCtx("reset failed")
			require.ErrorIs(t, l.doLock(e, p), *resetErr)
			require.Equal(t, 3, *resets)
			*resetErr = nil
			l.appendMetaTables(&plan.ObjectRef{SchemaName: "b", ObjName: "__mo_index_unique_t"})
			require.Nil(t, l.lockTableVec)
			require.Nil(t, l.lockDbVec)
			for _, failure := range []string{"table", "database"} {
				t.Run(failure, func(t *testing.T) {
					l.lockTableVec, l.lockDbVec = nil, nil
					if failure == "table" {
						l.lockTableExe = &failingLockMetaExpression{l.lockTableExe, true}
					} else {
						l.lockDbExe = &failingLockMetaExpression{l.lockDbExe, true}
					}
					require.ErrorContains(t, l.buildLockVectors(p, account), "injected serial failure")
					require.Nil(t, l.lockTableVec)
					require.Nil(t, l.lockDbVec)
					require.NoError(t, l.buildLockVectors(p, account))
					require.Equal(t, [][]interface{}{{account, "a", "t"}, {account, "b", "__mo_index_unique_t"}}, unpackLockMetaKeys(t, l.lockTableVec))
					require.ElementsMatch(t, [][]interface{}{{account, "a"}, {account, "b"}}, unpackLockMetaKeys(t, l.lockDbVec))
				})
			}
			l.clearInitializedState()
			require.Nil(t, l.lockTableVec)
			require.Nil(t, l.lockDbVec)
			require.NoError(t, l.doLock(e, p))
			require.Len(t, unpackLockMetaKeys(t, l.lockDbVec), 2)
		})
	}
}

func BenchmarkLockMetaWarmAdmissionPreparation(b *testing.B) {
	for _, n := range []int{1, 2, 8} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			l, p, e, _, _ := newLockMetaTestOwner(b, 0)
			for i := 0; i < n; i++ {
				l.appendMetaTables(&plan.ObjectRef{SchemaName: fmt.Sprintf("db%d", i%2), ObjName: fmt.Sprintf("t%d", i)})
			}
			if err := l.doLock(e, p); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := l.doLock(e, p); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
