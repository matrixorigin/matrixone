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

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

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
