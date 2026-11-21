// Copyright 2024 Matrix Origin
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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mockfrontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mocklock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/stretchr/testify/require"
)

func TestLockReadQueryContextUsesCurrentSelectOwner(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	root.EnableLockRead()
	ctx := resource.ContextWithRoot(context.Background(), root)
	selectPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT}}}
	selectCompile := &Compile{stmt: &tree.Select{}, pn: selectPlan, resourceAttemptOwnerEligible: true}
	query := selectCompile.lockReadQueryContext(ctx)
	require.Same(t, root, resource.LockReadRootFromContext(query))

	for _, c := range []*Compile{
		{stmt: &tree.Update{}, pn: selectPlan, resourceAttemptOwnerEligible: true},
		{stmt: &tree.Delete{}, pn: selectPlan, resourceAttemptOwnerEligible: true},
		{stmt: &tree.Select{}, pn: selectPlan, resourceAttemptOwnerEligible: true, isInternal: true},
		{stmt: &tree.Select{}, pn: selectPlan},
		{stmt: &tree.Select{}, resourceAttemptOwnerEligible: true},
		{stmt: &tree.Select{}, pn: &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_UPDATE}}}, resourceAttemptOwnerEligible: true},
	} {
		require.Nil(t, resource.LockReadRootFromContext(c.lockReadQueryContext(query)))
	}
	require.Nil(t, resource.LockReadRootFromContext(selectCompile.lockReadQueryContext(context.Background())))
	nextRoot := resource.NewRoot(resource.ConnExternal)
	nextRoot.EnableLockRead()
	next := resource.ContextWithRoot(query, nextRoot)
	require.Same(t, nextRoot, resource.LockReadRootFromContext(selectCompile.lockReadQueryContext(next)))
}

func TestLockReadRecordsWaitAfterRecompile(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rebuild func(*Compile) (*Compile, error)
	}{
		{name: "snapshot refresh", rebuild: func(c *Compile) (*Compile, error) {
			return c.buildRetryCompile(false)
		}},
		{name: "definition change", rebuild: func(c *Compile) (*Compile, error) {
			return c.buildRetryCompile(true)
		}},
		{name: "import memory rebuild", rebuild: func(c *Compile) (*Compile, error) {
			return c.recompileImportMemory(c.proc.GetTopContext(), 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc := plan2.NewMockCompilerContext(true)
			proc := cc.GetProcess()
			t.Cleanup(proc.Free)
			root := resource.NewRoot(resource.ConnExternal)
			root.EnableLockRead()
			ctx := resource.ContextWithRoot(defines.AttachAccountId(cc.GetContext(), 7), root)
			const sql = "select n_nationkey from nation where n_nationkey = 1 for update"
			stmt, err := mysql.ParseOne(ctx, sql, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)

			ctrl := gomock.NewController(t)
			txnID := []byte{1}
			op := mockfrontend.NewMockTxnOperator(ctrl)
			op.EXPECT().Txn().Return(txn.TxnMeta{ID: txnID, Mode: txn.TxnMode_Pessimistic, Isolation: txn.TxnIsolation_RC}).AnyTimes()
			op.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
			op.EXPECT().NextSequence().Return(uint64(1)).AnyTimes()
			op.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
			op.EXPECT().CreateTS().Return(timestamp.Timestamp{})
			const catalogTableID uint64 = 2
			op.EXPECT().AddWaitLock(catalogTableID, gomock.Any(), gomock.Any()).Return(uint64(1))
			op.EXPECT().RemoveWaitLock(uint64(1))
			proc.Base.TxnOperator = op

			service := mocklock.NewMockLockService(ctrl)
			service.EXPECT().GetConfig().Return(lockservice.Config{MaxLockRowCount: 16}).AnyTimes()
			service.EXPECT().GetServiceID().Return("")
			service.EXPECT().Lock(gomock.Any(), catalogTableID, gomock.Any(), txnID, gomock.Any()).
				Return(lock.Result{}, lockservice.ErrLockTimeout)
			proc.Base.LockService = service

			eng := newStubEngineForGenerateNodes("tpch", "nation")
			eng.dbs[catalog.MO_CATALOG].rels[catalog.MO_TABLES].tableID = catalogTableID
			buildPlan := func(context.Context) (*plan.Plan, error) { return plan2.BuildPlan(cc, stmt, false) }
			pn, err := buildPlan(ctx)
			require.NoError(t, err)
			c := NewCompile("127.0.0.1:1", "tpch", sql, "", "", eng, proc, stmt, false, nil, time.Now())
			t.Cleanup(c.Release)
			c.SetResourceAttemptOwnerEligible()
			c.SetBuildPlanFunc(buildPlan)
			require.NoError(t, c.Compile(ctx, pn, nil))
			c.InitPipelineContextToExecuteQuery()
			require.Same(t, root, resource.LockReadRootFromContext(proc.Ctx))

			next, err := tc.rebuild(c)
			require.NoError(t, err)
			t.Cleanup(next.Release)
			next.InitPipelineContextToRetryQuery()
			require.ErrorIs(t, next.prePipelineInitializer(), lockservice.ErrLockTimeout)
			_, snapshot := root.SealWithLockRead(uint64(100 * time.Millisecond))
			require.True(t, snapshot.Qualifies(100*time.Millisecond, time.Second))
			d := snapshot.Diagnostic([16]byte{}, 2, true)
			require.Positive(t, d.AcquisitionNS)
			require.Equal(t, "acquisition", d.LastIncomplete)
		})
	}
}
