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

package frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestViewSchemaDependencyIdentityUsesEffectiveCatalogAccount(t *testing.T) {
	ses := &Session{feSessionImpl: feSessionImpl{accountId: 7, tenant: &TenantInfo{TenantID: 7}}}
	ctx := defines.AttachAccountId(t.Context(), 23)
	compiler := &TxnCompilerContext{viewSchemaRead: true, execCtx: &ExecCtx{reqCtx: ctx, ses: ses}}
	for _, tc := range []struct {
		name     string
		object   *pb.ObjectRef
		def      *pb.TableDef
		snapshot *plan.Snapshot
		want     uint32
	}{
		{name: "实际读取域", object: &pb.ObjectRef{SchemaName: "db", ObjName: "t"}, want: 23},
		{name: "名称来自定义", def: &pb.TableDef{DbName: "db", Name: "t"}, want: 23},
		{name: "历史租户覆盖实际域", object: &pb.ObjectRef{SchemaName: "db", ObjName: "t"},
			snapshot: &plan.Snapshot{Tenant: &pb.SnapshotTenant{TenantID: 8}}, want: 8},
		{name: "cluster表覆盖历史域", object: &pb.ObjectRef{SchemaName: catalog.MO_CATALOG, ObjName: "cluster_table"},
			snapshot: &plan.Snapshot{Tenant: &pb.SnapshotTenant{TenantID: 8}}, want: 0},
		{name: "publication覆盖历史域", object: &pb.ObjectRef{SchemaName: "db", ObjName: "t", PubInfo: &pb.PubInfo{TenantId: 9}},
			snapshot: &plan.Snapshot{Tenant: &pb.SnapshotTenant{TenantID: 8}}, want: 9},
		{name: "publication覆盖通用cluster名称", object: &pb.ObjectRef{SchemaName: catalog.MO_CATALOG,
			ObjName: "cluster_table", PubInfo: &pb.PubInfo{TenantId: 9}}, want: 9},
		{name: "强制系统表覆盖publication", object: &pb.ObjectRef{SchemaName: catalog.MO_SYSTEM,
			ObjName: catalog.MO_STATEMENT, PubInfo: &pb.PubInfo{TenantId: 9}}, want: 0},
		{name: "普通CDC表仍使用实际域", object: &pb.ObjectRef{SchemaName: catalog.MO_CATALOG,
			ObjName: catalog.MO_CDC_SNAPSHOT}, want: 23},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account, err := compiler.ResolveViewDependencyAccount(tc.object, tc.def, tc.snapshot)
			require.NoError(t, err)
			require.Equal(t, tc.want, account)
			require.Equal(t, uint32(7), ses.GetAccountId())
			require.Same(t, ctx, compiler.GetContext())
		})
	}
	// opt-in 的实际读取域不能改变普通统计/编译路径既有的 session 起点。
	compiler.viewSchemaRead = false
	account, err := compiler.ResolveViewDependencyAccount(&pb.ObjectRef{SchemaName: "db", ObjName: "t"}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, uint32(7), account)
	compiler.viewSchemaRead = true
	compiler.execCtx.reqCtx = t.Context()
	account, err = compiler.ResolveViewDependencyAccount(&pb.ObjectRef{SchemaName: "db", ObjName: "t"}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, uint32(7), account, "未附加读取account时保留session起点")
}

func TestViewSchemaSubscriptionDatabaseLookupRestoresContext(t *testing.T) {
	for _, tc := range []struct {
		name         string
		database     string
		wantDatabase string
		historical   bool
		failure      bool
		cancel       bool
		plain        bool
	}{
		{name: "current alias", database: "sub", wantDatabase: "pub"},
		{name: "current qualified publisher", database: "pub", wantDatabase: "pub"},
		{name: "historical alias", database: "sub", wantDatabase: "pub", historical: true},
		{name: "historical qualified publisher", database: "pub", wantDatabase: "pub", historical: true},
		{name: "catalog error", database: "sub", wantDatabase: "pub", historical: true, failure: true},
		{name: "cancel during catalog read", database: "sub", wantDatabase: "pub", historical: true, cancel: true},
		{name: "ordinary lookup", database: "ordinary", wantDatabase: "ordinary", plain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaOwnedFixture(t)
			f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 100})
			if !tc.plain {
				f.parent.SetQueryingSubscription(&pb.SubscriptionMeta{AccountId: 23, SubName: "sub", DbName: "pub"})
			}
			ctrl := gomock.NewController(t)
			storage := mock_frontend.NewMockEngine(ctrl)
			f.session.txnHandler.storage = storage
			lookupCtx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			provider := &viewSchemaProvider{parent: f.parent,
				authorize: func(context.Context, string, string, *plan.Snapshot) error { return nil }}
			binding, err := provider.OpenViewSchemaBinding(lookupCtx)
			require.NoError(t, err)
			t.Cleanup(binding.Close)
			child := binding.Compiler.(*viewSchemaCompilerContext)
			beforeCtx, beforeProcCtx := child.GetContext(), child.GetProcess().Ctx
			parentCtx, parentProcCtx := f.parent.GetContext(), f.proc.Ctx
			var snapshot, beforeSnapshot *plan.Snapshot
			if tc.historical {
				snapshot = &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 50}, Tenant: &pb.SnapshotTenant{TenantID: 7}}
				beforeSnapshot = plan.DeepCopySnapshot(snapshot)
			}
			wantAccount := uint32(23)
			if tc.plain {
				wantAccount = 7
			}
			lookupErr := errors.New("native catalog lookup failed")
			database := mock_frontend.NewMockDatabase(ctrl)
			if !tc.failure && !tc.cancel {
				database.EXPECT().GetDatabaseId(gomock.Any()).Return("42")
			}
			storage.EXPECT().Database(gomock.Any(), tc.wantDatabase, gomock.Any()).DoAndReturn(
				func(got context.Context, _ string, op client.TxnOperator) (engine.Database, error) {
					account, err := defines.GetAccountId(got)
					require.NoError(t, err)
					require.Equal(t, wantAccount, account)
					// 回调观察真实 GetDatabaseId 调用时的 child，而不是独立模拟 account 计算。
					dependencyAccount, err := child.ResolveViewDependencyAccount(&pb.ObjectRef{SchemaName: "db", ObjName: "source"}, nil, nil)
					require.NoError(t, err)
					require.Equal(t, wantAccount, dependencyAccount)
					if tc.historical {
						require.NotSame(t, f.op, op)
						require.True(t, op.IsSnapOp())
						require.Equal(t, *snapshot.TS, op.SnapshotTS())
					} else {
						require.Same(t, f.op, op)
					}
					if tc.cancel {
						cancel()
						return nil, context.Cause(got)
					}
					if tc.failure {
						return nil, lookupErr
					}
					return database, nil
				})
			id, err := child.GetDatabaseId(tc.database, snapshot)
			switch {
			case tc.cancel:
				require.ErrorIs(t, err, context.Canceled)
			case tc.failure:
				require.ErrorIs(t, err, lookupErr)
			default:
				require.NoError(t, err)
				require.Equal(t, uint64(42), id)
			}
			require.Equal(t, beforeSnapshot, snapshot, "调用者的历史tenant/TS不能被改写")
			require.Same(t, beforeCtx, child.GetContext())
			require.Same(t, beforeProcCtx, child.GetProcess().Ctx)
			require.Same(t, parentCtx, f.parent.GetContext())
			require.Same(t, parentProcCtx, f.proc.Ctx)
			require.Equal(t, uint32(7), f.session.GetAccountId())
			require.Equal(t, uint32(7), f.session.GetTenantInfo().GetTenantID())
			require.Same(t, f.op, f.session.GetTxnHandler().GetTxn())
			if tc.cancel {
				require.ErrorIs(t, binding.Check(), context.Canceled)
			} else {
				require.NoError(t, binding.Check(), "成功和错误返回均应恢复可继续读取的subscriber上下文")
			}
		})
	}
}

func TestViewSchemaReadFloorChangesInvalidateBinding(t *testing.T) {
	for _, tc := range []struct {
		name    string
		before  any
		after   any
		changed bool
	}{
		{name: "floor appears", after: int64(98), changed: true},
		{name: "floor disappears", before: int64(98), changed: true},
		{name: "floor advances", before: int64(97), after: int64(98), changed: true},
		{name: "floor regresses", before: int64(98), after: int64(97), changed: true},
		{name: "floor becomes invalid", before: int64(98), after: "invalid", changed: true},
		{name: "same floor", before: int64(98), after: int64(98)},
		{name: "both absent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaOwnedFixture(t)
			rt := moruntime.ServiceRuntime(f.proc.GetService())
			setFloor := func(value any) {
				if value != nil {
					rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, value)
				} else if current, present := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); present {
					rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
				}
			}
			previous, present := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
			t.Cleanup(func() {
				if present {
					setFloor(previous)
				} else {
					setFloor(nil)
				}
			})
			setFloor(tc.before)
			binding := f.open(t)
			require.NoError(t, binding.Check())
			setFloor(tc.after)
			if tc.changed {
				require.ErrorIs(t, binding.Check(), plan.ErrViewSchemaChanged)
			} else {
				require.NoError(t, binding.Check())
			}
			binding.Close()
			require.False(t, binding.Generation.Closed())
			require.Zero(t, binding.Generation.Used())
		})
	}
}
