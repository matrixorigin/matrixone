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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

func TestBranchComponentRejectsUnsafeOwnerBeforeCatalogAccess(t *testing.T) {
	for _, meta := range []txn.TxnMeta{{Mode: txn.TxnMode_Pessimistic, Isolation: txn.TxnIsolation_SI}, {Mode: txn.TxnMode_Optimistic, Isolation: txn.TxnIsolation_RC}} {
		t.Run(meta.Mode.String()+meta.Isolation.String(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			defer ses.Close()
			owner := mock_frontend.NewMockTxnOperator(ctrl)
			owner.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
			owner.EXPECT().TxnOptions().Return(txn.TxnOptions{ByBegin: true}).AnyTimes()
			owner.EXPECT().Txn().Return(meta).AnyTimes()
			ses.proc.Base.TxnOperator = owner
			ses.GetTxnHandler().SetShareTxn(owner)
			// No Engine, lock service, rollback, or commit expectation: this is the
			// real share-txn background constructor and must fail before touching them.
			bh, admission, finish, err := getDataBranchComponentExecutor(defines.AttachAccountId(t.Context(), 0), ses)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			require.Nil(t, bh)
			require.Nil(t, admission)
			require.Nil(t, finish)
			require.Same(t, owner, ses.GetTxnHandler().GetTxn())
		})
	}
}

func TestBranchComponentFinishNeverCommitsPanic(t *testing.T) {
	want := errors.New("statement failure")
	for _, fail := range []string{"error", "panic", "success"} {
		t.Run(fail, func(t *testing.T) {
			calls := 0
			run := func() (err error) {
				defer finishDataBranchComponent(context.Background(), func(e error) error {
					calls++
					if fail == "success" {
						require.NoError(t, e)
					} else {
						require.Error(t, e)
					}
					return e
				}, &err)
				if fail == "panic" {
					panic(want)
				}
				if fail == "error" {
					return want
				}
				return nil
			}
			if fail == "panic" {
				require.PanicsWithValue(t, want, func() { _ = run() })
			} else if fail == "error" {
				require.ErrorIs(t, run(), want)
			} else {
				require.NoError(t, run())
			}
			require.Equal(t, 1, calls)
		})
	}
}

func TestBranchDeleteReceiptCannotEscapeOwnerOrScope(t *testing.T) {
	ctrl := gomock.NewController(t)
	owner := mock_frontend.NewMockTxnOperator(ctrl)
	other := mock_frontend.NewMockTxnOperator(ctrl)
	input := databranchutils.BranchDeleteTarget{AccountID: 7, Database: "db", DatabaseID: 12, TableIDs: []uint64{4, 2}, MembershipSQL: "select rel_id from mo_catalog.mo_tables"}
	ctx, release := databranchutils.WithBranchDeleteTarget(t.Context(), owner, input)
	input.TableIDs[0] = 99
	got, err := databranchutils.BranchDeleteTargetFromContext(ctx, owner)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 4}, got.TableIDs)
	got.TableIDs[0] = 88
	again, err := databranchutils.BranchDeleteTargetFromContext(ctx, owner)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 4}, again.TableIDs)
	_, err = databranchutils.BranchDeleteTargetFromContext(ctx, other)
	require.Error(t, err)
	release()
	_, err = databranchutils.BranchDeleteTargetFromContext(context.WithoutCancel(ctx), owner)
	require.Error(t, err)
}

func TestBranchCloneInventoryIgnoresScanOrderButDetectsDefinitionChange(t *testing.T) {
	a := cloneDatabaseSource{srcResolveDBName: "db", opAccountId: 7, toAccountId: 8, snapshot: &plan.Snapshot{},
		srcTblInfos: []*tableInfo{{dbName: "db", tblName: "a", createSql: "create table a(id int)"}, {dbName: "db", tblName: "b"}}, sortedFkTbls: []string{"db.a", "db.b"},
		fkTableMap:       map[string]*tableInfo{"db.a": {dbName: "db", tblName: "a"}},
		userDefinedFuncs: []userDefinedFunctionDefinition{{name: "f", argTypes: "int"}, {name: "f", argTypes: "text"}, {name: "g"}}}
	b := a
	b.snapshot = &plan.Snapshot{}
	b.srcTblInfos = []*tableInfo{a.srcTblInfos[1], a.srcTblInfos[0]}
	b.sortedFkTbls = []string{"db.b", "db.a"}
	b.userDefinedFuncs = []userDefinedFunctionDefinition{{name: "g"}, {name: "f", argTypes: "text"}, {name: "f", argTypes: "int"}}
	require.True(t, sameBranchCloneDatabaseSource(a, b))
	require.Equal(t, "a", a.srcTblInfos[0].tblName, "comparison must not sort caller slices")
	changed := *a.srcTblInfos[0]
	changed.createSql = "create table a(id bigint)"
	b.srcTblInfos = []*tableInfo{&changed, a.srcTblInfos[1]}
	require.False(t, sameBranchCloneDatabaseSource(a, b))
}
