// Copyright 2026 Matrix Origin
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
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/stretchr/testify/require"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestCloneDatabaseSourceBranchTableCount(t *testing.T) {
	tests := []struct {
		name   string
		tables []*tableInfo
		want   int64
	}{
		{
			name: "empty database consumes no branch table quota",
			want: 0,
		},
		{
			name: "mixed objects count only receipt-backed tables",
			tables: []*tableInfo{
				{tblName: "regular"},
				{tblName: "sequence", relKind: catalog.SystemSequenceRel},
				{tblName: "view", typ: view},
			},
			want: 1,
		},
		{
			name: "sequence-only database consumes no branch table quota",
			tables: []*tableInfo{
				{tblName: "sequence", relKind: catalog.SystemSequenceRel},
			},
			want: 0,
		},
		{
			name: "view-only database consumes no branch table quota",
			tables: []*tableInfo{
				{tblName: "view", typ: view},
			},
			want: 0,
		},
		{
			name: "ordinary tables each consume branch table quota",
			tables: []*tableInfo{
				{tblName: "regular"},
				{tblName: "foreign_key"},
			},
			want: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := cloneDatabaseSource{srcTblInfos: test.tables}
			require.Equal(t, test.want, source.branchTableCount())
		})
	}
}

func TestValidateCloneDatabaseAccounts(t *testing.T) {
	tests := []struct {
		name     string
		accounts cloneDatabaseAccountResolution
		wantErr  string
	}{
		{
			name: "same tenant",
			accounts: cloneDatabaseAccountResolution{
				opAccountId: 1,
				toAccountId: 1,
			},
		},
		{
			name: "cross tenant without snapshot",
			accounts: cloneDatabaseAccountResolution{
				opAccountId: sysAccountID,
				toAccountId: 1,
			},
			wantErr: "clone database between different accounts need a snapshot",
		},
		{
			name: "non sys cross tenant with snapshot",
			accounts: cloneDatabaseAccountResolution{
				opAccountId: 1,
				toAccountId: 2,
				snapshot:    &plan.Snapshot{},
			},
			wantErr: "only sys can clone table to another account",
		},
		{
			name: "sys cross tenant with snapshot",
			accounts: cloneDatabaseAccountResolution{
				opAccountId: sysAccountID,
				toAccountId: 1,
				snapshot:    &plan.Snapshot{},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateCloneDatabaseAccounts(context.Background(), test.accounts)
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestValidateCloneDatabaseSourceAccess(t *testing.T) {
	for _, database := range catalog.SystemDatabases {
		t.Run("non sys cannot clone "+database, func(t *testing.T) {
			err := validateCloneDatabaseSourceAccess(1, database)
			require.EqualError(t, err, "internal error: non-sys account cannot clone data from system database")
		})
	}
	t.Run("system database matching is case insensitive", func(t *testing.T) {
		err := validateCloneDatabaseSourceAccess(1, strings.ToUpper(catalog.MO_CATALOG))
		require.EqualError(t, err, "internal error: non-sys account cannot clone data from system database")
	})
	t.Run("sys can clone system catalog", func(t *testing.T) {
		require.NoError(t, validateCloneDatabaseSourceAccess(sysAccountID, catalog.MO_CATALOG))
	})
	t.Run("non sys can clone user database", func(t *testing.T) {
		require.NoError(t, validateCloneDatabaseSourceAccess(1, "user_database"))
	})
}

func TestLockDataBranchCloneDatabaseSourcesSkipsSourcesWithoutTables(t *testing.T) {
	ctx := context.WithValue(context.Background(), dataBranchCloneLockCtxKey{}, true)
	for _, source := range []cloneDatabaseSource{
		{},
		{srcTblInfos: []*tableInfo{{tblName: "view", typ: view}}},
	} {
		require.NoError(t, lockDataBranchCloneDatabaseSources(ctx, nil, nil, source))
	}
}

func TestCloneFkTableOrder(t *testing.T) {
	t.Run("acyclic dependencies retain topological order", func(t *testing.T) {
		parent := genKey("db", "parent")
		child := genKey("db", "child")
		order, hasCycle := cloneFkTableOrder(map[string][]string{
			child: {parent},
		})

		require.False(t, hasCycle)
		require.Equal(t, []string{parent, child}, order)
	})

	t.Run("cyclic dependencies use deterministic forward-reference order", func(t *testing.T) {
		a := genKey("db", "a")
		b := genKey("db", "b")
		order, hasCycle := cloneFkTableOrder(map[string][]string{
			a: {b},
			b: {a},
		})

		require.True(t, hasCycle)
		require.Equal(t, []string{a, b}, order)
	})
}

func TestCloneSnapshotTxnOperator(t *testing.T) {
	ctrl := gomock.NewController(t)
	outerTxn := mock_frontend.NewMockTxnOperator(ctrl)
	branchTxn := mock_frontend.NewMockTxnOperator(ctrl)
	ses := newFeatureLimitTestSession(t)
	ses.proc.Base.TxnOperator = outerTxn

	t.Run("normal clone keeps frontend transaction", func(t *testing.T) {
		bh := ses.InitBackExec(branchTxn, "", fakeDataSetFetcher2)
		require.Same(t, outerTxn, cloneSnapshotTxnOperator(ses, bh))
	})

	t.Run("data branch uses owning background transaction", func(t *testing.T) {
		bh := ses.InitBackExec(branchTxn, "", fakeDataSetFetcher2, &BackgroundExecOption{
			forcePessimisticRC: true,
		})
		require.Same(t, branchTxn, cloneSnapshotTxnOperator(ses, bh))
	})
}

func TestDataBranchCloneLockProcessUsesOwningBackgroundTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	outerTxn := mock_frontend.NewMockTxnOperator(ctrl)
	branchTxn := mock_frontend.NewMockTxnOperator(ctrl)
	ses := newFeatureLimitTestSession(t)
	ses.proc.Base.TxnOperator = outerTxn
	bh := ses.InitBackExec(branchTxn, "", fakeDataSetFetcher2, &BackgroundExecOption{
		forcePessimisticRC: true,
	})

	lockProc := newDataBranchCloneLockProcess(context.Background(), ses, bh)
	defer lockProc.Free()
	require.Same(t, branchTxn, lockProc.GetTxnOperator())
	require.Same(t, outerTxn, ses.proc.GetTxnOperator())
}

func TestCloneDatabaseTargetLockProcessUsesOwningBackgroundTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	outerTxn := mock_frontend.NewMockTxnOperator(ctrl)
	cloneTxn := mock_frontend.NewMockTxnOperator(ctrl)
	ses := newFeatureLimitTestSession(t)
	ses.proc.Base.TxnOperator = outerTxn
	bh := ses.InitBackExec(cloneTxn, "", fakeDataSetFetcher2, &BackgroundExecOption{
		forcePessimisticRC: true,
	})

	lockProc, err := newCloneDatabaseTargetLockProcess(context.Background(), ses, bh)
	require.NoError(t, err)
	defer lockProc.Free()
	require.Same(t, cloneTxn, lockProc.GetTxnOperator())
	require.Same(t, outerTxn, ses.proc.GetTxnOperator())
}
