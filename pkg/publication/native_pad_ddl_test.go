// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package publication

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Keep nullable GETDDL scanning local: existing mockResultForTest deliberately
// rejects these targets in scan-error controls.
type nativePADDDLResult struct{ testMockResult }

func (r *nativePADDDLResult) Scan(dest ...interface{}) error {
	if r.currentRow < 0 || r.currentRow >= len(r.data) || len(dest) != len(r.data[r.currentRow]) {
		return errors.New("invalid GETDDL result position or column count")
	}
	for i, target := range dest {
		scanner, ok := target.(sql.Scanner)
		if !ok {
			return errors.New("GETDDL expects nullable SQL values")
		}
		if err := scanner.Scan(r.data[r.currentRow][i]); err != nil {
			return err
		}
	}
	return nil
}

func nativePADPublicationProcess(t *testing.T) *process.Process {
	t.Helper()
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		old, exists := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if exists {
				rt.SetGlobalVariables(key, old)
			} else if value, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, value)
			}
		})
		rt.SetGlobalVariables(key, defines.MORPCVersion130)
	}
	return proc
}

func TestNativePADPublicationLegacyDDLBinding(t *testing.T) {
	proc := nativePADPublicationProcess(t)
	parent := plan.WithCollationBindingRevision(defines.AttachAccountId(t.Context(), 42), 1)
	var target *pbplan.TableDef
	stop := errors.New("stop after binding, before catalog effects")
	exec := &mockSQLExecutor{execSQLFunc: func(ctx context.Context, _ *ActiveRoutine, account uint32, sql string, _, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
		if !strings.HasPrefix(strings.ToLower(sql), "create table") {
			return nil, nil, nil
		}
		require.Equal(t, uint32(42), account)
		gotAccount, err := defines.GetAccountId(ctx)
		require.NoError(t, err)
		require.Equal(t, uint32(42), gotAccount)
		compiler := plan.NewMockCompilerContext(false, proc)
		compiler.SetContext(ctx)
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, nil, err
		}
		defer stmt.Free()
		p, err := plan.BuildPlan(compiler, stmt, false)
		if err != nil {
			return nil, nil, err
		}
		target = p.GetDdl().GetCreateTable().TableDef
		return nil, nil, stop
	}}
	err := createTable(parent, exec, "db", "n", "create table db.n (s varchar(8) collate utf8mb4_bin)",
		&IterationContext{SrcInfo: SrcInfo{AccountID: 42}}, &TableDDLInfo{}, nil)
	require.ErrorContains(t, err, stop.Error())
	require.NotNil(t, target)
	require.Zero(t, target.Cols[0].Typ.CollationVersion, "unversioned publication DDL is a legacy transport, not new authoring")
}

func TestNativePADPublicationInplaceBindingContext(t *testing.T) {
	proc := nativePADPublicationProcess(t)
	parent := plan.WithCollationBindingRevision(defines.AttachAccountId(t.Context(), 42), 1)
	bindingRevision := func(ctx context.Context) uint32 {
		compiler := plan.NewMockCompilerContext(false, proc)
		compiler.SetContext(ctx)
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "create table probe(s varchar(8) collate utf8mb4_general_ci)", 1)
		require.NoError(t, err)
		defer stmt.Free()
		p, err := plan.BuildPlan(compiler, stmt, false)
		require.NoError(t, err)
		return p.GetDdl().GetCreateTable().TableDef.Cols[0].Typ.CollationVersion
	}
	require.Equal(t, uint32(1), bindingRevision(parent))
	ctrl := gomock.NewController(t)
	eng, db, rel := mock_frontend.NewMockEngine(ctrl), mock_frontend.NewMockDatabase(ctrl), mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	source := &pbplan.TableDef{Name: "n", DbName: "db", Cols: []*pbplan.ColDef{{Name: "id", Typ: pbplan.Type{Id: int32(types.T_int32)}, Default: &pbplan.Default{NullAbility: true}}}}
	currentSQL, _, err := plan.ConstructCreateTableSQL(nil, source, nil, true, nil)
	require.NoError(t, err)
	eng.EXPECT().Database(gomock.Any(), "db", txn).Return(db, nil).Times(3)
	db.EXPECT().Relation(gomock.Any(), "n", nil).Return(rel, nil).Times(2)
	rel.EXPECT().CopyTableDef(gomock.Any()).Return(proto.Clone(source).(*pbplan.TableDef))
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1))
	rel.EXPECT().GetTableDef(gomock.Any()).Return(source)
	upstreamResult := &nativePADDDLResult{testMockResult{
		data:       [][]interface{}{{"db", "n", int64(1), currentSQL + " COMMENT='changed'"}},
		currentRow: -1,
	}}
	upstreamCanceled, localCanceled, localCalls := 0, 0, 0
	upstream := &mockSQLExecutor{execSQLFunc: func(context.Context, *ActiveRoutine, uint32, string, bool, bool, time.Duration) (*Result, context.CancelFunc, error) {
		return &Result{mockResult: upstreamResult}, func() { upstreamCanceled++ }, nil
	}}
	stop := errors.New("stop at inplace SQL receiver")
	localResult := &testMockResult{}
	local := &mockSQLExecutor{execSQLFunc: func(ctx context.Context, _ *ActiveRoutine, account uint32, sql string, _, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
		localCalls++
		require.Equal(t, "ALTER TABLE `db`.`n` COMMENT 'changed'", sql)
		require.Equal(t, uint32(42), account)
		gotAccount, err := defines.GetAccountId(ctx)
		require.NoError(t, err)
		require.Equal(t, uint32(42), gotAccount)
		// Current generated inplace SQL has no COLLATE expression. A real
		// binder probe checks the receiver context, not a fabricated ALTER.
		require.Zero(t, bindingRevision(ctx))
		return &Result{mockResult: localResult}, func() { localCanceled++ }, stop
	}}
	err = ProcessDDLChanges(parent, eng, &IterationContext{
		SrcInfo:  SrcInfo{AccountID: 42, SyncLevel: SyncLevelTable, DBName: "db", TableName: "n"},
		LocalTxn: txn, UpstreamExecutor: upstream, LocalExecutor: local, CurrentSnapshotName: "snapshot",
		TableIDs: map[TableKey]uint64{{DBName: "db", TableName: "n"}: 1},
	})
	require.ErrorContains(t, err, stop.Error())
	require.Equal(t, 1, localCalls)
	require.Equal(t, 1, upstreamCanceled)
	require.True(t, upstreamResult.closed)
	require.Equal(t, 1, localCanceled)
	require.True(t, localResult.closed)
	require.Equal(t, uint32(1), bindingRevision(parent))
}

func TestNativePADPublicationSchemaExport(t *testing.T) {
	for _, source := range []*pbplan.TableDef{
		{Name: "native", Cols: []*pbplan.ColDef{{Name: "s", Typ: pbplan.Type{Id: int32(types.T_varchar), Width: 8, Charset: 3, CollationVersion: 1}}}},
		{Name: "view", ViewSql: &pbplan.ViewDef{View: `{"Stmt":"create view v as select true as b","collation_binding_revision":1,"required_protocol_version":130}`}},
		{Name: "legacy", DefaultCharset: 2, Cols: []*pbplan.ColDef{{Name: "s", Typ: pbplan.Type{Id: int32(types.T_varchar), Width: 8, Charset: 2}, Default: &pbplan.Default{NullAbility: true}}}},
	} {
		t.Run(source.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			rel := mock_frontend.NewMockRelation(ctrl)
			ctx := defines.AttachAccountId(t.Context(), 42)
			before := proto.Clone(source)
			rel.EXPECT().CopyTableDef(ctx).Return(proto.Clone(source).(*pbplan.TableDef))
			sql, err := getCurrentTableCreateSQL(ctx, rel, "db", source.Name)
			if source.Name == "legacy" {
				require.NoError(t, err)
				require.Contains(t, sql, "COLLATE=utf32_bin")
			} else {
				require.Error(t, err)
				require.Empty(t, sql)
			}
			require.Equal(t, before, source)
		})
	}
}

func TestNativePADPublicationDeleteRejectsBeforeEffects(t *testing.T) {
	for _, source := range []*pbplan.TableDef{
		{Name: "n", Cols: []*pbplan.ColDef{{Name: "s", Typ: pbplan.Type{Id: int32(types.T_varchar), Width: 8, Charset: 3, CollationVersion: 1}}}},
		{Name: "v", ViewSql: &pbplan.ViewDef{View: `{"Stmt":"create view v as select true as b","collation_binding_revision":1,"required_protocol_version":130}`}},
	} {
		t.Run(source.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			eng := mock_frontend.NewMockEngine(ctrl)
			db := mock_frontend.NewMockDatabase(ctrl)
			rel := mock_frontend.NewMockRelation(ctrl)
			txn := mock_frontend.NewMockTxnOperator(ctrl)
			ctx := defines.AttachAccountId(t.Context(), 42)
			eng.EXPECT().Database(ctx, "db", txn).Return(db, nil)
			db.EXPECT().Relation(ctx, source.Name, nil).Return(rel, nil)
			rel.EXPECT().GetTableDef(ctx).Return(source).AnyTimes()
			rel.EXPECT().GetTableID(ctx).Return(uint64(1001)).AnyTimes()
			deletions := 0
			db.EXPECT().Delete(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, string) error {
				deletions++
				return errors.New("unexpected publication deletion")
			}).AnyTimes()
			before := proto.Clone(source)
			err := dropTable(ctx, &mockSQLExecutor{}, "db", source.Name,
				&IterationContext{LocalTxn: txn}, 1001, eng)
			require.Error(t, err)
			require.Zero(t, deletions, "the direct engine owner must preserve the legacy-only publication boundary")
			require.Equal(t, before, source)
		})
	}
}
