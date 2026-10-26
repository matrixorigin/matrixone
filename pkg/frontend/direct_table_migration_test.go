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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestDirectTableMigrationGrammar(t *testing.T) {
	for _, sql := range []string{"select id from db.t", "SELECT id, tenant FROM db.t ORDER BY tenant DESC, id ASC"} {
		for _, mode := range []string{"", "ANSI_QUOTES,PIPES_AS_CONCAT,NO_BACKSLASH_ESCAPES,REAL_AS_FLOAT,HIGH_NOT_PRECEDENCE,IGNORE_SPACE"} {
			shape, ok := parseDirectTableSelectV1(t.Context(), sql, mode)
			require.True(t, ok, sql)
			require.Equal(t, "db", shape.database)
			require.Equal(t, "t", shape.table)
		}
	}
	for _, sql := range []string{
		"select id from t", "select id from db.t;", "select id from db.t -- x",
		"select id from db.t /* x */", "select id from db.t x", "select id as x from db.t",
		"select id from db.t where false", "select id from db.t limit 1",
		"select id from db.t order by tenant", "select id from db.t order by 1",
		"select id from db.t order by id nulls first", "select id,id from db.t",
		"select * from db.t", "select t.id from db.t", "select ID from db.t",
		"select `id` from db.t", "select id from `db`.t",
		"select ? from db.t", "select @id from db.t", "select crc32(id) from db.t",
		"select case when 1 then id else crc32(tenant) end from db.t",
		"select id from db.t join db.u on t.id=u.id", "select id from (select id from db.t) t",
		"select id into @v from db.t", "select distinct id from db.t", "select id from db.t for update",
		"select id from db.t union select id from db.t",
	} {
		_, ok := parseDirectTableSelectV1(t.Context(), sql, "")
		require.False(t, ok, sql)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, ok := parseDirectTableSelectV1(ctx, "select id from db.t", "")
	require.False(t, ok)
}

// The real binder qualifies the saved projection in-place. The reviewed
// saved-only normalization must preserve the raw descriptor without mutation.
func TestDirectTableMigrationSavedASTBinderCounterexample(t *testing.T) {
	const sql = "select id from db.t order by id"
	statements, err := mysql.ParseWithSQLMode(t.Context(), sql, 1, "")
	require.NoError(t, err)
	require.Len(t, statements, 1)
	defer statements[0].Free()
	saved := statements[0].(*tree.Select)
	raw, ok := directShapeFromAST(saved)
	require.True(t, ok)
	compiler := &directCompilerFixture{CompilerContext: plan2.NewEmptyCompilerContext(newPlanTestProcess(t)), def: directTestTable()}
	wrapper := tree.NewPrepareStmt("p", saved)
	defer wrapper.Free()
	_, err = plan2.BuildPlan(compiler, wrapper, false)
	require.NoError(t, err)
	name := saved.Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.UnresolvedName)
	require.Equal(t, 2, name.NumParts)
	require.Equal(t, "t", name.TblName())
	require.Equal(t, "id", name.ColName())
	normalized, ok := directSavedShapeFromAST(saved, "t")
	require.True(t, ok)
	require.Equal(t, raw, normalized)
	require.Equal(t, 2, name.NumParts, "classification must not mutate the saved AST")
	_, ok = parseDirectTableSelectV1(t.Context(), sql, "")
	require.True(t, ok)
	_, ok = parseDirectTableSelectV1(t.Context(), "select t.id from db.t order by id", "")
	require.False(t, ok, "the raw language must not expand")
}

func TestDirectTableMigrationSavedOnlyQualifierMatrix(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		safe bool
	}{
		{"select id from db.t order by id", true},
		{"select t.id from db.t order by id", true},
		{"select id from db.t order by t.id", true},
		{"select t.id from db.t order by t.id", true},
		{"select other.id from db.t order by id", false},
		{"select id from db.t order by other.id", false},
		{"select db.t.id from db.t order by id", false},
		{"select id from db.t order by db.t.id", false},
		{"select T.id from db.t order by id", false},
		{"select t.ID from db.t order by id", false},
		{"select t.id as id from db.t", false},
		{"select t.* from db.t", false},
		{"select crc32(t.id) from db.t", false},
		{"select case when 1 then id else crc32(t.id) end from db.t", false},
		{"select ? from db.t", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			statements, err := mysql.ParseWithSQLMode(t.Context(), tc.sql, 1, "")
			require.NoError(t, err)
			require.Len(t, statements, 1)
			defer statements[0].Free()
			_, ok := directSavedShapeFromAST(statements[0], "t")
			require.Equal(t, tc.safe, ok)
			if tc.safe {
				_, rawOK := parseDirectTableSelectV1(t.Context(), tc.sql, "")
				require.Equal(t, tc.sql == "select id from db.t order by id", rawOK)
			}
		})
	}
	for _, qualified := range []bool{false, true} {
		n := tree.NewUnresolvedColName("id")
		if qualified {
			n.NumParts = 2
			n.CStrParts[1] = tree.NewCStr("t", 1)
		}
		_, ok := directSavedName(n, "t")
		require.True(t, ok)
		n.CStrParts[n.NumParts] = tree.NewCStr("extra", 1)
		_, ok = directSavedName(n, "t")
		require.False(t, ok, "hidden extra component must reject")
	}
}

func directTestTable() *plan.TableDef {
	return &plan.TableDef{TblId: 11, DbId: 7, DbName: "db", Name: "t", Version: 3,
		TableType: catalog.SystemOrdinaryRel,
		Pkey:      &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
		Cols: []*plan.ColDef{
			{Name: "id", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32), Width: 32, Scale: -1}},
			{Name: "tenant", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int32), Width: 32, Scale: -1}},
			{Name: catalog.FakePrimaryKeyColName, Hidden: true, Primary: true, Seqnum: 2,
				Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
		}}
}

func directTestProof(t *testing.T) *query.PreparedDirectTableSelectV1 {
	t.Helper()
	cols, ok := directColumns(directTestTable())
	require.True(t, ok)
	return &query.PreparedDirectTableSelectV1{Version: 1, AccountID: 0, DatabaseID: 7, TableID: 11,
		TableVersion: 3, DatabaseName: "db", TableName: "t", Columns: cols, Projection: []string{"id"}}
}

func TestDirectTableMigrationCompleteSchema(t *testing.T) {
	p := directTestProof(t)
	require.True(t, directColumnsMatch(directTestTable(), p))
	zeroScale := directTestTable()
	zeroScale.Cols[0].Typ.Scale = 0
	require.True(t, directColumnsMatch(zeroScale, p), "canonical no-scale integer representation")
	for _, mutate := range []func(*plan.TableDef){
		func(d *plan.TableDef) { d.Cols[1].GeneratedCol = &plan.GeneratedCol{} },
		func(d *plan.TableDef) { d.Cols[1].Typ.Id = int32(types.T_json) },
		func(d *plan.TableDef) { d.Cols[1].Typ.AutoIncr = true },
		func(d *plan.TableDef) { d.Cols[1].Seqnum = d.Cols[0].Seqnum },
		func(d *plan.TableDef) { d.Cols[1].Name = d.Cols[0].Name },
		func(d *plan.TableDef) { d.Cols[1].Typ.Charset = 1 },
		func(d *plan.TableDef) { d.Cols[0].Typ.Scale = 1 },
		func(d *plan.TableDef) { d.Cols[0].Typ.Scale = -2 },
		func(d *plan.TableDef) { d.Cols[2].Typ.Scale = -1 },
		func(d *plan.TableDef) { d.Cols[2].GeneratedCol = &plan.GeneratedCol{} },
		func(d *plan.TableDef) { d.TableType = "v"; d.ViewSql = &plan.ViewDef{View: "select crc32(id)"} },
		func(d *plan.TableDef) { d.IsTemporary = true },
		func(d *plan.TableDef) { d.FeatureFlag = 1 },
		func(d *plan.TableDef) { d.Indexes = []*plan.IndexDef{{}} },
	} {
		d := directTestTable()
		mutate(d)
		require.False(t, directColumnsMatch(d, p))
	}
	p.Columns[0].Width++
	require.False(t, directColumnsMatch(directTestTable(), p))
}

type directCompilerFixture struct {
	plan2.CompilerContext
	def          *plan.TableDef
	resolveCount int
}

func (c *directCompilerFixture) GetAccountId() (uint32, error) { return 0, nil }
func (c *directCompilerFixture) GetSnapshot() *plan.Snapshot   { return nil }
func (c *directCompilerFixture) DatabaseExists(name string, _ *plan.Snapshot) bool {
	return name == "db"
}
func (c *directCompilerFixture) GetDatabaseId(name string, snapshot *plan.Snapshot) (uint64, error) {
	if !c.DatabaseExists(name, snapshot) {
		return 0, directMigrationRefusal()
	}
	return 7, nil
}
func (c *directCompilerFixture) Resolve(string, string, *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	c.resolveCount++
	return &plan.ObjectRef{SchemaName: "db", ObjName: "t", Obj: 11}, c.def, nil
}

// Deterministic barrier: preflight saw generation3, Resolve sees its
// replacement. Replacement metadata never reaches the binder/view expander.
func TestDirectTableMigrationResolveDDLBarrier(t *testing.T) {
	p := directTestProof(t)
	base := &directCompilerFixture{def: directTestTable()}
	guard := &directTableMigrationCompiler{CompilerContext: base, proof: p}
	ref, def, err := guard.Resolve("db", "t", nil)
	require.NoError(t, err)
	require.NotNil(t, ref)
	require.NotNil(t, def)
	require.True(t, guard.resolved)
	for _, mutate := range []func(*plan.TableDef){
		func(d *plan.TableDef) { d.Version++ },
		func(d *plan.TableDef) { d.TblId++ },
		func(d *plan.TableDef) { d.TableType = "v"; d.ViewSql = &plan.ViewDef{View: "select crc32(id)"} },
		func(d *plan.TableDef) { d.Cols[1].GeneratedCol = &plan.GeneratedCol{} },
	} {
		base.def = directTestTable()
		mutate(base.def)
		guard.resolved = false
		ref, def, err = guard.Resolve("db", "t", nil)
		require.Error(t, err)
		require.Nil(t, ref)
		require.Nil(t, def)
		require.False(t, guard.resolved)
	}
	calls := base.resolveCount
	_, _, err = guard.Resolve("foreign", "t", nil)
	require.Error(t, err)
	_, _, err = guard.ResolveById(11, nil)
	require.Error(t, err)
	_, _, err = guard.ResolveIndexTableByRef(nil, "i", nil)
	require.Error(t, err)
	require.Equal(t, calls, base.resolveCount)
}

func TestDirectTableMigrationMissingDispositionPreservesTargetAndOnce(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	ses.SetDatabaseName("keep")
	ses.SetLastInsertID(91)
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	req := &query.MigrateConnToRequest{DB: "db", LastInsertIDExported: true,
		PrepareStmts: []*query.PrepareStmt{{Name: "p", SQL: "select id from db.t",
			DirectTableSelect: directTestProof(t), RequiredProtocolVersion: defines.MORPCVersion134}}}
	err := rt.migrateConnectionTo(t.Context(), req)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))
	require.Equal(t, "keep", ses.GetDatabaseName())
	require.Equal(t, uint64(91), ses.GetLastInsertID())
	consumed := false
	rt.mc.migrateOnce.Do(func() { consumed = true })
	require.True(t, consumed)
	require.True(t, rt.mc.tryBeginOperation())
	rt.mc.endOperation()
}

func TestDirectTableMigrationRuntimePresenceAndValue(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	vars := []migratedSystemVariable{
		{name: "optimizer_hints"}, {name: "runtime_filter_limit_in"}, {name: "runtime_filter_limit_bloom_filter"},
	}
	runtime := moruntime.ServiceRuntime(ses.GetService())
	for i := range vars {
		v, present := runtime.GetGlobalVariables(vars[i].name)
		vars[i].runtimeValue, vars[i].runtimeValuePresent = v, present
	}
	require.True(t, directRuntimeMatches(ses, vars))
	vars[0].runtimeValuePresent = !vars[0].runtimeValuePresent
	require.False(t, directRuntimeMatches(ses, vars))
	vars[0].runtimeValuePresent = !vars[0].runtimeValuePresent
	vars[0].runtimeValue = struct{}{}
	if vars[0].runtimeValuePresent {
		require.False(t, directRuntimeMatches(ses, vars))
	}
	require.False(t, directRuntimeMatches(ses, vars[:2]))
}

func TestDirectTableMigrationPublicationGenerationBarrier(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*viewSchemaOwnedFixture)
	}{
		{"workspace revision", func(f *viewSchemaOwnedFixture) { f.workspace.UpdateSnapshotWriteOffset() }},
		{"snapshot revision", func(f *viewSchemaOwnedFixture) { f.op.SetSnapshotTS(f.op.SnapshotTS().Next()) }},
		{"local DDL", func(f *viewSchemaOwnedFixture) { f.session.advanceDDLVersion() }},
		{"role identity", func(f *viewSchemaOwnedFixture) { f.session.GetTenantInfo().SetDefaultRoleID(10) }},
		{"role grants", func(f *viewSchemaOwnedFixture) { f.session.cache.invalidate() }},
		{"compiler owner", func(f *viewSchemaOwnedFixture) {
			f.parent.SetExecCtx(&ExecCtx{reqCtx: f.ctx, ses: f.session, proc: f.proc})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Existing fixture owns a real txn operator/workspace, not catalog
			// SQL. This proves publication stamps, not distributed DDL itself.
			f := newViewSchemaOwnedFixture(t)
			check, err := directPublicationGuard(f.ctx, f.session, f.parent.execCtx, f.parent)
			require.NoError(t, err)
			require.NoError(t, check())
			tc.change(f)
			require.Error(t, check())
		})
	}
}

type directDisposalProtocol struct {
	*testMysqlWriter
	controller *migrateController
	closed     bool
}

func (p *directDisposalProtocol) Close() {
	// Closed() can synchronously wait for lifecycle admission. The operation
	// must already be released, so this does not wait on its own caller.
	p.controller.waitAndClose()
	p.closed = true
}

func TestDirectTableMigrationReplayFailureClosesAfterOperation(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	p := &directDisposalProtocol{testMysqlWriter: &testMysqlWriter{}}
	rt := NewRoutine(t.Context(), p, &config.FrontendParameters{})
	p.controller = rt.mc
	rt.setSession(ses)
	t.Cleanup(rt.cancelRoutineFunc)
	// Invalid typed state fails after entering replay (including counter
	// restoration), without needing a service or fake catalog transaction.
	req := &query.MigrateConnToRequest{LastInsertIDExported: true, LastInsertID: 91,
		CloseTargetOnReplayFailure: true, UserDefinedVarsExported: true,
		UserDefinedVars: []*query.MigrateUserDefinedVar{nil}}
	require.Error(t, rt.migrateConnectionTo(t.Context(), req))
	require.True(t, p.closed)
	require.True(t, rt.shouldCloseConnection())
	require.False(t, rt.mc.tryBeginOperation())
	// Even a syntactically corrected request cannot reuse a disposed target.
	req.UserDefinedVars = nil
	require.Error(t, rt.migrateConnectionTo(t.Context(), req))
}

func TestDirectTableMigrationReplayPanicOwnership(t *testing.T) {
	for _, disposable := range []bool{false, true} {
		t.Run(fmt.Sprintf("disposable=%t", disposable), func(t *testing.T) {
			ses := newTestSession(t, gomock.NewController(t))
			t.Cleanup(ses.Close)
			tenant := ses.GetTenantInfo()
			// A missing session identity deliberately faults inside replay, after
			// lifecycle admission. Restore it before the fixture's ordinary close.
			ses.SetTenantInfo(nil)
			t.Cleanup(func() { ses.SetTenantInfo(tenant) })
			p := &directDisposalProtocol{testMysqlWriter: &testMysqlWriter{}}
			rt := NewRoutine(t.Context(), p, &config.FrontendParameters{})
			p.controller = rt.mc
			rt.setSession(ses)
			t.Cleanup(rt.cancelRoutineFunc)
			req := &query.MigrateConnToRequest{LastInsertIDExported: true,
				CloseTargetOnReplayFailure: disposable}
			if disposable {
				require.Error(t, rt.migrateConnectionTo(t.Context(), req))
				require.True(t, p.closed)
				require.True(t, rt.shouldCloseConnection())
				require.False(t, rt.mc.tryBeginOperation())
			} else {
				require.Panics(t, func() { _ = rt.migrateConnectionTo(t.Context(), req) })
				require.False(t, p.closed)
				// The outer panic owner still receives the panic, and this call
				// has released admission rather than leaving a self-wait behind.
				require.True(t, rt.mc.tryBeginOperation())
				rt.mc.endOperation()
			}
		})
	}
}
