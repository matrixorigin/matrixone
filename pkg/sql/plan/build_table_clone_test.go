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

package plan

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type cloneSnapshotCompilerContext struct {
	*MockCompilerContext
	snapshotErr error
}

type cloneSourceScopeContext struct {
	*MockCompilerContext
	current, named, lookupScope *Snapshot
	lookupErr                   error
}

func (c *cloneSourceScopeContext) GetSnapshot() *Snapshot         { return c.current }
func (c *cloneSourceScopeContext) SetSnapshot(snapshot *Snapshot) { c.current = snapshot }
func (c *cloneSourceScopeContext) ResolveSnapshotWithSnapshotName(string) (*Snapshot, error) {
	c.lookupScope = c.current
	return c.named, c.lookupErr
}

func TestCloneSourceSnapshotScope(t *testing.T) {
	parse := func(sql string) *tree.CloneTable {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		return stmt.(*tree.CloneTable)
	}
	ctx := &cloneSourceScopeContext{MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t))}
	prior := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 90}, Tenant: &planpb.SnapshotTenant{TenantID: 15}}
	ctx.current = prior
	ctx.named = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}, Tenant: &planpb.SnapshotTenant{TenantID: 7}}
	stmt := parse("create table dst clone tpch.nation {snapshot='snap'}")
	stmt.IsRestore, stmt.IsRestoreByTS, stmt.FromAccount, stmt.ToAccountId = true, true, 0, 8
	builder := NewQueryBuilder(planpb.Query_INSERT, ctx, false, true)
	snapshot, err := builder.ResolveCloneTableSourceSnapshot(stmt)
	require.NoError(t, err)
	require.NotNil(t, ctx.lookupScope)
	require.EqualValues(t, 0, ctx.lookupScope.Tenant.TenantID, "snapshot catalog owner")
	require.Same(t, ctx.named, snapshot, "snapshot data owner and timestamp")
	require.Same(t, snapshot, ctx.GetSnapshot())

	ctx.current = prior
	stmt = parse("create table dst clone tpch.nation {MO_TS=42}")
	stmt.IsRestore, stmt.IsRestoreByTS, stmt.FromAccount = true, true, 11
	builder = NewQueryBuilder(planpb.Query_INSERT, ctx, false, true)
	snapshot, err = builder.ResolveCloneTableSourceSnapshot(stmt)
	require.NoError(t, err)
	require.EqualValues(t, 11, snapshot.Tenant.TenantID)
	require.EqualValues(t, 42, snapshot.TS.PhysicalTime)

	// With no hint, the planner keeps an existing valid snapshot for normal
	// clones, but a restore's tenant-only scope is not a historical snapshot.
	stmt = parse("create table dst clone tpch.nation")
	ctx.current = prior
	builder = NewQueryBuilder(planpb.Query_INSERT, ctx, false, true)
	snapshot, err = builder.ResolveCloneTableSourceSnapshot(stmt)
	require.NoError(t, err)
	require.Same(t, prior, snapshot)
	stmt.IsRestore, stmt.FromAccount = true, 11
	builder = NewQueryBuilder(planpb.Query_INSERT, ctx, false, true)
	snapshot, err = builder.ResolveCloneTableSourceSnapshot(stmt)
	require.NoError(t, err)
	require.Nil(t, snapshot)
	require.EqualValues(t, 11, ctx.GetSnapshot().Tenant.TenantID)
}

type prepareCloneSourceContext struct {
	*cloneSourceScopeContext
	sourceErr   error
	targetReads int
}

func (c *prepareCloneSourceContext) Resolve(db, table string, snapshot *Snapshot, borrowOutput *TableResolution) (*ObjectRef, *TableDef, error) {
	borrowOutput.Reset()
	if db == "targetdb" {
		c.targetReads++
		return nil, nil, errors.New("caller destination must not be resolved")
	}
	if c.sourceErr != nil {
		return nil, nil, c.sourceErr
	}
	return c.MockCompilerContext.Resolve(db, table, snapshot, nil)
}

func (c *prepareCloneSourceContext) DatabaseExists(db string, snapshot *Snapshot) bool {
	if db == "targetdb" {
		c.targetReads++
		return false
	}
	return c.MockCompilerContext.DatabaseExists(db, snapshot)
}

func TestPrepareCloneWithAccountValidatesSourceWithoutTargetProbe(t *testing.T) {
	ctx := &prepareCloneSourceContext{cloneSourceScopeContext: &cloneSourceScopeContext{
		MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)),
	}}
	prior := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 90}, Tenant: &planpb.SnapshotTenant{TenantID: 0}}
	ctx.current = prior
	ctx.named = &Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42}, Tenant: &planpb.SnapshotTenant{TenantID: 0},
		ExtraInfo: &planpb.SnapshotExtraInfo{Level: tree.SNAPSHOTLEVELACCOUNT.String(), ObjId: 0, Name: "snap"},
	}
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table targetdb.copy clone tpch.nation {snapshot='snap'} to account tenant", 1)
	require.NoError(t, err)
	defer stmt.Free()
	prepare := &tree.PrepareStmt{Name: "p", Stmt: stmt}
	p, err := BuildPlan(ctx, prepare, false, nil)
	require.NoError(t, err)
	require.Same(t, prior, ctx.GetSnapshot())
	require.Zero(t, ctx.targetReads)
	carrier := p.GetDcl().GetPrepare()
	require.NotNil(t, carrier.GetPlan().GetDdl().GetCloneTable().GetSrcTableDef())
	require.Len(t, carrier.GetSchemas(), 1)
	require.Equal(t, "nation", carrier.GetSchemas()[0].GetObjName())
	require.Equal(t, int64(42), carrier.GetSchemas()[0].GetSnapshot().GetTS().GetPhysicalTime())

	// The same shared clone privilege check rejects a snapshot scoped to a
	// different table even when that table name resolves at its timestamp.
	ctx.named.ExtraInfo = &planpb.SnapshotExtraInfo{Level: tree.SNAPSHOTLEVELTABLE.String(), ObjId: ^uint64(0), Name: "snap"}
	_, err = BuildPlan(ctx, prepare, false, nil)
	require.Error(t, err)
	require.Same(t, prior, ctx.GetSnapshot())
	require.Zero(t, ctx.targetReads)

	ctx.named.ExtraInfo = &planpb.SnapshotExtraInfo{Level: tree.SNAPSHOTLEVELACCOUNT.String(), ObjId: 0, Name: "snap"}
	func() {
		source := ctx.tables["nation"]
		previousKind := source.TableType
		source.TableType = catalog.SystemViewRel
		defer func() { source.TableType = previousKind }()
		_, err = BuildPlan(ctx, prepare, false, nil)
		require.ErrorContains(t, err, "not BASE TABLE")
	}()
	require.Same(t, prior, ctx.GetSnapshot())
	require.Zero(t, ctx.targetReads)

	func() {
		previousAccount := ctx.GetAccountIdFunc
		ctx.GetAccountIdFunc = func() (uint32, error) { return 7, nil }
		defer func() { ctx.GetAccountIdFunc = previousAccount }()
		systemSource, parseErr := parsers.ParseOne(t.Context(), dialect.MYSQL,
			"create table targetdb.copy clone mo_catalog.mo_tables to account tenant", 1)
		require.NoError(t, parseErr)
		defer systemSource.Free()
		_, err = BuildPlan(ctx, &tree.PrepareStmt{Name: "system", Stmt: systemSource}, false, nil)
		require.ErrorContains(t, err, "non-sys account cannot clone data from system database")
	}()
	require.Same(t, prior, ctx.GetSnapshot())
	require.Zero(t, ctx.targetReads)

	ctx.sourceErr = errors.New("physical source denied")
	_, err = BuildPlan(ctx, prepare, false, nil)
	require.ErrorIs(t, err, ctx.sourceErr)
	require.Same(t, prior, ctx.GetSnapshot())
	require.Zero(t, ctx.targetReads)
}

func (c *cloneSnapshotCompilerContext) ResolveSnapshotWithSnapshotName(string) (*Snapshot, error) {
	return nil, c.snapshotErr
}

func TestBuildCloneTableSnapshotResolutionErrors(t *testing.T) {
	const snapshotName = "missing_snapshot"

	tests := []struct {
		name        string
		snapshotErr error
		wantErr     string
		wantUserErr bool
	}{
		{
			name: "missing named snapshot",
			snapshotErr: moerr.NewInternalErrorf(
				context.Background(),
				"find 0 snapshot records by name(%s), expect only 1",
				snapshotName,
			),
			wantErr:     "invalid input: snapshot 'missing_snapshot' not found",
			wantUserErr: true,
		},
		{
			name:        "other snapshot resolution error",
			snapshotErr: moerr.NewInternalError(context.Background(), "snapshot catalog unavailable"),
			wantErr:     "internal error: snapshot catalog unavailable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(
				t.Context(),
				dialect.MYSQL,
				"create table clone_target clone tpch.nation {snapshot = 'missing_snapshot'}",
				1,
			)
			require.NoError(t, err)
			defer stmt.Free()

			ctx := &cloneSnapshotCompilerContext{
				MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)),
				snapshotErr:         test.snapshotErr,
			}
			_, err = BuildPlan(ctx, stmt, false, nil)
			require.EqualError(t, err, test.wantErr)

			if test.wantUserErr {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				require.NotContains(t, err.Error(), "internal error")
				require.NotContains(t, err.Error(), "snapshot records")
			}
		})
	}
}
