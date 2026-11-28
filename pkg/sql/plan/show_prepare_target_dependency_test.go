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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

// showPrepareTargetDependency returns the catalog dependency that records the
// resolved SHOW target itself, or fails the test when the builder did not
// declare it (#33353, #33367).
func showPrepareTargetDependency(t *testing.T, p *Plan, objName string) *plan.ObjectRef {
	t.Helper()
	for _, dependency := range p.GetQuery().GetCatalogDependencies() {
		if dependency.GetObjName() == objName {
			return dependency
		}
	}
	t.Fatalf("prepared SHOW did not capture target %q; catalog dependencies: %v",
		objName, p.GetQuery().GetCatalogDependencies())
	return nil
}

// requireTargetDependencyCollected proves the captured target reaches the
// schemas PREPARE collects: through serialization and the same reset route
// buildPrepare uses, exactly as for prepared SHOW COLUMNS.
func requireTargetDependencyCollected(t *testing.T, mock *MockOptimizer, target *plan.ObjectRef, p *Plan) {
	t.Helper()
	require.Contains(t, objList(p), objSummary(target), "target missing from the built plan")
	data, err := p.Marshal()
	require.NoError(t, err)
	var decoded plan.Plan
	require.NoError(t, decoded.Unmarshal(data))
	schemas, _, err := ResetPreparePlan(&mock.ctxt, &decoded)
	require.NoError(t, err)
	require.Contains(t, schemas, target,
		"PREPARE must collect the SHOW target for EXECUTE-time invalidation")
}

func objList(p *Plan) []string {
	deps := p.GetQuery().GetCatalogDependencies()
	out := make([]string, 0, len(deps))
	for _, d := range deps {
		out = append(out, objSummary(d))
	}
	return out
}

func objSummary(d *plan.ObjectRef) string {
	if d == nil {
		return "<nil>"
	}
	return d.GetObjName()
}

// TestShowCreateTableTracksTargetDependency covers #33353: SHOW CREATE TABLE is
// rewritten into a literal SELECT that never scans the target, so without the
// builder-declared dependency a warmed prepared handle has nothing to
// invalidate against cross-session COPY ALTER.
func TestShowCreateTableTracksTargetDependency(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	def := mock.ctxt.tables["nation"]
	require.NotNil(t, def)
	def.DbId, def.TblId, def.Version = 42, 100, 7

	p, err := runOneStmt(mock, t, "show create table nation")
	require.NoError(t, err)
	target := showPrepareTargetDependency(t, p, "nation")
	require.Equal(t, int64(42), target.GetDb())
	require.Equal(t, int64(100), target.GetObj())
	require.Equal(t, int64(7), target.GetServer())
	require.Equal(t, int64(target.GetDb()), target.GetSchema(), "database identity must stay with the captured target")
	require.Nil(t, target.GetSnapshot(), "ordinary SHOW CREATE captures the live table, not history")
	requireTargetDependencyCollected(t, mock, target, p)
}

// TestShowCreateViewTracksTargetDependency covers the view shape of #33353:
// SHOW CREATE VIEW materializes the stored definition, and SHOW CREATE TABLE on
// a view delegates to the same builder, so both must capture the view itself.
func TestShowCreateViewTracksTargetDependency(t *testing.T) {
	for _, sql := range []string{
		"show create view v1",
		"show create table v1",
	} {
		t.Run(sql, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			def := mock.ctxt.tables["v1"]
			require.NotNil(t, def)
			def.DbId, def.TblId, def.Version = 52, 200, 9
			def.TableType = catalog.SystemViewRel
			def.ViewSql.View = `{"Stmt":"create view v1 as select n_name from nation","DefaultDatabase":"tpch"}`

			p, err := runOneStmt(mock, t, sql)
			require.NoError(t, err)
			target := showPrepareTargetDependency(t, p, "v1")
			require.Equal(t, int64(52), target.GetDb())
			require.Equal(t, int64(200), target.GetObj())
			require.Equal(t, int64(9), target.GetServer())
			require.Nil(t, target.GetSnapshot())
			requireTargetDependencyCollected(t, mock, target, p)
		})
	}
}

// TestShowIndexTracksTargetDependency covers #33367: SHOW INDEX reads catalog
// rows rather than the target, so a dropped table otherwise keeps serving
// warmed handles as an empty success. The builder must capture the resolved
// object for EXECUTE-time invalidation, including the WHERE variant.
func TestShowIndexTracksTargetDependency(t *testing.T) {
	for _, tc := range []struct {
		sql   string
		table string
	}{
		{sql: "show index from single_idx_t", table: "single_idx_t"},
		{sql: "show index from tpch.nation", table: "nation"},
		{sql: "show index from single_idx_t where Key_name = 'PRIMARY'", table: "single_idx_t"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			def := mock.ctxt.tables[tc.table]
			require.NotNil(t, def)
			def.DbId, def.TblId, def.Version = 62, 300, 13

			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			target := showPrepareTargetDependency(t, p, tc.table)
			require.Equal(t, int64(62), target.GetDb())
			require.Equal(t, int64(300), target.GetObj())
			require.Equal(t, int64(13), target.GetServer())
			require.Nil(t, target.GetSnapshot())
			requireTargetDependencyCollected(t, mock, target, p)
		})
	}
}

// TestShowMetadataDependenciesCarryResolvedSnapshot pins the qualifier
// contract of the captured dependency: a snapshot-qualified SHOW create keeps
// the immutable historical identity (named snapshot recorded for the
// missing-snapshot lifecycle check), while an ordinary target ref revalidates
// the live table.
func TestShowMetadataDependenciesCarryResolvedSnapshot(t *testing.T) {
	snap := &plan.Snapshot{
		TS:        &timestamp.Timestamp{PhysicalTime: 1748352000},
		ExtraInfo: &plan.SnapshotExtraInfo{Name: "sp_test_33353", Level: "database"},
	}
	require.True(t, IsSnapshotValid(snap))

	historical := &Query{}
	setShowMetadataDependencies(historical, &plan.ObjectRef{SchemaName: "db1", ObjName: "t1"},
		&plan.TableDef{DbId: 7, TblId: 9, Version: 11, DbName: "db1", Name: "t1"}, nil, false, snap)
	require.Len(t, historical.CatalogDependencies, 1)
	captured := historical.CatalogDependencies[0]
	require.NotNil(t, captured.GetSnapshot(), "snapshot-qualified SHOW CREATE must record the resolved named snapshot")
	require.Equal(t, "sp_test_33353", captured.GetSnapshot().GetExtraInfo().GetName(),
		"the named snapshot identity must survive capture for the missing-snapshot lifecycle check")
	require.Equal(t, "database", captured.GetSnapshot().GetExtraInfo().GetLevel())
	require.True(t, IsSnapshotValid(captured.GetSnapshot()))

	live := &Query{}
	setShowMetadataDependencies(live, &plan.ObjectRef{SchemaName: "db1", ObjName: "t1"},
		&plan.TableDef{DbId: 7, TblId: 9, Version: 11, DbName: "db1", Name: "t1"}, nil, false, nil)
	require.Len(t, live.CatalogDependencies, 1)
	require.Nil(t, live.CatalogDependencies[0].GetSnapshot(),
		"an ordinary target must revalidate the live table, not freeze history")
}
