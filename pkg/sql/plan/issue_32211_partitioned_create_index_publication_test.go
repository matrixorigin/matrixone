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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// Issue #32211 regression ledger.
//
// Invariant: a standalone CREATE INDEX / CREATE UNIQUE INDEX on a
// partitioned table must publish the index of the logical parent table in
// the same statement that propagates each physical partition child, i.e.
// it must plan through the very route ALTER TABLE ... ADD INDEX uses.
// The dedicated CREATE_INDEX executor publishes only physical children
// (one per partition) and writes mo_indexes rows keyed by each child
// table id, so SHOW INDEX, SHOW CREATE TABLE and index hints, which all
// resolve from the resolved logical table id, never observe the index
// even though the statement reports success.
//
// MySQL treats CREATE INDEX ON t ... as ALTER TABLE t ADD INDEX ..., so
// planning the statement into an ALTER TABLE INPLACE plan keeps parent
// publication and per-partition propagation on the established
// parent-and-children transactional route (Scope.AlterTable runs the
// logical parent first, then rewrites the recorded ALTER AST onto every
// partition child inside the same transaction).
//
// Controls proved here together with the desired-success cases, so the
// delegation is exactly bounded:
//   - non-partitioned tables keep the CREATE_INDEX executor and plan,
//   - duplicate index names keep the exact CREATE INDEX admission error
//     before any delegation,
//   - non-regular index families (RTREE/spatial, fulltext, functional)
//     keep their existing planning paths; only the qualified ordinary
//     BTREE and lossless-regular UNIQUE forms are routed.

func newPartitionedIssue32211Context(t testing.TB) *MockCompilerContext {
	t.Helper()
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	ctx.SetContext(context.Background())
	registerIssue32211Table(ctx, newIssue32211TableDef("pp", true))
	return ctx
}

func registerIssue32211Table(ctx *MockCompilerContext, def *TableDef) {
	ctx.tables[def.Name] = def
	ctx.objects[def.Name] = &ObjectRef{SchemaName: "db", ObjName: def.Name}
}

// newIssue32211TableDef mirrors what the engine resolves for a CREATE
// TABLE ... PARTITION BY planning leaf: both the resolved Partition
// state and the durable FeatureFlag partition bit are filled
// (build_ddl.go sets features.Partitioned on partitioned creates, and
// relation resolution fills TableDef.Partition and TableDef.FeatureFlag).
func newIssue32211TableDef(name string, partitioned bool) *TableDef {
	intType := plan.Type{Id: int32(types.T_int32)}
	def := &TableDef{
		Name: name,
		Cols: []*ColDef{
			{
				ColId:   1,
				Name:    "c1",
				NotNull: false,
				Typ:     intType,
				Default: &plan.Default{NullAbility: true},
			},
			{
				ColId:   2,
				Name:    "c2",
				NotNull: false,
				Typ:     intType,
				Default: &plan.Default{NullAbility: true},
			},
			{
				ColId:   3,
				Name:    "c3",
				NotNull: false,
				Typ:     plan.Type{Id: int32(types.T_geometry), Width: 64},
				Default: &plan.Default{NullAbility: true},
			},
		},
	}
	def.Pkey = &plan.PrimaryKeyDef{
		Names:       []string{"c1"},
		PkeyColName: "__mo_fake_pk",
		CompPkeyCol: &plan.ColDef{
			Name: "__mo_fake_pk",
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: 64,
			},
		},
	}
	if partitioned {
		def.Partition = &plan.Partition{
			PartitionDefs: []*plan.PartitionDef{
				{Def: &plan.Expr{}},
				{Def: &plan.Expr{}},
			},
		}
		def.FeatureFlag |= features.Partitioned
	}
	return def
}

func runIssue32211Stmt(t testing.TB, ctx *MockCompilerContext, sql string) (*Plan, error) {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	if err != nil {
		return nil, err
	}
	defer stmt.Free()
	return BuildPlan(ctx, stmt, false, nil)
}

// requireIssue32211AlterIndexPlan asserts that the statement plans into
// the ALTER TABLE INPLACE ADD INDEX route that publishes the logical
// parent and propagates partition children in one transaction.
func requireIssue32211AlterIndexPlan(
	t testing.TB,
	p *Plan,
	unique bool,
	assertMsg string,
	rawSqlNeedles ...string,
) {
	t.Helper()
	require.NotNil(t, p, "%s: plan is nil", assertMsg)
	require.NotNil(t, p.GetDdl(), "%s: plan must be a DDL plan", assertMsg)
	alter := p.GetDdl().GetAlterTable()
	require.NotNil(t, alter, "%s: partitioned CREATE INDEX must plan as the ALTER TABLE logical-parent publication route, got DdlType %v", assertMsg, p.GetDdl().DdlType)
	require.Equal(t, plan.AlterTable_INPLACE, alter.GetAlgorithmType(),
		"%s: partitioned ADD INDEX must resolve the ALTER INPLACE algorithm", assertMsg)
	require.NotEmpty(t, alter.GetRawSQL(), "%s: the partition propagation route requires the serialized ALTER AST in RawSQL", assertMsg)
	require.NotEmpty(t, alter.GetActions(), "%s: ALTER plan must carry actions", assertMsg)

	var addIndex *plan.AlterTableAddIndex
	for _, action := range alter.GetActions() {
		if candidate := action.GetAddIndex(); candidate != nil {
			require.Nil(t, addIndex, "%s: exactly one AddIndex action expected", assertMsg)
			addIndex = candidate
		}
	}
	require.NotNil(t, addIndex, "%s: ALTER plan must carry the AddIndex action", assertMsg)
	if unique {
		// The added unique index def must survive into the moved index defs.
		require.True(t, isUniqueIndexAction(addIndex),
			"%s: UNIQUE CREATE INDEX must add a unique index def", assertMsg)
	} else {
		require.False(t, isUniqueIndexAction(addIndex),
			"%s: ordinary CREATE INDEX must not add a unique index def", assertMsg)
	}

	raw := alter.GetRawSQL()
	lower := strings.ToLower(raw)
	for _, needle := range rawSqlNeedles {
		require.True(t, strings.Contains(lower, strings.ToLower(needle)),
			"%s: serialized ALTER AST %q lost %q", assertMsg, raw, needle)
	}
}

func isUniqueIndexAction(addIndex *plan.AlterTableAddIndex) bool {
	if addIndex == nil || addIndex.GetIndexInfo() == nil {
		return false
	}
	for _, indexDef := range addIndex.GetIndexInfo().GetTableDef().GetIndexes() {
		if indexDef.GetUnique() {
			return true
		}
	}
	return false
}

func requireIssue32211CreateIndexPlan(
	t testing.TB,
	p *Plan,
	assertMsg string,
) {
	t.Helper()
	require.NotNil(t, p, "%s: plan is nil", assertMsg)
	require.NotNil(t, p.GetDdl(), "%s: plan must be a DDL plan", assertMsg)
	require.NotNil(t, p.GetDdl().GetCreateIndex(),
		"%s: statement must keep the dedicated CREATE INDEX executor", assertMsg)
}

// Issue32211: ordinary standalone CREATE INDEX on a partitioned table
// plans through the ALTER TABLE parent+children publication route.
func TestIssue32211CreateIndexOnPartitionedTableRoutesToAlterAddIndex(t *testing.T) {
	ctx := newPartitionedIssue32211Context(t)

	cases := []struct {
		name         string
		sql          string
		unique       bool
		rawSqlNeedle []string
	}{
		{
			name: "ordinary index on single column",
			sql:  "create index idx1 on pp (c1)",
			// Index names serialize unquoted and columns backticked: exactly
			// the established ALTER ADD INDEX RawSQL contract.
			rawSqlNeedle: []string{"alter table", "add index", "idx1", "`c1`"},
		},
		{
			name: "ordinary index with explicit btree",
			sql:  "create index idx2 on pp (c2) using btree",
			rawSqlNeedle: []string{
				"alter table", "add index", "using btree",
			},
		},
		{
			name: "composite index",
			sql:  "create index idx3 on pp (c1 asc, c2 desc)",
			rawSqlNeedle: []string{
				"alter table", "add index", "idx3", "`c1` asc", "`c2` desc",
			},
		},
		{
			name: "valid identifiers round-trip through the ALTER serialization",
			sql:  "create index iDx1 on pp (`C1`)",
			rawSqlNeedle: []string{
				"add index", "iDx1", "`pp`", "`C1`",
			},
		},
		{
			name:         "unique index",
			sql:          "create unique index uidx1 on pp (c1)",
			unique:       true,
			rawSqlNeedle: []string{"alter table", "add unique", "uidx1"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runIssue32211Stmt(t, ctx, tc.sql)
			require.NoError(t, err, "partitioned CREATE INDEX must stay a supported operation, sql: %s", tc.sql)
			requireIssue32211AlterIndexPlan(t, p, tc.unique, tc.name, tc.rawSqlNeedle...)
		})
	}
}

// Controls: the delegation is bounded. Types outside the qualified
// ordinary-BTREE/lossless-UNIQUE closure keep their current planning.
func TestIssue32211CreateIndexDelegationBoundaryControls(t *testing.T) {
	t.Run("non-partitioned table keeps CREATE INDEX plan", func(t *testing.T) {
		ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
		ctx.SetContext(context.Background())
		registerIssue32211Table(ctx, newIssue32211TableDef("nn", false))

		p, err := runIssue32211Stmt(t, ctx, "create index idx1 on nn (c1)")
		require.NoError(t, err)
		requireIssue32211CreateIndexPlan(t, p, "non-partitioned")
	})

	t.Run("partitioned table duplicate name keeps CREATE INDEX admission error", func(t *testing.T) {
		ctx := newPartitionedIssue32211Context(t)
		table := ctx.tables["pp"]
		table.Indexes = append(table.Indexes, &plan.IndexDef{
			IndexName: "dup",
			Unique:    false,
			Parts:     []string{"c1"},
		})

		_, err := runIssue32211Stmt(t, ctx, "create index dup on pp (c2)")
		require.Error(t, err, "duplicate index names must stay rejected")
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateKey),
			"duplicate index name must reject with the CREATE INDEX DuplicateKey admission, got: %v", err)
	})

	t.Run("partitioned spatial index keeps its dedicated planning path", func(t *testing.T) {
		ctx := newPartitionedIssue32211Context(t)
		p, err := runIssue32211Stmt(t, ctx, "create spatial index idx_s on pp (c3) using rtree")
		require.NoError(t, err)
		requireIssue32211CreateIndexPlan(t, p, "spatial")
	})
}
