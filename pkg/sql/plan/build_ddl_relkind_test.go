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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// relKindOf returns the relkind the CREATE TABLE plan stamps on the table.
func relKindOf(t *testing.T, tableDef *plan.TableDef) (string, bool) {
	t.Helper()
	for _, def := range tableDef.GetDefs() {
		proDef, ok := def.Def.(*plan.TableDef_DefType_Properties)
		if !ok {
			continue
		}
		for _, kv := range proDef.Properties.Properties {
			if kv.Key == catalog.SystemRelAttr_Kind {
				return kv.Value, true
			}
		}
	}
	return "", false
}

func buildCreateTablePlan(t *testing.T, ctx context.Context, sql string) *plan.TableDef {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()

	cc := NewMockCompilerContext(false)
	cc.SetContext(ctx)
	p, err := BuildPlan(cc, stmt, false)
	require.NoError(t, err)
	return p.GetDdl().GetCreateTable().GetTableDef()
}

// Without a carried relkind the plan derives one from the table name: an ordinary
// name gets "r", a hidden index name gets "".
func TestBuildCreateTableDerivesRelKindFromName(t *testing.T) {
	ctx := context.Background()

	kind, ok := relKindOf(t, buildCreateTablePlan(t, ctx, "create table t(a int)"))
	require.True(t, ok)
	require.Equal(t, catalog.SystemOrdinaryRel, kind)

	hidden := catalog.SecondaryIndexTableNamePrefix + "0195f1e0"
	kind, ok = relKindOf(t, buildCreateTablePlan(t, ctx,
		"create table `"+hidden+"`(a int)"))
	require.True(t, ok)
	require.Equal(t, "", kind, "a hidden name derives the empty kind")
}

// ALTER TABLE ... COPY and the truncate/recreate path rebuild a table from regenerated
// DDL, which cannot express relkind, and hand the original's kind through the statement
// context. The plan must adopt it verbatim instead of deriving one from the replica's
// name -- otherwise an index metadata table loses the kind that is the only thing keeping
// it out of the relkind-keyed restore/CLONE filters.
func TestBuildCreateTableKeepsCarriedRelKind(t *testing.T) {
	// A real replica is "<original>_copy_<uuid>"; the plan derives "" from the prefix
	// either way, so a short stand-in exercises the same branch. The production name is
	// longer than MaxIdentifierLength and is exempt only because the ALTER runs on the
	// internal executor (validateCreateTableIdentifier).
	hidden := catalog.SecondaryIndexTableNamePrefix + "0195f1e0_copy_0195f1e1"

	for _, carried := range []string{
		catalog.Hnsw_TblType_Metadata,
		catalog.Cagra_TblType_Metadata,
		catalog.Ivfpq_TblType_Metadata,
		catalog.FullText2Index_TblType_Metadata,
		catalog.SystemIndexRel,
		catalog.SystemOrdinaryRel,
	} {
		t.Run(carried, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), defines.RelKindKey{}, carried)
			kind, ok := relKindOf(t, buildCreateTablePlan(t, ctx,
				"create table `"+hidden+"`(a int)"))
			require.True(t, ok)
			require.Equal(t, carried, kind)
		})
	}

	// The empty kind is a real value, not "unset": a generic hidden table carries it and
	// must keep carrying it rather than being promoted to an ordinary table.
	t.Run("empty is carried, not ignored", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), defines.RelKindKey{}, "")
		kind, ok := relKindOf(t, buildCreateTablePlan(t, ctx, "create table t(a int)"))
		require.True(t, ok)
		require.Equal(t, "", kind, "an ordinary name does not override the carried empty kind")
	})
}

// --- ALTER TABLE -----------------------------------------------------------

// ALTER TABLE ... ADD COLUMN is resolved to COPY, which rebuilds the table from
// constructCreateTableSQL. That regenerated DDL cannot express relkind, so the replica would
// take the kind buildCreateTable derives from its name. These pin the two halves of the fix:
// the original's kind is present on the plan for alter.go to carry, and the generated DDL is
// indeed missing it -- which is why carrying it is necessary at all.
func TestAlterTableAddColumnCarriesRelKind(t *testing.T) {
	for _, kind := range []string{
		catalog.Hnsw_TblType_Metadata,
		catalog.Cagra_TblType_Metadata,
		catalog.Ivfpq_TblType_Metadata,
		catalog.FullText2Index_TblType_Metadata,
		catalog.SystemIndexRel,
	} {
		t.Run(kind, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			mock.ctxt.tables["t1"].TableType = kind

			logicPlan, err := buildSingleStmt(mock, t, "alter table t1 add column nrow bigint")
			require.NoError(t, err)

			at := logicPlan.GetDdl().GetAlterTable()
			require.NotNil(t, at)
			require.Equal(t, plan.AlterTable_COPY, at.AlgorithmType,
				"ADD COLUMN is a COPY; that is what loses the kind")

			// alter.go reads this to populate StatementOption.WithKeepRelKind.
			require.Equal(t, kind, at.GetTableDef().GetTableType(),
				"the original kind must survive onto the plan for the copy to carry it")

			require.NotEmpty(t, at.CreateTmpTableSql)
			if len(kind) > 2 { // a one-letter kind like "i" occurs in ordinary DDL text
				require.NotContains(t, at.CreateTmpTableSql, kind,
					"the regenerated DDL cannot carry relkind -- the gap the option closes")
			}
		})
	}
}

// An ordinary table is unaffected: its kind is carried too, and it is the same "r" the
// replica's name would have derived anyway, so behaviour is unchanged.
func TestAlterTableAddColumnOrdinaryTableKeepsOrdinaryKind(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.tables["t1"].TableType = catalog.SystemOrdinaryRel

	logicPlan, err := buildSingleStmt(mock, t, "alter table t1 add column d bigint")
	require.NoError(t, err)

	at := logicPlan.GetDdl().GetAlterTable()
	require.Equal(t, catalog.SystemOrdinaryRel, at.GetTableDef().GetTableType())
}
