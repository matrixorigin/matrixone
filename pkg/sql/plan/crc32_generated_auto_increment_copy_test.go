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
	"testing"

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// Bind a lawful composite expression with the production CREATE planner, then
// inject only its persisted legacy CRC32 overload. Do not infer legacy identity
// from freshly authored CRC32 SQL or inject stale column coordinates.
func legacyCRC32AutoIncrementCopyFixture(t *testing.T) *MockOptimizer {
	t.Helper()
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	built, err := runOneStmt(mock, t, "create table t_on_update_gen(id bigint unsigned not null primary key, val json, g bigint unsigned generated always as (crc32(val) + id) stored)")
	require.NoError(t, err)
	table := built.GetDdl().GetCreateTable().TableDef
	generated := FindColumn(table.Cols, "g")
	require.NotNil(t, generated)
	require.NotNil(t, generated.GeneratedCol)
	converted := 0
	err = planpb.VisitExprTree(generated.GeneratedCol.Expr, func(expr *planpb.Expr) error {
		f := expr.GetF()
		if f != nil && f.Func.Obj == function.EncodeOverloadID(function.CRC32, function.CRC32JSONTextOverload) {
			f.Func.Obj = function.EncodeOverloadID(function.CRC32, function.CRC32LegacyOverload)
			converted++
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, converted)
	require.True(t, containsLegacyCRC32(generated.GeneratedCol.Expr))
	mock.ctxt.tables["t_on_update_gen"] = table
	return mock
}

func TestLegacyCRC32CopyRejectsAutoIncrementInput(t *testing.T) {
	for _, clause := range []string{
		"modify column id bigint unsigned not null auto_increment",
		"change column id id bigint unsigned not null auto_increment",
	} {
		t.Run(clause, func(t *testing.T) {
			mock := legacyCRC32AutoIncrementCopyFixture(t)
			original := proto.Clone(mock.ctxt.tables["t_on_update_gen"]).(*planpb.TableDef)
			_, err := runOneStmt(mock, t, "alter table constraint_test.t_on_update_gen "+clause+", algorithm=copy")
			require.Error(t, err, "COPY must reject making a legacy generated input auto-increment")
			require.ErrorContains(t, err, "explicit table rebuild")
			require.True(t, proto.Equal(original, mock.ctxt.tables["t_on_update_gen"]), "rejected ALTER must not mutate its source catalog")
		})
	}
}

func TestLegacyCRC32CopyKeepsOrdinaryIntegerInput(t *testing.T) {
	mock := legacyCRC32AutoIncrementCopyFixture(t)
	original := proto.Clone(mock.ctxt.tables["t_on_update_gen"]).(*planpb.TableDef)
	built, err := runOneStmt(mock, t, "alter table constraint_test.t_on_update_gen modify column id bigint unsigned not null, algorithm=copy")
	require.NoError(t, err)
	alter := built.GetDdl().GetAlterTable()
	require.Equal(t, planpb.AlterTable_COPY, alter.AlgorithmType)
	require.False(t, FindColumn(alter.CopyTableDef.Cols, "id").Typ.AutoIncr)
	require.True(t, containsLegacyCRC32(FindColumn(alter.CopyTableDef.Cols, "g").GeneratedCol.Expr))
	require.True(t, proto.Equal(original, mock.ctxt.tables["t_on_update_gen"]))
}
