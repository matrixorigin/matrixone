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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// Exercise the internal CREATE consumer independently of the ALTER guard.
// The final schema transition is explicit test injection, not evidence that
// production omits replay: the replay-enabled case must naturally decline g.
func TestLegacyCRC32CopyConsumerAutoIncrementInput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		auto   bool
		replay bool
	}{
		{"ordinary_replay", false, true},
		{"ordinary_missing_replay", false, false},
		{"auto_replay_declined", true, true},
		{"auto_missing_replay", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := legacyCRC32AutoIncrementCopyFixture(t)
			original := proto.Clone(mock.ctxt.tables["t_on_update_gen"]).(*planpb.TableDef)
			target := proto.Clone(original).(*planpb.TableDef)
			target.Name = "t_crc32_copy"
			FindColumn(target.Cols, "id").Typ.AutoIncr = tc.auto
			before := proto.Clone(target).(*planpb.TableDef)
			parent := mock.ctxt.GetContext()
			ctx := context.WithValue(parent, defines.CRC32CopyExpressionsKey{}, target)
			if tc.replay {
				ctx = WithPersistedDDLReplay(ctx, original, target)
				replay := ddlReplayForTable(ctx, target.Name)
				require.NotNil(t, replay)
				require.NotNil(t, replay.columns["g"])
				if tc.auto {
					require.Nil(t, replay.columns["g"].generated, "real compatibility predicate must decline AutoIncr change")
				} else {
					require.NotNil(t, replay.columns["g"].generated)
				}
			}
			mock.ctxt.SetContext(ctx)
			defer mock.ctxt.SetContext(parent)
			attribute := ""
			if tc.auto {
				attribute = " auto_increment"
			}
			built, err := runOneStmt(mock, t, "create table t_crc32_copy(id bigint unsigned not null"+attribute+" primary key, val json, g bigint unsigned generated always as (crc32(val) + id) stored)")
			if tc.auto {
				require.Error(t, err, "legacy fallback must not restore an illegal generated input after replay declines")
				require.ErrorContains(t, err, "explicit table rebuild")
			} else {
				require.NoError(t, err)
				got := FindColumn(built.GetDdl().GetCreateTable().TableDef.Cols, "g")
				require.True(t, proto.Equal(FindColumn(target.Cols, "g").GeneratedCol.Expr, got.GeneratedCol.Expr))
				require.True(t, containsLegacyCRC32(got.GeneratedCol.Expr))
			}
			require.True(t, proto.Equal(original, mock.ctxt.tables["t_on_update_gen"]))
			require.True(t, proto.Equal(before, target), "consumer must not mutate replay-owned source")
		})
	}
}

// Inject a final dependency graph to isolate the retained-expression fallback.
// Unlike the COPY tests above, this is a helper-level adversarial consumer test,
// not a claim that production admits this intermediate schema transition.
func TestLegacyCRC32FallbackRejectsIndirectAutoIncrementInput(t *testing.T) {
	mock := legacyCRC32AutoIncrementCopyFixture(t)
	original := mock.ctxt.tables["t_on_update_gen"]
	target := proto.Clone(original).(*planpb.TableDef)
	inputType := FindColumn(target.Cols, "id").Typ
	inputType.AutoIncr = true
	position := int32(len(target.Cols))
	target.Cols = append(target.Cols, &planpb.ColDef{Name: "a", Typ: inputType})
	dependency, err := BindFuncExprImplByPlanExpr(mock.ctxt.GetContext(), "+", []*Expr{
		{Typ: inputType, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: position, Name: "a"}}},
		MakePlan2Uint64ConstExprWithType(1),
	})
	require.NoError(t, err)
	FindColumn(target.Cols, "id").GeneratedCol = &planpb.GeneratedCol{Expr: dependency, IsStored: true}
	before := proto.Clone(target).(*planpb.TableDef)
	statement, err := mysql.ParseOne(context.Background(), "create table shape(g bigint unsigned generated always as (crc32(val) + id) stored)", 1)
	require.NoError(t, err)
	defer statement.Free()
	column := statement.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
	owner := FindColumn(target.Cols, "g")
	_, err = buildGeneratedExpr(mock.ctxt.GetContext(), column, owner.Typ, target.Cols, mock.ctxt.GetProcess(), owner)
	require.ErrorContains(t, err, "cannot refer to auto-increment column")
	require.True(t, proto.Equal(before, target), "dependency validation must not mutate retained expressions")
	require.False(t, FindColumn(original.Cols, "id").Typ.AutoIncr)
	require.Nil(t, FindColumn(original.Cols, "id").GeneratedCol)
}
