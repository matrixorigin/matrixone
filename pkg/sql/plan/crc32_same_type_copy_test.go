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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// A same-type competitor makes stale ColPos pass a type-only validator.
// Use the real ALTER parser/planner and its emitted CREATE, not a hand-reordered
// target. The no-replay subtest is an explicitly injected fallback control;
// production installs replay, and the test does not claim otherwise.
func TestLegacyCRC32SameTypeCopyInputIdentity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		clause     string
		competitor int32
	}{
		{"move_referenced_input", "modify column val json after updated_at", 1},
		{"move_competing_input", "modify column updated_at json first", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			base := mock.ctxt.tables["t_on_update_gen"]
			base.Indexes = nil
			base.Name2ColIndex = make(map[string]int32, len(base.Cols))
			for i, col := range base.Cols {
				base.Name2ColIndex[col.Name] = int32(i)
			}
			for _, name := range []string{"val", "updated_at"} {
				col := FindColumn(base.Cols, name)
				require.NotNil(t, col)
				col.Typ = planpb.Type{Id: int32(types.T_json)}
				col.Default = &planpb.Default{NullAbility: true}
				col.OnUpdate = nil
			}
			shape, err := mysql.ParseOne(context.Background(), "create table shape(g bigint unsigned)", 1)
			require.NoError(t, err)
			defer shape.Free()
			generated := FindColumn(base.Cols, "g")
			require.NotNil(t, generated)
			generated.Typ, err = getTypeFromAst(context.Background(), shape.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef).Type)
			require.NoError(t, err)
			legacy := legacyCRC32Expr()
			legacy.GetF().Args[0].GetCol().ColPos = 1
			legacy.GetF().Args[0].GetCol().Name = "val"
			generated.GeneratedCol = &planpb.GeneratedCol{Expr: legacy, OriginString: "crc32(val)", IsStored: true}
			generated.Default = &planpb.Default{NullAbility: true}
			original := proto.Clone(base).(*planpb.TableDef)

			built, err := runOneStmt(mock, t, "alter table constraint_test.t_on_update_gen "+tc.clause)
			require.NoError(t, err)
			alter := built.GetDdl().GetAlterTable()
			require.NotNil(t, alter)
			require.Equal(t, planpb.AlterTable_COPY, alter.AlgorithmType)
			target := alter.CopyTableDef
			require.Equal(t, "val", target.Cols[2].Name)
			require.Equal(t, "updated_at", target.Cols[tc.competitor].Name)
			require.Equal(t, int32(types.T_json), target.Cols[tc.competitor].Typ.Id)
			want := DeepCopyExpr(legacy)
			want.GetF().Args[0].GetCol().ColPos = 2
			require.True(t, proto.Equal(want, FindColumn(target.Cols, "g").GeneratedCol.Expr), "COPY source must already reference val, not the same-type competitor")
			require.True(t, proto.Equal(original, base), "ALTER must not mutate source catalog")
			targetBeforeCreate := proto.Clone(target).(*planpb.TableDef)
			parent := mock.ctxt.GetContext()

			for _, useReplay := range []bool{true, false} {
				name := "production_replay"
				if !useReplay {
					name = "injected_no_replay_fallback"
				}
				t.Run(name, func(t *testing.T) {
					ctx := context.WithValue(parent, defines.CRC32CopyExpressionsKey{}, target)
					if useReplay {
						ctx = WithPersistedDDLReplay(ctx, alter.TableDef, target)
						replay := ddlReplayForTable(ctx, target.Name)
						require.NotNil(t, replay)
						require.NotNil(t, replay.columns["g"])
						require.NotNil(t, replay.columns["g"].generated, "same-type reorder must not decline generated replay")
						require.True(t, proto.Equal(want, replay.columns["g"].generated.Expr))
					}
					mock.ctxt.SetContext(ctx)
					defer mock.ctxt.SetContext(parent)
					rebuilt, err := runOneStmt(mock, t, alter.CreateTmpTableSql)
					require.NoError(t, err)
					copied := FindColumn(rebuilt.GetDdl().GetCreateTable().TableDef.Cols, "g")
					require.NotNil(t, copied)
					require.NotNil(t, copied.GeneratedCol)
					require.True(t, copied.GeneratedCol.IsStored)
					require.True(t, proto.Equal(want, copied.GeneratedCol.Expr))
					crc := copied.GeneratedCol.Expr.GetF()
					require.Equal(t, function.EncodeOverloadID(function.CRC32, function.CRC32LegacyOverload), crc.Func.Obj)
					require.Equal(t, int32(2), crc.Args[0].GetCol().ColPos)
					require.NotEqual(t, tc.competitor, crc.Args[0].GetCol().ColPos)
					crc.Args[0].GetCol().ColPos = 99
					require.True(t, proto.Equal(targetBeforeCreate, target), "reconstruction must own its expression")
					require.True(t, proto.Equal(original, base))
				})
			}
		})
	}
}
