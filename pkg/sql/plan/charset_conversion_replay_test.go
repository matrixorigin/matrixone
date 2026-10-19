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

package plan

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestCharsetConversionRetainsTypedComputation(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion118)
	for _, expression := range []string{"a/b", "cast(a/b as char(32))", "cast(a/b as char(64))"} {
		for _, collation := range []string{"utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci"} {
			t.Run(expression+"/"+collation, func(t *testing.T) {
				compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
				compiler.tables["t"] = &planpb.TableDef{Name: "t"}
				build := func(increment int64, sql string) *planpb.Plan {
					t.Helper()
					stmt, err := mysql.ParseOne(t.Context(), sql, 1)
					require.NoError(t, err)
					defer stmt.Free()
					p, err := BuildPlan(divPrecisionCompilerContext{CompilerContext: compiler, increment: increment}, stmt, false)
					require.NoError(t, err)
					return p
				}
				source := build(10, "create table t(a decimal(10,2),b decimal(10,2),g varchar(64) generated always as("+expression+") stored,d varchar(64) default (cast(1 as decimal(10,2))/cast(3 as decimal(10,2))),u varchar(64) on update current_timestamp()) collate utf8mb4_bin").GetDdl().GetCreateTable().TableDef
				compiler.tables["t"] = source
				compiler.objects["t"] = &ObjectRef{SchemaName: "tpch", ObjName: "t"}
				before := proto.Clone(source)
				target := build(0, "alter table t convert to character set utf8mb4 collate "+collation).GetDdl().GetAlterTable().CopyTableDef
				oldExpr := FindColumn(source.Cols, "g").GeneratedCol.Expr
				newCol := FindColumn(target.Cols, "g")
				newExpr := newCol.GeneratedCol.Expr
				computation := oldExpr
				if root := oldExpr.GetF(); root.Func.ObjName == "cast_strict" {
					computation = root.Args[0]
				}
				if collation == "utf8mb4_bin" {
					require.True(t, proto.Equal(oldExpr, newExpr), "unchanged destination must retain the complete expression")
				} else {
					require.NotNil(t, newExpr.GetF())
					require.Equal(t, "cast_strict", newExpr.GetF().Func.ObjName)
					require.True(t, proto.Equal(computation, newExpr.GetF().Args[0]), "only the destination assignment may change; a root authored CAST is not that assignment")
					require.Equal(t, newCol.Typ.Charset, newExpr.Typ.Charset)
				}
				replay := ddlReplayForTable(WithPersistedDDLReplay(context.Background(), source, target), target.Name)
				require.NotNil(t, replay.columns["g"].generated, "COPY reconstruction must not re-author the retained computation")
				require.True(t, proto.Equal(newExpr, replay.columns["g"].generated.Expr))
				for _, name := range []string{"d", "u"} {
					oldCol, newCol := FindColumn(source.Cols, name), FindColumn(target.Cols, name)
					oldValue, newValue := oldCol.Default.Expr, newCol.Default.Expr
					if name == "u" {
						oldValue, newValue = oldCol.OnUpdate.Expr, newCol.OnUpdate.Expr
					}
					if collation == "utf8mb4_bin" {
						require.True(t, proto.Equal(oldValue, newValue), name)
					} else {
						input := oldValue
						if name == "u" {
							require.Equal(t, "cast_strict", oldValue.GetF().Func.ObjName)
							input = oldValue.GetF().Args[0]
						}
						require.True(t, proto.Equal(input, newValue.GetF().Args[0]), name)
					}
					entry := replay.columns[name]
					if name == "u" {
						require.NotNil(t, entry.onUpdate)
					} else {
						require.NotNil(t, entry.defaultExpr)
					}
				}
				current := newExpr
				for i := 0; i < 8; i++ {
					destination := newCol.Typ
					destination.Charset = uint32(3 - i%2)
					destination.CollationVersion = 0
					next, err := convertedPersistedAssignment(context.Background(), current, destination)
					require.NoError(t, err)
					current = next
					require.True(t, proto.Equal(computation, current.GetF().Args[0]), "round trips must not grow assignment layers or remove authored CAST")
				}
				require.True(t, proto.Equal(before, source), "conversion planning must not mutate catalog expressions")
			})
		}
	}
}
