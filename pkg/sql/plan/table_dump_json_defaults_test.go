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
	"encoding/json"
	"os"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// These complete TableDefs were authored and verified by the real parser and
// planner using production sources from 75ae9a79d626622e6ccec0bcb28412f5fcf84c7f,
// before the JSON fixes. The fixture includes each CREATE statement; it does
// not synthesize old expressions using the verifier's compatibility mode.
func TestTableDumpHistoricalJSONDefaults(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy_json_defaults_75ae9a79d6.json")
	require.NoError(t, err)
	var fixtures map[string]struct {
		SQL      string `json:"sql"`
		TableDef []byte `json:"table_def"`
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 11)
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			var source planpb.TableDef
			require.NoError(t, source.Unmarshal(fixture.TableDef))
			compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
			contextBefore := compiler.GetProcess().Ctx
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, fixture.SQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			fresh, err := BuildPlan(compiler, stmt, false)
			require.NoError(t, err)
			target := fresh.GetDdl().GetCreateTable().GetTableDef()
			for _, pair := range []struct {
				name             string
				target, supplied *planpb.TableDef
			}{
				{"old_catalog_dump", &source, &source},
				{"old_payload_load", target, &source},
				{"current_payload", target, target},
			} {
				t.Run(pair.name, func(t *testing.T) {
					sensitive, err := AnalyzeTableDumpBindings(compiler, pair.target, pair.supplied)
					require.NoError(t, err)
					require.True(t, sensitive, "MONTHNAME must retain the bound archive format")
				})
			}
			for _, corruption := range []string{"type", "value"} {
				t.Run("reject_"+corruption, func(t *testing.T) {
					bad := proto.Clone(&source).(*planpb.TableDef)
					if corruption == "type" {
						bad.Cols[0].Default.Expr.Typ.Id = int32(types.T_bool)
					} else {
						bad.Cols[0].Default.Expr = makePlan2Int64ConstExprWithType(99)
					}
					_, err := AnalyzeTableDumpBindings(compiler, target, bad)
					require.ErrorContains(t, err, "cannot verify bound default expression a")
				})
			}
			after, err := source.Marshal()
			require.NoError(t, err)
			require.Equal(t, fixture.TableDef, after, "verification must not rewrite archived expressions")
			require.Equal(t, contextBefore, compiler.GetProcess().Ctx)
			current, err := BuildPlan(compiler, stmt, false)
			require.NoError(t, err)
			require.True(t, proto.Equal(target, current.GetDdl().GetCreateTable().GetTableDef()), "legacy verification must not change ordinary DDL")
		})
	}
}

func TestTableDumpRejectsMixedJSONEra(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table mixed_json_era(a int default (ifnull(json_length('null'),0) + ifnull(json_length('null'),0)))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(compiler, stmt, false)
	require.NoError(t, err)
	target := built.GetDdl().GetCreateTable().GetTableDef()
	item := tableDumpExpressions(target)[0]
	parsed, ast, _, err := parseTableDumpExpression(t.Context(), item.origin, "")
	require.NoError(t, err)
	defer parsed.Free()
	oldExpr, err := bindTableDumpExpression(compiler, target, item, ast, 0, tableDumpBindingPreJSON, &tableDumpDefaultSession{})
	require.NoError(t, err)
	legacy := proto.Clone(target).(*planpb.TableDef)
	legacy.Cols[0].Default.Expr = oldExpr
	// The complete expression yields 0 before the fix and 2 after it. A value
	// of 1 would require mixing old/new JSON semantics inside the same tree.
	for _, complete := range []*planpb.TableDef{target, legacy} {
		_, err := AnalyzeTableDumpBindings(compiler, target, complete)
		require.NoError(t, err, "each complete historical/current era must remain valid")
	}
	bad := proto.Clone(target).(*planpb.TableDef)
	currentSum := bad.Cols[0].Default.Expr.GetF().Args[0].GetF()
	legacySum := oldExpr.GetF().Args[0].GetF()
	require.Equal(t, "+", currentSum.Func.ObjName)
	require.Equal(t, "+", legacySum.Func.ObjName)
	require.False(t, proto.Equal(currentSum.Args[0], legacySum.Args[0]))
	currentSum.Args[0] = DeepCopyExpr(legacySum.Args[0])
	_, err = AnalyzeTableDumpBindings(compiler, target, bad)
	require.ErrorContains(t, err, "cannot verify bound default expression a")
}
