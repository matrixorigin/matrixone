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
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// These TableDefs were authored and bound by the parser and planner of the
// predecessor production sources c7d7536bdac944e7c148f699fcf8ad68914fb9d2
// (the PR base, the release lineage that persisted the archived shapes) through
// a one-off probe kept outside the tree, mirroring the
// testdata/legacy_json_scalar_defaults_eabd1009f4.json authoring procedure.
// This suite never synthesized the archived expressions with the verifier's
// own compatibility binding, so the reconstruction is verified against
// independently authored predecessors.
//
// The historical-scalar-JSON reconstruction used to stop at DEFAULT owners, so
// an upgraded table whose generated column (g BOOL GENERATED ALWAYS AS
// (j = 'x') STORED), CHECK constraint (j = 'x', or the mixed JSON/BOOL
// BETWEEN) or ON UPDATE expression was bound by the predecessor through the
// VARCHAR/BOOL cast folds could no longer be DUMP-verified: the current
// binding rebinds those predicates without the casts, no era candidate ever
// composed, and the dump failed with "cannot verify bound ...". The cells here
// pin that every expression owner that persists a bound tree gets the same era
// reconstruction while exact-tree verification is retained. The control cells
// pin the opposite edge: owner expressions outside the JSON era never trigger
// it, so their current and archived binds match directly.
func TestTableDumpHistoricalJSONScalarOwnerSemantics(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy_json_scalar_owner_fixtures_c7d7536bda.json")
	require.NoError(t, err)
	var fixtures map[string]struct {
		SQL      string `json:"sql"`
		TableDef []byte `json:"table_def"`
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	for name, fixture := range fixtures {
		if len(fixture.TableDef) == 0 {
			t.Fatalf("fixture %s carries no table_def payload", name)
		}
		t.Run(name, func(t *testing.T) {
			var source planpb.TableDef
			require.NoError(t, source.Unmarshal(fixture.TableDef))
			compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, fixture.SQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			fresh, err := BuildPlan(compiler, stmt, false)
			require.NoError(t, err)
			target := fresh.GetDdl().GetCreateTable().GetTableDef()
			// Each fixture persists exactly one non-DEFAULT expression owner.
			var owners []tableDumpExpression
			for _, item := range tableDumpExpressions(&source) {
				if item.kind != "default" {
					owners = append(owners, item)
				}
			}
			require.Len(t, owners, 1, "fixture %s must persist exactly one owner expression", name)
			archived := owners[0]
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
					_ = sensitive
				})
			}
			after, err := source.Marshal()
			require.NoError(t, err)
			require.Equal(t, fixture.TableDef, after, "verification must not rewrite archived expressions")
			// The era is load-bearing exactly for the JSON-scalar owner cells:
			// rebinding their persisted predicate under only the current
			// binding must produce a different complete tree, so a
			// forged current-shaped payload cannot verify as a
			// predecessor archive. The control owner cells fold and bind
			// identically in both eras and pin that the era candidate
			// never over-applies.
			stmt2, ast, _, err := parseTableDumpExpression(t.Context(), archived.origin, "")
			require.NoError(t, err)
			defer stmt2.Free()
			currentExpr, err := bindTableDumpExpression(compiler, target, archived, ast, 0, tableDumpBindingCurrent, &tableDumpDefaultSession{})
			require.NoError(t, err)
			if strings.Contains(name, "control") {
				require.True(t, sameTableDumpBoundExpression(archived.expr, currentExpr),
					"the control's eras bind identically; it pins that the era never over-applies")
			} else {
				require.False(t, sameTableDumpBoundExpression(archived.expr, currentExpr),
					"the current-era bind must diverge from the predecessor archive")
			}
		})
	}
}

// Corrupted owner archives must stay rejected: neither a retyped tree nor a
// replaced folded value may verify for any owner kind, so the widened era
// reconstruction cannot become a forgery tunnel.
func TestTableDumpRejectsCorruptedScalarJSONOwnerArchive(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy_json_scalar_owner_fixtures_c7d7536bda.json")
	require.NoError(t, err)
	var fixtures map[string]struct {
		SQL      string `json:"sql"`
		TableDef []byte `json:"table_def"`
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	for name, fixture := range fixtures {
		if strings.Contains(name, "control") {
			continue
		}
		t.Run(name, func(t *testing.T) {
			var source planpb.TableDef
			require.NoError(t, source.Unmarshal(fixture.TableDef))
			compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, fixture.SQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			fresh, err := BuildPlan(compiler, stmt, false)
			require.NoError(t, err)
			target := fresh.GetDdl().GetCreateTable().GetTableDef()
			// The predecessor-authored archive verifies before corruption.
			_, err = AnalyzeTableDumpBindings(compiler, target, &source)
			require.NoError(t, err, "the %s archive must verify before corruption", name)
			var owners []tableDumpExpression
			for _, item := range tableDumpExpressions(&source) {
				if item.kind != "default" {
					owners = append(owners, item)
				}
			}
			require.Len(t, owners, 1)
			archived := owners[0]
			for _, corruption := range []string{"type", "value"} {
				t.Run("reject_"+corruption, func(t *testing.T) {
					bad := proto.Clone(&source).(*planpb.TableDef)
					var owners []tableDumpExpression
					for _, item := range tableDumpExpressions(bad) {
						if item.kind != "default" {
							owners = append(owners, item)
						}
					}
					require.Len(t, owners, 1)
					corrupted := owners[0]
					if corruption == "type" {
						require.NotNil(t, corrupted.expr.Typ)
						require.NotEqual(t, types.T_int64, types.T(corrupted.expr.Typ.Id))
						corrupted.expr.Typ = planpb.Type{Id: int32(types.T_int64)}
					} else {
						require.NotNil(t, corrupted.expr)
						*corrupted.expr = *makePlan2Int64ConstExprWithType(99)
					}
					_, err := AnalyzeTableDumpBindings(compiler, target, bad)
					require.ErrorContains(t, err, "table dump cannot verify bound "+archived.kind+" expression "+archived.name)
				})
			}
		})
	}
}
