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
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// These TableDefs were authored and folded by the real parser and planner of
// the predecessor production sources eabd1009f443cd26064d51eb63d2b0b58af91c5e,
// the direct parent of this change set, before any #31121 fix: the payloads
// below were serialized by that source's own BuildPlan through a one-off
// probe kept outside the tree, mirroring the upstream
// testdata/legacy_json_defaults_75ae9a79d6.json authoring procedure. This
// suite never synthesized the archived expressions here with the verifier's
// compatibility mode, so the reconstruction below is verified against
// independently authored payloads.
//
// No release tag carries the persisted-expression machinery or the mixed
// JSON comparison kernels (neither b457977980, the origin of this dump
// binding path, nor a1220dec75, the origin of the JSON/BOOL kernel ancestor,
// is on any v* tag), so the predecessor main sources above are the earliest
// sources that can author or accept these persisted shape families.
//
// The cells cover the complete pre-#31121 scalar-JSON era: the conflated
// JSON/string equality cast (#31121 #1), JSON literal-null folding into the
// SQL NULL contract for numeric and boolean comparisons (#30770), the mixed
// JSON/BOOL ordering cast pair with its implicit CAST(JSON AS BOOL), the
// COALESCE common-text cast that unquoted the selected JSON branch (#30934),
// and compound defaults combining the string/boolean predicate with the
// changed JSON-null behavior. The json_pretty_control cell pins that the era
// never over-applies to an unchanged surface: the predecessor sources at
// eabd1009f4 already serialize DOUBLE values through the post-#30316
// serializer (#30782 fixes other paths), so its fold is identical in both
// eras. The json_string_bool_equal_control cell is the local review's
// counterexample: both eras fold its equality to false, but a
// reconstruction that parses legacy strings as boolean text folds it to
// true and cannot verify the archived payload.
func TestTableDumpHistoricalJSONScalarSemantics(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy_json_scalar_defaults_eabd1009f4.json")
	require.NoError(t, err)
	var fixtures map[string]struct {
		SQL      string `json:"sql"`
		TableDef []byte `json:"table_def"`
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 14)
	for name, fixture := range fixtures {
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
			// The era is load-bearing: rebinding the same default expression
			// under only the current binding must produce a different complete
			// tree, so a forged current-shaped payload cannot verify as a
			// predecessor archive.
			item := tableDumpExpressions(target)[0]
			parsed, ast, _, err := parseTableDumpExpression(t.Context(), item.origin, "")
			require.NoError(t, err)
			defer parsed.Free()
			currentExpr, err := bindTableDumpExpression(compiler, target, item, ast, 0, tableDumpBindingCurrent, &tableDumpDefaultSession{})
			require.NoError(t, err)
			if !strings.Contains(name, "control") {
				require.False(t, sameTableDumpBoundExpression(source.Cols[0].Default.Expr, currentExpr),
					"the current-era bind must diverge from the predecessor archive")
			} else {
				require.True(t, sameTableDumpBoundExpression(source.Cols[0].Default.Expr, currentExpr),
					"the control's eras fold identically; it pins that the era never over-applies")
			}
		})
	}
}

// Corrupted archives must stay rejected for every historical cell, including
// the compound defaults: neither a rewritten folded value nor a retyped
// expression may verify, so the era cannot become a forgery tunnel.
func TestTableDumpRejectsCorruptedScalarJSONArchive(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy_json_scalar_defaults_eabd1009f4.json")
	require.NoError(t, err)
	var fixtures map[string]struct {
		SQL      string `json:"sql"`
		TableDef []byte `json:"table_def"`
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	for name, fixture := range fixtures {
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
			for _, corruption := range []string{"type", "value"} {
				t.Run("reject_"+corruption, func(t *testing.T) {
					bad := proto.Clone(&source).(*planpb.TableDef)
					if corruption == "type" {
						require.NotNil(t, bad.Cols[0].Default.Expr.Typ)
						original := types.T(bad.Cols[0].Default.Expr.Typ.Id)
						require.NotEqual(t, types.T_int64, original)
						bad.Cols[0].Default.Expr.Typ = planpb.Type{Id: int32(types.T_int64)}
					} else {
						require.NotNil(t, bad.Cols[0].Default.Expr)
						bad.Cols[0].Default.Expr = makePlan2Int64ConstExprWithType(99)
					}
					_, err := AnalyzeTableDumpBindings(compiler, target, bad)
					require.ErrorContains(t, err, "cannot verify bound default expression a")
				})
			}
		})
	}
}
