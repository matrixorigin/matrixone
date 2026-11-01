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
// the predecessor production source eabd1009f443cd26064d51eb63d2b0b58af91c5e,
// the direct parent of this change set, through a one-off probe kept outside
// the tree (mirroring legacy_json_scalar_defaults_eabd1009f4.json). The
// predecessor bound the forward (JSON = binary-string) equality leg through
// the comparison cast row, so forward-equality payloads across BINARY,
// VARBINARY and BLOB exist in real catalogs, while reverse and ordering legs
// never bound in that era and cannot appear as persisted defaults.
//
// The current binder rejects every JSON/binary-string pair outright, so a
// DUMP verification of these archives fails binding before any era
// classifier can observe the shape. Verification must still classify the
// archived expression itself and select the pre-#31121 scalar-JSON era,
// which reproduces the predecessor forward cast row exactly. The
// json_char_equal_control cell verifies an unchanged, still-bindable
// surface, so the failure-path classification cannot silently start
// reclassifying ordinary defaults.
func TestTableDumpPreJSONBinaryScalarSemantics(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy_json_binary_defaults_eabd1009f4.json")
	require.NoError(t, err)
	var fixtures map[string]struct {
		SQL      string `json:"sql"`
		TableDef []byte `json:"table_def"`
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 7)
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			var source planpb.TableDef
			require.NoError(t, source.Unmarshal(fixture.TableDef))
			compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, fixture.SQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(compiler, stmt, false)
			if strings.Contains(name, "control") {
				require.NoError(t, err, "an unchanged surface still binds today")
			} else {
				require.Error(t, err,
					"the current binder rejects the mixed JSON/binary-string pair outright")
			}
			// DUMP verification of the predecessor catalog must still pass:
			// the archived expression selects the applicable era instead of
			// being rejected through a failed current-semantics bind.
			_, err = AnalyzeTableDumpBindings(compiler, &source, &source)
			require.NoError(t, err, "the %s archive must verify after era selection", name)
			after, err := source.Marshal()
			require.NoError(t, err)
			require.Equal(t, fixture.TableDef, after, "verification must not rewrite archived expressions")
		})
	}
}

// Corrupted archives must stay rejected on every binary leg: neither a
// rewritten folded value nor a retyped expression may verify, so the new
// failure-path era selection cannot become a forgery tunnel.
func TestTableDumpRejectsCorruptedBinaryScalarArchive(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy_json_binary_defaults_eabd1009f4.json")
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
			_, err = AnalyzeTableDumpBindings(compiler, &source, &source)
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
					_, err := AnalyzeTableDumpBindings(compiler, &source, bad)
					require.ErrorContains(t, err, "cannot verify bound default expression a")
				})
			}
		})
	}
}
