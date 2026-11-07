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

package frontend

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// Exercise the bound-payload producer/consumer used by DUMP/LOAD against the
// pre-#31121 scalar-JSON era archives: conflated JSON/string equality casts,
// JSON literal-null extraction folded as SQL NULL, and COALESCE's common-text
// cast that unquoted the selected JSON branch. The archived payloads in
// legacy_json_scalar_defaults_eabd1009f4.json were authored and folded by the
// predecessor production sources eabd1009f443cd26064d51eb63d2b0b58af91c5e
// through their own parser and BuildPlan, not by this verifier. Predecessor
// folded defaults must dump, restore and re-verify byte-exactly; a forged
// digest must be rejected.
func TestTableDumpHistoricalJSONScalarSemanticsBoundPayload(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "sql", "plan", "testdata",
		"legacy_json_scalar_defaults_eabd1009f4.json"))
	require.NoError(t, err)
	var fixtures map[string]struct {
		SQL      string `json:"sql"`
		TableDef []byte `json:"table_def"`
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.NotEmpty(t, fixtures)
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			var source planpb.TableDef
			require.NoError(t, source.Unmarshal(fixture.TableDef))
			compiler := sqlplan.NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, fixture.SQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			fresh, err := sqlplan.BuildPlan(compiler, stmt, false)
			require.NoError(t, err)
			target := fresh.GetDdl().GetCreateTable().GetTableDef()
			before := proto.Clone(target)

			_, err = sqlplan.AnalyzeTableDumpBindings(compiler, &source, &source)
			require.NoError(t, err, "DUMP must accept historical catalog bindings")
			payload, digest, err := tableDumpBoundExpressions(&source)
			require.NoError(t, err)
			restored, _, err := tableDumpRestoredExpressions(target, payload, digest)
			require.NoError(t, err)
			_, err = sqlplan.AnalyzeTableDumpBindings(compiler, target, restored)
			require.NoError(t, err, "LOAD must accept the historical bound payload")
			require.True(t, proto.Equal(source.Cols[0].Default.Expr, restored.Cols[0].Default.Expr),
				"restored default must equal the archived fold byte-exactly")
			require.True(t, proto.Equal(before, target), "restoring archived defaults must not mutate the target")
			_, _, err = tableDumpRestoredExpressions(target, payload, "invalid-digest")
			require.Error(t, err, "legacy binding must not bypass payload validation")
		})
	}
}
