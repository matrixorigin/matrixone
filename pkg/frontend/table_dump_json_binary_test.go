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
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// Exercise the bound-payload producer/consumer used by DUMP/LOAD against
// predecessor catalogs whose persisted defaults carry the forward JSON =
// binary-string equality leg (BINARY, VARBINARY, BLOB): the shape bound
// through the predecessor's comparison cast row and folded to a survived
// value there. Today the binding rejects that pair outright, so a LOAD
// target cannot be freshly authored; the target of such an archive is
// itself a restored catalog, mirrored here by the cloned archive. Predecessor
// folded defaults must dump, restore and re-verify byte-exactly once era
// selection classifies the archived expression, and a forged digest must
// still be rejected. The control cell (json_char_equal_control) pins that
// the ordinary still-bindable character leg keeps the same round trip
// unchanged.
func TestTableDumpPreJSONBinaryScalarSemanticsBoundPayload(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "sql", "plan", "testdata",
		"legacy_json_binary_defaults_eabd1009f4.json"))
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
			_, err = sqlplan.BuildPlan(compiler, stmt, false)
			if strings.Contains(name, "control") {
				require.NoError(t, err, "an unchanged surface still binds today")
			} else {
				require.Error(t, err,
					"the current binder rejects the mixed JSON/binary-string pair outright")
			}
			target := proto.Clone(&source).(*planpb.TableDef)
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
