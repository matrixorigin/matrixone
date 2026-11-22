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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// The TableDefs were produced by the real parser and planner using production
// sources from 98796a5456, before unary NOT accepted native temporal operands.
// Non-temporal unary NOT defaults must keep that bound form so tables created
// before the upgrade can still be dumped and their archives loaded.
func TestTableDumpHistoricalUnaryTildeDefaults(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy_unary_tilde_defaults_98796a5456.json")
	require.NoError(t, err)
	var fixtures map[string]struct {
		SQL      string `json:"sql"`
		TableDef []byte `json:"table_def"`
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 6)
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			var source planpb.TableDef
			require.NoError(t, source.Unmarshal(fixture.TableDef))
			compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
			// DUMP of a table created before the upgrade re-verifies its catalog.
			_, err := AnalyzeTableDumpBindings(compiler, &source, &source)
			require.NoError(t, err)
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, fixture.SQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			fresh, err := BuildPlan(compiler, stmt, false, nil)
			require.NoError(t, err)
			target := fresh.GetDdl().GetCreateTable().GetTableDef()
			require.True(t, proto.Equal(source.Cols[0].Default, target.Cols[0].Default),
				"the current planner must bind the historical default identically")
			for _, pair := range []struct {
				name             string
				target, supplied *planpb.TableDef
			}{
				{"old_payload_load", target, &source},
				{"current_payload", target, target},
			} {
				t.Run(pair.name, func(t *testing.T) {
					_, err := AnalyzeTableDumpBindings(compiler, pair.target, pair.supplied)
					require.NoError(t, err)
				})
			}
		})
	}
}
