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
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Reuse complete protobufs authored by the actual base planner, not trees
// reconstructed by the historical binding implementation under test.
func TestTableDumpPersistedDecimalBindings(t *testing.T) {
	for _, file := range []string{
		"legacy_decimal_catalog_63a964a82f.json",
		"legacy_decimal_between_63a964a82f.json",
		"legacy_decimal_in_range_63a964a82f.json",
	} {
		t.Run(file, func(t *testing.T) {
			data, err := os.ReadFile("testdata/" + file)
			require.NoError(t, err)
			var fixtures map[string][]byte
			require.NoError(t, json.Unmarshal(data, &fixtures))
			require.NotEmpty(t, fixtures)
			for name, wire := range fixtures {
				t.Run(name, func(t *testing.T) {
					var source pb.TableDef
					require.NoError(t, source.Unmarshal(wire))
					before := proto.Clone(&source)
					compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
					_, err := AnalyzeTableDumpBindings(compiler, &source, &source)
					require.NoError(t, err, "DUMP must verify the original historical tree")
					target := DeepCopyTableDef(&source, true)
					require.NoError(t, MigrateLegacyCatalogTableDef(t.Context(), compiler.GetProcess(), target))
					_, err = AnalyzeTableDumpBindings(compiler, target, &source)
					require.NoError(t, err, "LOAD must verify an old payload against current execution bindings")
					if name == "signed_compare" {
						for _, width := range []int32{17, 19} {
							forged := DeepCopyTableDef(&source, true)
							forged.Checks[0].Check.GetF().Args[0].Typ.Width = width
							_, err = AnalyzeTableDumpBindings(compiler, target, forged)
							require.ErrorContains(t, err, "cannot verify bound check", "normalizing an arbitrary width must not establish trust")
						}
					}
					require.True(t, proto.Equal(before, &source), "verification must preserve catalog bytes")
				})
			}
		})
	}
}
