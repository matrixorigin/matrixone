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
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// Exercise the exact binding/payload boundaries used by handleDumpTable and
// handleLoadTable. A current CREATE cannot author these historical bindings.
func TestTableDumpHistoricalDecimalBoundPayload(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "sql", "plan", "testdata", "legacy_decimal_dump_63a964a82f.json"))
	require.NoError(t, err)
	var fixtures map[string]struct {
		SQL      string `json:"sql"`
		TableDef []byte `json:"table_def"`
		Payload  []byte `json:"bound_expressions"`
		Digest   string `json:"expression_hash"`
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 10)
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			var source pb.TableDef
			require.NoError(t, source.Unmarshal(fixture.TableDef))
			before := proto.Clone(&source)
			compiler := sqlplan.NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, fixture.SQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			fresh, err := sqlplan.BuildPlan(compiler, stmt, false)
			require.NoError(t, err)
			target := fresh.GetDdl().GetCreateTable().GetTableDef()
			targetBefore := proto.Clone(target)
			sourceHash, err := tableSchemaHash(&source)
			require.NoError(t, err)
			targetHash, err := tableSchemaHash(target)
			require.NoError(t, err)
			require.Equal(t, sourceHash, targetHash)
			sensitive, err := sqlplan.AnalyzeTableDumpBindings(compiler, &source, &source)
			require.NoError(t, err, "DUMP must accept the unchanged old catalog")
			require.True(t, sensitive, "base MONTHNAME default genuinely requires a bound archive")
			payload, digest, err := tableDumpBoundExpressions(&source)
			require.NoError(t, err)
			require.NotEmpty(t, fixture.Payload)
			require.Equal(t, fixture.Payload, payload, "current writer retains the actual base writer's wire bytes")
			require.Equal(t, fixture.Digest, digest)
			restored, _, err := tableDumpRestoredExpressions(target, fixture.Payload, fixture.Digest)
			require.NoError(t, err)
			_, err = sqlplan.AnalyzeTableDumpBindings(compiler, target, restored)
			require.NoError(t, err, "LOAD must verify an actual old-writer payload")
			recaptured, recapturedDigest, err := tableDumpBoundExpressions(restored)
			require.NoError(t, err)
			require.Equal(t, payload, recaptured, "preserve original payload, not a normalized substitute")
			require.Equal(t, digest, recapturedDigest)
			_, _, err = tableDumpRestoredExpressions(target, payload, "invalid-digest")
			require.ErrorContains(t, err, "checksum")

			// Recompute the digest after a forgery: checksum alone is not trust.
			forged := sqlplan.DeepCopyTableDef(&source, true)
			if name == "mixed" {
				forged.Checks[0].Check.GetF().Args[0] = sqlplan.DeepCopyExpr(target.Checks[0].Check.GetF().Args[0])
			} else if len(forged.Checks) > 0 {
				forged.Checks[0].Check = &pb.Expr{Typ: forged.Checks[0].Check.Typ, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Bval{Bval: true}}}}
			} else {
				forged.Cols[2].GeneratedCol.Expr = &pb.Expr{Typ: forged.Cols[2].Typ, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Bval{Bval: true}}}}
			}
			badPayload, badDigest, err := tableDumpBoundExpressions(forged)
			require.NoError(t, err)
			untrusted, _, err := tableDumpRestoredExpressions(target, badPayload, badDigest)
			require.NoError(t, err, "declarations and recomputed digest remain valid")
			_, err = sqlplan.AnalyzeTableDumpBindings(compiler, target, untrusted)
			require.ErrorContains(t, err, "cannot verify bound", "no per-node mixed-era or constant-substitution witness")
			require.True(t, proto.Equal(before, &source))
			require.True(t, proto.Equal(targetBefore, target))
		})
	}
}
