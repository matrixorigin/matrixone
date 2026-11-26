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
	"testing"

	"github.com/gogo/protobuf/proto"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// tableDumpRadixCastPrepatchFixture preserves, byte-for-byte, the prepatch
// planner's bound TableDef for a table whose DEFAULT and generated column
// spell cast(HEX literal AS DECIMAL). It was authored by the actual base
// planner on pinned prepatch main d9a43b3156dd6ee54f14bdd4197071e522efd285
// and captured in the repository's canonical table-dump testdata wire format
// (a JSON map of protobuf wire bytes; captured file sha256
// 5f4bab3262b04377207a63552fca6074972d2b2c41ac69ad7fd63f9ce2af2d3c). Holding
// the exact preserved bytes in this narrowly scoped fixture constant keeps the
// changed source visible to the official full-input SCA selector.
const tableDumpRadixCastPrepatchFixture = `{"legacy_radix_cast":"EgxyYWRpeF9sZWdhY3kiIBIBYSABKg8IFiAgKP///////////wEwAToAaAGaAQFhIkwIARIBZCABKgYIICAEKAQ6NwoUCggIIBABIAQoBBIIigECCEHoAQESHShjYXN0KDB4NDEgYXMgZGVjaW1hbCg0LCA0KSkpGAGaAQFkInsIAhIBZyABKgYIICAEKAQ6AhgBmgEBZ6IBYQpACggIIBABIAQoBDo0Cg0ggYCAgNACQgRjYXN0EhUKCAg9EAEgAUABEglaAUHIAQHgAQMSDAoICCAQASAEKARaABIbY2FzdCgweDQxIGFzIGRlY2ltYWwoNCwgNCkpGAFaBiIBYSoBYcoB3AEK2QEKDAoHcmVsa2luZBIBcgrIAQoNcmVsX2NyZWF0ZXNxbBK2AWNyZWF0ZSB0YWJsZSBgcmFkaXhfbGVnYWN5YCAoYGFgIGludCBwcmltYXJ5IGtleSwgYGRgIGRlY2ltYWwoNCwgNCkgZGVmYXVsdCAoY2FzdCgweDQxIGFzIGRlY2ltYWwoNCwgNCkpKSwgYGdgIGRlY2ltYWwoNCwgNCkgZ2VuZXJhdGVkIGFsd2F5cyBhcyAoY2FzdCgweDQxIGFzIGRlY2ltYWwoNCwgNCkpKSBzdG9yZWQpuAID"}`

// TestTableDumpRadixCastPrepatchDefinitionVsRebind pins review27's
// compatibility contract for issue #31816: a genuine prepatch definition
// (historical coefficient-form bound tree) is valid schema. The frontend
// delegates exactly to AnalyzeTableDumpBindings for exporting a table with
// bound expressions (pkg/frontend/table_dump.go:1306, Analyze(def, def)) and
// for verifying a restored bound dump (:1744, Analyze(target, restoredDef)),
// so a valid old schema must pass both shapes with its catalog bytes
// untouched, a current-era authored definition with the same SQL spelling and
// schema must bind to the same historical tree under the persisted route
// (persistedFormatCompatibility keeps DEFAULT/ON UPDATE/generated/CHECK/
// functional-index/dump bindings historical), and a forged or mismatched
// manifest must still be rejected exactly as before.
func TestTableDumpRadixCastPrepatchDefinitionVsRebind(t *testing.T) {
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal([]byte(tableDumpRadixCastPrepatchFixture), &fixtures))
	wire, ok := fixtures["legacy_radix_cast"]
	require.True(t, ok)
	var source pb.TableDef
	require.NoError(t, source.Unmarshal(wire))
	before := proto.Clone(&source).(*pb.TableDef)
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))

	_, err := AnalyzeTableDumpBindings(compiler, &source, &source)
	require.NoError(t, err, "an original valid prepatch schema must pass the export analysis")
	require.True(t, proto.Equal(before, &source), "analysis must preserve catalog bytes")

	restored := DeepCopyTableDef(before, true)
	target := DeepCopyTableDef(before, true)
	_, err = AnalyzeTableDumpBindings(compiler, target, restored)
	require.NoError(t, err, "an original valid prepatch payload must pass restored-dump verification")

	def := tableDumpDDLCreate(t, compiler,
		"create table radix_legacy(a int primary key, d decimal(4,4) default (cast(x'41' as decimal(4,4))), g decimal(4,4) as (cast(x'41' as decimal(4,4))) stored)")
	_, err = AnalyzeTableDumpBindings(compiler, def, def)
	require.NoError(t, err, "the same valid definition authored now must be exportable too")
	require.True(t, sameTableDumpBoundExpression(before.Cols[1].Default.Expr, def.Cols[1].Default.Expr),
		"the current-era authored default must bind to the same historical tree as the prepatch payload")
	t.Logf("issue31816 witness: same-DDL rebind parity confirmed over %d cols", len(def.Cols))

	forged := DeepCopyTableDef(before, true)
	forged.Cols[1].Default.Expr = makePlan2Int64ConstExprWithType(71)
	_, err = AnalyzeTableDumpBindings(compiler, forged, forged)
	require.ErrorContains(t, err, "cannot verify bound default expression d",
		"a forged or mismatched manifest must remain rejected")
}
