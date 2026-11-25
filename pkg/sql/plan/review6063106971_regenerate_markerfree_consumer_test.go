// Copyright 2021 - 2026 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// The EXACT release-written v4.2.9 pg189 ViewDef bytes captured from the real
// release service (marker-free, never transformed). Regeneration and the
// metadata consumer must keep the released one-level boundary: the complete
// authored shape (the inner WITH, both window levels, the trailing LIMIT and
// the authored ORDER where present) and the actual pg189.tw_src dependency.
const (
	releasePg189VInnerWith = `{"Stmt":"create view pg189.v_innerwith as (with c as (select ts from pg189.tw_src) select count(*) as a from c interval(ts, 1, minute)) interval(ts, 2, minute) limit 5","DefaultDatabase":"","sql_mode":"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES","security_type":"DEFINER"}`
	releasePg189VInnerLim  = `{"Stmt":"create view pg189.v_innerlim as (select count(*) as a from pg189.tw_src interval(ts, 1, minute) order by a limit 3) interval(ts, 2, minute) order by a limit 5","DefaultDatabase":"","sql_mode":"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES","security_type":"DEFINER"}`
	releasePg189VPlain     = `{"Stmt":"create view pg189.v_plain as (select count(*) as a from pg189.tw_src interval(ts, 1, minute)) interval(ts, 2, minute) limit 5","DefaultDatabase":"","sql_mode":"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES","security_type":"DEFINER"}`
)

func seedReleasePg189TwSrc(t *testing.T, ctx CompilerContext) {
	mc := ctx.(*MockCompilerContext)
	tsType := types.T_timestamp.ToType()
	srcDef := &planpb.TableDef{
		Name:      "tw_src",
		DbName:    "pg189",
		TblId:     4201,
		LogicalId: 4201,
		Cols:      []*planpb.ColDef{{Name: "ts", Typ: makePlan2Type(&tsType)}},
	}
	objRef := &planpb.ObjectRef{DbName: "pg189", ObjName: "tw_src", Obj: 4201}
	qualified := mockQualifiedTableName("pg189", "tw_src")
	mc.objectsByQualifiedName[qualified] = objRef
	mc.tablesByQualifiedName[qualified] = srcDef
	mc.objects["tw_src"] = objRef
	mc.tables["tw_src"] = srcDef
}

func TestRegenerateViewDefinitionPreservesReleaseAuthoredMarkerFreeDefinitions(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	seedReleasePg189TwSrc(t, ctx)
	for name, vd := range map[string]string{
		"v_innerwith": releasePg189VInnerWith,
		"v_innerlim":  releasePg189VInnerLim,
		"v_plain":     releasePg189VPlain,
	} {
		regenerated, err := RegenerateViewDefinition(ctx, vd)
		require.NoErrorf(t, err, "case %s: the release-authored marker-free definition must regenerate without the legacy-context rejection", name)
		var inData ViewData
		require.NoErrorf(t, json.Unmarshal([]byte(vd), &inData), "case %s: the captured release ViewDef must decode", name)
		var outData ViewData
		require.NoErrorf(t, json.Unmarshal([]byte(regenerated.TableDef.GetViewSql().GetView()), &outData), "case %s: the regenerated view must decode as ViewData", name)
		// the complete canonical persisted statement equals the authentic release
		// input statement: direct equality over both full CREATE VIEW texts, so
		// any dropped clause, reshaped window, renamed qualifier or scope change
		// fails the oracle; the marker-free input must also stay marker-free.
		require.Equalf(t, inData.Stmt, outData.Stmt, "case %s: the regenerated Stmt must preserve the complete authentic statement", name)
		require.Nilf(t, outData.CTEScopeProtocol, "case %s: the marker-free input must stay marker-free", name)
		// the physical pg189.tw_src dependency must bind on the fixture's own
		// stable object identity, not any generic nonempty dependency list.
		var sawPg189TwSrc bool
		for _, dep := range regenerated.Dependencies {
			if dep.DatabaseName == "pg189" && dep.RelationName == "tw_src" {
				require.Equalf(t, uint64(4201), dep.RelationID, "case %s: the dependency RelationID must resolve from the seeded physical table identity", name)
				require.Equalf(t, uint64(4201), dep.LogicalID, "case %s: the dependency LogicalID must carry the seeded physical identity", name)
				sawPg189TwSrc = true
			}
		}
		require.Truef(t, sawPg189TwSrc, "case %s: the actual pg189.tw_src dependency must bind on the seeded physical identity", name)
		// the derived output column a is BIGINT, matching the v4.2.9 SHOW
		// COLUMNS receipts of these very release-authored views.
		// the exact visible schema per the v4.2.9 SHOW COLUMNS readbacks: these
		// release views expose EXACTLY one visible output column, named a, of
		// BIGINT. Hidden entries are qualified out first, so a hidden column
		// named a can never satisfy this oracle, and any second visible column
		// (whatever its name) is a hard failure.
		var visibleCt int
		var colA *planpb.ColDef
		for _, c := range regenerated.TableDef.Cols {
			if c.Hidden {
				continue
			}
			visibleCt++
			colA = c
		}
		require.Equalf(t, 1, visibleCt, "case %s: the regenerated view must expose EXACTLY one visible output column (per the v4.2.9 SHOW COLUMNS readback), got %d", name, visibleCt)
		require.NotNilf(t, colA, "case %s: the visible output column must be present", name)
		require.Falsef(t, colA.Hidden, "case %s: the visible output column must not be hidden", name)
		require.Truef(t, strings.EqualFold(colA.Name, "a"), "case %s: the visible output column must be exactly a, got %q", name, colA.Name)
		require.Equalf(t, int32(types.T_int64), colA.Typ.Id, "case %s: the output column a must be BIGINT per the v4.2.9 SHOW COLUMNS readback", name)
		// the persisted catalog record (the stored ViewData written separately by
		// view_regeneration.go's patchPersistedViewMetadata) must carry the same
		// dependency agreement as the returned slice, with the exact seeded
		// physical identity - a missing or wrong stored dependency fails.
		require.Equalf(t, len(regenerated.Dependencies), len(outData.Dependencies), "case %s: the stored Dependencies must agree with the returned slice", name)
		var storedPg189TwSrc bool
		for _, dep := range outData.Dependencies {
			if dep.DatabaseName == "pg189" && dep.RelationName == "tw_src" {
				require.Equalf(t, uint64(4201), dep.RelationID, "case %s: the stored dependency RelationID must also carry the seeded identity", name)
				require.Equalf(t, uint64(4201), dep.LogicalID, "case %s: the stored dependency LogicalID must also carry the seeded identity", name)
				storedPg189TwSrc = true
			}
		}
		require.Truef(t, storedPg189TwSrc, "case %s: the stored catalog record must also bind the actual pg189.tw_src dependency", name)
	}
}
