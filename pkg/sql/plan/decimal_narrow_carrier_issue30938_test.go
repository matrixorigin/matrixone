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
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// This file pins design section 9 I2's NONCONSTANT narrow-carrier and
// parent-side-typing column oracles for issue #30938 (revision 38,
// F-NARROW-CARRIER-ENVELOPE, decision log 84 (iii); revision 39,
// F-UNSEAMED-ENVELOPE-PROVENANCE, decision log 85; revision 40,
// F-OUTPUT-DIVERGENCE-PROVENANCE, decision log 86 (i); revision 41,
// F-VALIDATION-MATRIX-ALIGNMENT, decision log 87 (ii)). Constant products
// fold to literals and never exercise the envelope rule, so every witness
// multiplies two table columns. Each persisted tag is the refreshed
// parent's own authored schema on base be8eb40ad8:
//   - Declared-pair families: the parent's multiplication publication over
//     decimalMultiplyDomain operand domains. Declared DECIMAL(17/18,.)
//     columns are DECIMAL128-typed (build_util.go's declaration mapping),
//     so the legacy-D64 width-18-to-19 coefficient adjustment does not
//     apply to them and the parent authors (35,0)/(36,0) for the declared
//     (18,0) pairs (revision 39's corrected values - parent-parity pins
//     passing on every RED variant). The discriminating declared-pair
//     control is the coefficient-promotion family (38,0)x(1,0): the
//     checked-D128 width-38 operand counts 39, the adjusted nominal
//     39+1 > 38 promotes the pair to the decimal256 route, and the parent
//     authors the promoted (40,0) where revision 2's uniform
//     min(w1+w2, 65) envelope persisted (39,0) - RED variant B's control.
//   - Intermediate families: the parent's fixed-DECIMAL64(18,s) d64+d64
//     add/subtract shortcut (list_operator.go:297-299) publishes
//     DECIMAL64(18,0) for (a+b) over DECIMAL(10,0) columns, and the
//     parent's multiplication counts that legacy-D64 intermediate 19, so
//     (a+b)*a and (a+b)*(a+b) persist the parent's (29,0)/(38,0). The
//     revision-39 add mirror (general rule only, no shortcut)
//     reconstructed (21,0)/(22,0) - RED variant A's controls.
//   - Cast-barrier family: CAST(a*b AS DECIMAL(10,0)) + d over (38,0)
//     columns with d DECIMAL(10,0). The SQL-authored cast is a
//     fixed-domain barrier pinning parent-side := published; both operands
//     are DECIMAL64(10,0) on either engine, the d64+d64 shortcut fires
//     exactly as the parent's, and the tree persists the parent's (18,0).
//     The revision-39 mirror reconstructed (11,0) - RED variant A's
//     control.

// narrowCarrierFixtureDDLs registers one table per carrier family so the
// witnesses stay nonconstant column pairs.
var narrowCarrierFixtureDDLs = []string{
	"create table m30938n10(a decimal(10,0), b decimal(10,0))",
	"create table m30938n18(a decimal(18,0), b decimal(17,0))",
	"create table m30938n38(a decimal(38,0), b decimal(38,0), o decimal(1,0), d decimal(10,0))",
}

// narrowCarrierWitness is one column-pair oracle: the select expression over
// its fixture table and the parent's own authored tag.
type narrowCarrierWitness struct {
	name       string
	expr       string
	table      string
	id         types.T
	width      int32
	scale      int32
	redControl string
}

var narrowCarrierWitnesses = []narrowCarrierWitness{
	// (10,0)x(10,0): 10+10 <= 38, so the narrow checked-D128 route
	// publishes (20,0); revision 2's formula agrees. Parent-parity pin
	// passing on every RED variant.
	{"(10,0)x(10,0)", "a*b", "m30938n10", types.T_decimal128, 20, 0, "20,0 (parent parity)"},
	// (18,0)x(17,0): declared DECIMAL128 operands count their declared
	// widths, so the parent publishes min(18+17, 38) = (35,0); revision
	// 2's formula coincides. Parent-parity pin (revision 39 correction).
	{"(18,0)x(17,0)", "a*b", "m30938n18", types.T_decimal128, 35, 0, "35,0 (parent parity)"},
	// (18,0)x(18,0): the parent publishes min(18+18, 38) = (36,0);
	// revision 2 coincides. Parent-parity pin (revision 39 correction).
	{"(18,0)x(18,0)", "a*a", "m30938n18", types.T_decimal128, 36, 0, "36,0 (parent parity)"},
	// (38,0)x(1,0): the checked-D128 width-38 operand counts 39, the
	// adjusted nominal 39+1 > 38 promotes the pair to the decimal256
	// route, and the parent authors the promoted (40,0). Passes on RED
	// variant A (the pre-provenance candidate's multiplication branch
	// already combines the promoted operand widths); RED variant B (the
	// revision-2-envelope revert) persists (39,0) - the RED control.
	{"(38,0)x(1,0)", "a*o", "m30938n38", types.T_decimal256, 40, 0, "39,0 (RED variant B)"},
	// (38,0)x(38,0): adjusted domains 39+39 take the decimal256 route and
	// the parent authors min(78, 65) = (65,0). Parent-parity pin.
	{"(38,0)x(38,0)", "a*b", "m30938n38", types.T_decimal256, 65, 0, "65,0 (parent parity)"},
	// (a+b)*a over DECIMAL(10,0): the parent's d64+d64 shortcut publishes
	// DECIMAL64(18,0) for the sum and its multiplication counts the
	// legacy-D64 width-18 intermediate 19, authoring (29,0). The
	// revision-39 mirror's shortcut-less reconstruction persisted (21,0) -
	// RED variant A's control.
	{"(a+b)*a over (10,0)", "(a+b)*a", "m30938n10", types.T_decimal128, 29, 0, "21,0 (RED variant A)"},
	// (a+b)*(a+b) over DECIMAL(10,0): both shortcut intermediates count
	// 19, and the parent authors min(19+19, 38) = (38,0). The revision-39
	// mirror persisted (22,0) - RED variant A's control.
	{"(a+b)*(a+b) over (10,0)", "(a+b)*(a+b)", "m30938n10", types.T_decimal128, 38, 0, "22,0 (RED variant A)"},
	// CAST(a*b AS DECIMAL(10,0)) + d over (38,0) columns with d
	// DECIMAL(10,0): the SQL-authored cast is a fixed-domain barrier, both
	// addition operands are DECIMAL64(10,0) on either engine, the d64+d64
	// shortcut publishes (18,0) exactly as the parent's, and the widened
	// product below the barrier stays contained. The revision-39 mirror
	// persisted (11,0) - RED variant A's control.
	{"cast-barrier (18,0)", "cast(a*b as decimal(10,0)) + d", "m30938n38", types.T_decimal64, 18, 0, "11,0 (RED variant A)"},
}

// buildNarrowCarrierPlan registers the narrow-carrier fixtures and runs one
// statement through the public planner, returning the built plan whose DDL
// record carries the persisted storage tags.
func buildNarrowCarrierPlan(t *testing.T, sql string) *pb.Plan {
	t.Helper()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	for _, ddl := range narrowCarrierFixtureDDLs {
		registerModBoundaryTable(t, mock, ddl)
	}
	built, err := runOneStmt(mock, t, sql)
	require.NoError(t, err)
	return built
}

// TestDecimalNarrowCarrierCTASAndViewIssue30938 is the section 9 I2
// narrow-carrier/parent-side family's CTAS and view-authoring leg: every
// witness persists the refreshed parent's own authored tag at both storage
// boundaries.
func TestDecimalNarrowCarrierCTASAndViewIssue30938(t *testing.T) {
	for _, w := range narrowCarrierWitnesses {
		t.Run(w.name+" ctas", func(t *testing.T) {
			ctas := buildNarrowCarrierPlan(t,
				"create table m30938nc_t as select "+w.expr+" as v from "+w.table)
			requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
				w.id, w.width, w.scale,
				"CTAS persists the parent's tag (RED control "+w.redControl+")")
		})
		t.Run(w.name+" view", func(t *testing.T) {
			view := buildNarrowCarrierPlan(t,
				"create view m30938nc_v as select "+w.expr+" as v from "+w.table)
			requireModBoundaryTyp(t, view.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetTyp(),
				w.id, w.width, w.scale,
				"view authoring persists the parent's tag (RED control "+w.redControl+")")
		})
	}
}

// TestDecimalNarrowCarrierViewReplayIssue30938 is the family's view-replay
// leg: DescribeViewColumns and RegenerateViewDefinition re-derive each
// witness's tag from the persisted definition and must reproduce the
// parent's own authored tag byte-for-byte (design I4's base-identical
// re-derivation at the replay locus).
func TestDecimalNarrowCarrierViewReplayIssue30938(t *testing.T) {
	for _, w := range narrowCarrierWitnesses {
		t.Run(w.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			t.Cleanup(proc.Free)
			mock := NewMockOptimizer(false, proc)
			for _, ddl := range narrowCarrierFixtureDDLs {
				registerModBoundaryTable(t, mock, ddl)
			}
			definition := fmt.Sprintf(
				`{"Stmt":"create view m30938nc_r as select %s as v from %s","DefaultDatabase":"tpch"}`,
				w.expr, w.table)
			cols, err := DescribeViewColumns(&mock.ctxt, definition)
			require.NoError(t, err)
			require.Len(t, cols, 1)
			requireModBoundaryTyp(t, cols[0].Typ, w.id, w.width, w.scale,
				"view describe re-derives the parent's tag (RED control "+w.redControl+")")
			regenerated, err := RegenerateViewDefinition(&mock.ctxt, definition)
			require.NoError(t, err)
			require.Len(t, regenerated.TableDef.Cols, 1)
			requireModBoundaryTyp(t, regenerated.TableDef.Cols[0].Typ, w.id, w.width, w.scale,
				"view replay re-derives the parent's tag (RED control "+w.redControl+")")
		})
	}
}

// TestDecimalNarrowCarrierPreparedCTASIssue30938 is the family's prepared
// CTAS leg: the PREPARE snapshot authors the parent's tag, and the
// execute-route inferred-column refresh reproduces it - parameter
// specialization through the function-headed output expression does not
// respecialize declared CTAS output types.
func TestDecimalNarrowCarrierPreparedCTASIssue30938(t *testing.T) {
	for _, w := range narrowCarrierWitnesses {
		t.Run(w.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			t.Cleanup(proc.Free)
			mock := NewMockOptimizer(false, proc)
			for _, ddl := range narrowCarrierFixtureDDLs {
				registerModBoundaryTable(t, mock, ddl)
			}
			sql := "create table m30938nc_p as select " + w.expr + " as v from " + w.table
			prepared, err := runOneStmt(mock, t,
				"prepare m30938nc_prep from '"+sql+"'")
			require.NoError(t, err)
			original := prepared.GetDcl().GetPrepare().Plan
			require.NotNil(t, original.GetDdl().GetCreateTable())
			requireModBoundaryTyp(t,
				original.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
				w.id, w.width, w.scale,
				"the PREPARE snapshot persists the parent's tag (RED control "+w.redControl+")")

			// The parameter-free execute route fills nothing and refreshes
			// the CTAS inferred columns over the retained evidence; the
			// refreshed record must keep the snapshot's parent-derived tag.
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), original, nil)
			require.NoError(t, err)
			stmt, err := mysql.ParseOne(context.Background(), sql, 1)
			require.NoError(t, err)
			retained, _ := mock.ctxt.GetProcess().ClaimDecimalBoundaryEvidence()
			require.NoError(t, RefreshPreparedCTASInferredColumns(
				context.Background(), filled, original, stmt, retained,
				function.DefaultDivPrecisionIncrement))
			requireModBoundaryTyp(t,
				filled.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
				w.id, w.width, w.scale,
				"the execute-route refresh keeps the parent's tag (RED control "+w.redControl+")")
		})
	}
}
