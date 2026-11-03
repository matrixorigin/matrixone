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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// This file pins design revision 29's cell (xxi) (c) MOD storage-envelope
// witnesses (F-MOD-STORAGE-ENVELOPE, decision log 75 (iv)) for issue #30938.
// DecimalModReturnType (pkg/sql/plan/function/decimal_mod.go:22-54) derives a
// MOD result's precision as the maximum ORIGINAL operand width and its scale
// as the maximum original operand scale, so a MOD chain over a widened
// multiplication product is width-sensitive across the widening seam: the
// widened binary published (64,0) for TRUNCATE((38,10)x(38,10),0) % (39,0)
// and (76,0) for ((38,0)x(38,0)) % (39,0), where the parent's own inference
// authors (53,0) and (65,0) (live-confirmed against the parent engine,
// decision log 75 (i)). The storage-envelope mirror (design section 4.1's
// mod bullet) reconstructs the parent's operand domains rule-for-rule, so
// CTAS and view records persist the parent's tags. All expected tags below
// are derived from the shared typing owners (DecimalModReturnType over the
// truncate rule's integral-digits derivation and the integer digit domains),
// never from observed candidate behavior.

// registerModBoundaryTable registers one MOD fixture table in the mock
// catalog.
func registerModBoundaryTable(t *testing.T, mock *MockOptimizer, ddl string) {
	t.Helper()
	name := ddl[len("create table "):]
	name = name[:strings.IndexByte(name, '(')]
	src, err := buildTestCreateTableStmt(mock, ddl)
	require.NoError(t, err)
	mock.ctxt.tables[name] = src
	mock.ctxt.objects[name] = &ObjectRef{SchemaName: "tpch", ObjName: name}
}

// buildModBoundaryCTAS registers the MOD fixture tables and runs one
// statement through the public planner, returning the built plan whose DDL
// record carries the persisted storage tags.
func buildModBoundaryCTAS(t *testing.T, sql string) *pb.Plan {
	t.Helper()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	for _, ddl := range []string{
		"create table m30938s(a decimal(38,10), b decimal(38,10), d decimal(39,0))",
		"create table m30938w(a decimal(38,0), b decimal(38,0), d decimal(39,0))",
		"create table m30938i(c decimal(8,2), mi mediumint, i int, bt bit)",
	} {
		registerModBoundaryTable(t, mock, ddl)
	}
	built, err := runOneStmt(mock, t, sql)
	require.NoError(t, err)
	return built
}

// requireModBoundaryTyp pins one persisted DECIMAL plan type's family, width
// and scale.
func requireModBoundaryTyp(t *testing.T, typ pb.Type, id types.T, width, scale int32, msgAndArgs ...any) {
	t.Helper()
	message := append([]any{typ.String()}, msgAndArgs...)
	require.Equal(t, int32(id), typ.Id, message...)
	require.Equal(t, width, typ.Width, message...)
	require.Equal(t, scale, typ.Scale, message...)
}

// TestDecimalModStorageBoundaryWrapperIssue30938 is cell (xxi) (c-mod1): the
// below-65 wrapper chain TRUNCATE(a*b, 0) % d over DECIMAL(38,10) operands
// with d DECIMAL(39,0) persists the parent's own MOD-derived tag (53,0) —
// the product publishes (65,12) on the parent, the truncate rule's
// integral-digits derivation yields (53,0), and MOD's maximum-operand-width
// rule over (53,0) and (39,0) yields (53,0) — never the widened derivation's
// (64,0). CTAS and view records alike.
func TestDecimalModStorageBoundaryWrapperIssue30938(t *testing.T) {
	ctas := buildModBoundaryCTAS(t,
		"create table m30938o1 as select truncate(a*b, 0) % d as v from m30938s")
	requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal256, 53, 0,
		"(c-mod1): the wrapper MOD chain's CTAS record persists the parent's (53,0)")
	view := buildModBoundaryCTAS(t,
		"create view m30938v1 as select truncate(a*b, 0) % d as v from m30938s")
	requireModBoundaryTyp(t, view.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal256, 53, 0,
		"(c-mod1): the wrapper MOD chain's view record persists the parent's (53,0)")
}

// TestDecimalModStorageBoundaryGenuineWideIssue30938 is cell (xxi) (c-mod2):
// the genuine-wide control (a*b) % d over DECIMAL(38,0) operands with d
// DECIMAL(39,0) persists the parent's own MOD-derived tag (65,0) — the
// parent's product publishes (65,0) and MOD's maximum-operand-width rule
// over (65,0) and (39,0) yields (65,0) — never the widened derivation's
// (76,0). CTAS and view records alike.
func TestDecimalModStorageBoundaryGenuineWideIssue30938(t *testing.T) {
	ctas := buildModBoundaryCTAS(t,
		"create table m30938o2 as select (a*b) % d as v from m30938w")
	requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal256, 65, 0,
		"(c-mod2): the genuine-wide MOD control's CTAS record persists the parent's (65,0)")
	view := buildModBoundaryCTAS(t,
		"create view m30938v2 as select (a*b) % d as v from m30938w")
	requireModBoundaryTyp(t, view.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal256, 65, 0,
		"(c-mod2): the genuine-wide MOD control's view record persists the parent's (65,0)")
}

// TestDecimalModStorageBoundaryIntegerParityIssue30938 guards the mirror's
// verbatim integer digit recovery (design section 4.1's mod bullet): the
// storage boundary evaluates the envelope for every decimal column, so a
// non-widened MOD chain must keep the parent's own published tag exactly.
// A mediumint operand contributes 7 digits (the IsMediumInt override), so
// DECIMAL(8,2) % mediumint keeps the parent's (8,2); a full int contributes
// 10, so DECIMAL(8,2) % int keeps the parent's (10,2). The widths and scales
// are the shared typing owner's derivations over the original operands; the
// family is the pre-existing overload's selected Decimal128 carrier, which
// DecimalModReturnType keeps verbatim ("keep the selected physical type")
// on both engines.
func TestDecimalModStorageBoundaryIntegerParityIssue30938(t *testing.T) {
	medium := buildModBoundaryCTAS(t,
		"create table m30938p1 as select c % mi as v from m30938i")
	requireModBoundaryTyp(t, medium.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal128, 8, 2,
		"the mediumint override keeps the parent's (8,2) for a non-widened MOD chain")
	full := buildModBoundaryCTAS(t,
		"create table m30938p2 as select c % i as v from m30938i")
	requireModBoundaryTyp(t, full.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal128, 10, 2,
		"the integer digit recovery keeps the parent's (10,2) for a non-widened MOD chain")
}

// TestDecimalModStorageBoundaryBitPassThroughIssue30938 pins the owner's
// non-decimal pass-through at the boundary (design section 4.1's mod
// bullet): BIT operands keep the selected type under DecimalModReturnType,
// so the envelope mirror declines and the persisted record equals the
// statement's own published head — the parent's publication by construction.
// A statement-time CTAS plan carries no query (build_ddl.go nils the
// assembled SELECT for non-prepared statements), so the published head is
// read from the plain SELECT form of the same expression.
func TestDecimalModStorageBoundaryBitPassThroughIssue30938(t *testing.T) {
	ctas := buildModBoundaryCTAS(t,
		"create table m30938p3 as select bt % c as v from m30938i")
	persisted := ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerModBoundaryTable(t, mock,
		"create table m30938i(c decimal(8,2), mi mediumint, i int, bt bit)")
	plain, err := runOneStmt(mock, t, "select bt % c as v from m30938i")
	require.NoError(t, err)
	query := plain.GetQuery()
	require.NotNil(t, query)
	head := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
	require.Len(t, head, 1)
	require.Equal(t, head[0].GetTyp(), persisted,
		"the BIT pass-through keeps the authored published type at the storage boundary")
}

// TestDecimalModSetOperationBoundaryIssue30938 is the statement-time half of
// design revision 29's cell (xxi) (a) MOD witness (decision log 74 (v),
// live-confirmed against the parent engine): a set-operation branch set
// exporting a widened product and a widened-product MOD binds the parent's
// own per-branch domains through the resolver's original-operand recovery —
// the `%` branch's parent domain derives through DecimalModReturnType over
// the reconstructed operands (the product's (65,0) against the integer
// literal's digit domain, yielding (65,0)) — the union combines to (65,0),
// and the COALESCE merge retry converges the parent's (66,1) family common,
// which the CTAS record persists. The widened binary without the resolver's
// MOD operand recovery declines this statement (live receipt, decision log
// 75 (i)); the prepared-route twin lives in
// TestPreparedCTASDecimalBoundarySetOperationModArmRetry.
func TestDecimalModSetOperationBoundaryIssue30938(t *testing.T) {
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerModBoundaryTable(t, mock, "create table m30938u1(a decimal(38,0), b decimal(38,0))")
	registerModBoundaryTable(t, mock, "create table m30938u2(a decimal(38,0), b decimal(38,0))")
	ctas, err := runOneStmt(mock, t,
		"create table m30938o3 as select coalesce(u.v, cast(0.5 as decimal(20,1))) as w from "+
			"(select a*b as v from m30938u1 union all select (a*b) % 1 as v from m30938u2) u")
	require.NoError(t, err)
	requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal256, 66, 1,
		"cell (xxi) (a) statement time: the MOD branch set binds the parent's (66,1) merge common")
}

// TestDecimalSetOperationPlainTwinIssue30938 is the plain-product control
// beside TestDecimalModSetOperationBoundaryIssue30938: with both
// set-operation branches exporting a plain widened product, the same outer
// COALESCE merge converges the identical parent (66,1) family common. The
// statement-time producer channel is therefore the set-operation resolution
// itself (design decision log 56 (b): the channel computes over branch
// channels through the family's own merge), not anything specific to the
// MOD arm. The widened binary without the lineage-anchored producer walk
// declines this statement with the pre-existing merge-bind error family.
func TestDecimalSetOperationPlainTwinIssue30938(t *testing.T) {
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerModBoundaryTable(t, mock, "create table m30938u1(a decimal(38,0), b decimal(38,0))")
	registerModBoundaryTable(t, mock, "create table m30938u2(a decimal(38,0), b decimal(38,0))")
	ctas, err := runOneStmt(mock, t,
		"create table m30938o4 as select coalesce(u.v, cast(0.5 as decimal(20,1))) as w from "+
			"(select a*b as v from m30938u1 union all select a*b as v from m30938u2) u")
	require.NoError(t, err)
	requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal256, 66, 1,
		"the plain-product branch set binds the parent's (66,1) merge common at statement time")
}
