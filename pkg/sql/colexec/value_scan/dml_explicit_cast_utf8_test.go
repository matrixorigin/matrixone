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

package value_scan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

type dmlCastWarningSink struct {
	codes []uint16
}

func (s *dmlCastWarningSink) AppendWarningDiagnostic(code uint16, _ string) {
	s.codes = append(s.codes, code)
}
func (s *dmlCastWarningSink) AppendWarningCount(uint64)     {}
func (s *dmlCastWarningSink) GetWarningRetentionLimit() int { return 0 }

// #31921: an explicit binary->text CAST inside INSERT VALUES keeps its own
// conversion contract through the real planner and the value-scan value
// consumer: invalid UTF-8 yields NULL with exactly one Warning 1300, and the
// outer assignment cast consumes that result. The INSERT/REPLACE value binder
// must not pre-coerce the cast's binary source literal to the destination
// column type, which would erase the binary domain before the explicit cast
// can apply its conversion contract.
func TestInsertValuesExplicitBinaryCastKeepsConversionContract(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := plan2.NewMockCompilerContext(true, proc)

	build := func(sql string) *planpb.Plan {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		p, err := plan2.BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		return p
	}
	rowsetExpr := func(p *planpb.Plan, col int) *planpb.Expr {
		t.Helper()
		for _, n := range p.GetQuery().GetNodes() {
			if n.RowsetData != nil {
				return n.RowsetData.Cols[col].Data[0].Expr
			}
		}
		t.Fatalf("plan has no rowset data")
		return nil
	}
	eval := func(e *planpb.Expr) (bool, []uint16) {
		t.Helper()
		sink := &dmlCastWarningSink{}
		proc.WarningSink = sink
		exec, err := colexec.NewExpressionExecutor(proc, e)
		require.NoError(t, err)
		defer exec.Free()
		vec, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		require.NoError(t, err)
		// The evaluated vector is borrowed: the executor owns and frees it
		// (evalExpression.go: executor result lifetime), so only exec.Free.
		return vec.GetNulls().Contains(0), sink.codes
	}

	// Plan shape: the explicit cast sits directly on the binary literal; no
	// intermediate assignment-target cast may erase the source binary domain.
	p := build("insert into dept values (1,'x',CAST(x'FF' AS CHAR))")
	valueExpr := rowsetExpr(p, 2)
	outer := valueExpr.GetF()
	require.NotNil(t, outer)
	require.Equal(t, "cast_assign", outer.GetFunc().GetObjName(),
		"VALUES value keeps its assignment cast as the outer boundary")
	explicitFn := outer.GetArgs()[0].GetF()
	require.NotNil(t, explicitFn)
	require.Equal(t, "cast", explicitFn.GetFunc().GetObjName())
	fid, overload := function.DecodeOverloadID(explicitFn.GetFunc().GetObj())
	require.Equal(t, int32(function.CAST), fid)
	require.Equal(t, int32(1), overload, "user CAST keeps the explicit overload")
	srcLit := explicitFn.GetArgs()[0].GetLit()
	require.NotNil(t, srcLit, "explicit cast source binds as the raw binary literal")
	require.Equal(t, planpb.StringLiteralForm_STRING_LITERAL_HEX, srcLit.GetLiteralForm())

	// Executor value consumer: NULL with exactly one Warning 1300.
	isNull, codes := eval(valueExpr)
	require.True(t, isNull, "invalid UTF-8 explicit cast result must be NULL")
	require.Equal(t, []uint16{1300}, codes)

	// INSERT IGNORE binds cast_ignore over the same explicit cast; the explicit
	// conversion contract is unchanged.
	ignoreExpr := rowsetExpr(build("insert ignore into dept values (3,'z',CAST(x'FF' AS CHAR))"), 2)
	require.Equal(t, "cast_ignore", ignoreExpr.GetF().GetFunc().GetObjName())
	isNull, codes = eval(ignoreExpr)
	require.True(t, isNull)
	require.Equal(t, []uint16{1300}, codes)

	// Implicit control: a bare binary literal stays assignment-mode — raw bytes
	// pass the assignment cast without Warning 1300.
	implicitExpr := rowsetExpr(build("insert into dept values (2,'y',x'FF')"), 2)
	require.Equal(t, "cast_assign", implicitExpr.GetF().GetFunc().GetObjName())
	require.NotNil(t, implicitExpr.GetF().GetArgs()[0].GetLit(),
		"implicit assignment keeps the raw binary literal (no explicit boundary)")
	isNull, codes = eval(implicitExpr)
	require.False(t, isNull, "implicit assignment must not produce NULL")
	require.Empty(t, codes, "implicit assignment must not produce Warning 1300")
}

// Suppression scope control: the binary-domain source protection is limited
// to explicit casts into non-binary-charset text targets. A hex literal under
// a numeric-target explicit cast keeps the historical INSERT VALUES
// pre-coercion to the destination column type: CAST(X'3132' AS SIGNED)
// coerces through the varchar column and yields 12 (bytes interpreted as the
// text "12"), not the raw-byte integer 0x3132 = 12594. This mirrors
// dtype/vecblock.sql's hexkeep case.
func TestInsertValuesExplicitNumericCastKeepsLegacyLiteralCoercion(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := plan2.NewMockCompilerContext(true, proc)

	build := func(sql string) *planpb.Plan {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		p, err := plan2.BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		return p
	}
	rowsetExpr := func(p *planpb.Plan, col int) *planpb.Expr {
		t.Helper()
		for _, n := range p.GetQuery().GetNodes() {
			if n.RowsetData != nil {
				return n.RowsetData.Cols[col].Data[0].Expr
			}
		}
		t.Fatalf("plan has no rowset data")
		return nil
	}
	eval := func(e *planpb.Expr) (bool, string, []uint16) {
		t.Helper()
		sink := &dmlCastWarningSink{}
		proc.WarningSink = sink
		exec, err := colexec.NewExpressionExecutor(proc, e)
		require.NoError(t, err)
		defer exec.Free()
		vec, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		require.NoError(t, err)
		// The evaluated vector is borrowed: the executor owns and frees it.
		return vec.GetNulls().Contains(0), vec.GetStringAt(0), sink.codes
	}

	p := build("insert into dept values (4,'w',CAST(x'3132' AS SIGNED))")
	valueExpr := rowsetExpr(p, 2)
	outer := valueExpr.GetF()
	require.NotNil(t, outer)
	require.Equal(t, "cast_assign", outer.GetFunc().GetObjName(),
		"VALUES value keeps its assignment cast as the outer boundary")
	// Constant folding collapses the bound literal chain differently across
	// binder paths, so the decisive contract is the evaluated value: 12 is
	// only reachable when the hex literal was pre-coerced through the
	// destination varchar column before the explicit cast ran.
	isNull, value, codes := eval(valueExpr)
	require.False(t, isNull)
	require.Equal(t, "12", value,
		"CAST(X'3132' AS SIGNED) through the varchar column must stay the historical 12, not raw-byte 12594")
	require.Empty(t, codes, "numeric-target casts emit no Warning 1300")
}

// Binary-domain protection reaches hex/bit literals nested inside explicit
// text-cast source expressions: defaultValueBindType would otherwise rewrite
// the literal to the destination column type and erase the binary domain
// before the cast's conversion contract could apply. Non-literal source
// expressions without hex/bit leaves keep the historical binding.
func TestInsertValuesExplicitTextCastProtectsNestedBinaryLiterals(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := plan2.NewMockCompilerContext(true, proc)

	eval := func(sql string) (bool, string, []uint16) {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		p, err := plan2.BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		var valueExpr *planpb.Expr
		for _, n := range p.GetQuery().GetNodes() {
			if n.RowsetData != nil {
				valueExpr = n.RowsetData.Cols[2].Data[0].Expr
			}
		}
		require.NotNil(t, valueExpr, "plan has no rowset data")
		sink := &dmlCastWarningSink{}
		proc.WarningSink = sink
		exec, err := colexec.NewExpressionExecutor(proc, valueExpr)
		require.NoError(t, err)
		defer exec.Free()
		vec, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		require.NoError(t, err)
		// The evaluated vector is borrowed: the executor owns and frees it.
		return vec.GetNulls().Contains(0), vec.GetStringAt(0), sink.codes
	}

	// Nested inside IFNULL: the hex literal keeps its raw bytes, so the
	// explicit cast applies binarySourceToText and synthesizes NULL + 1300.
	isNull, _, codes := eval("insert into dept values (5,'n',CAST(IFNULL(x'FF','a') AS CHAR))")
	require.True(t, isNull, "nested hex literal must keep binary domain: invalid UTF-8 yields NULL")
	require.Contains(t, codes, uint16(1300))

	// Nested inside CONCAT with a text peer: same contract.
	isNull, _, codes = eval("insert into dept values (6,'n',CAST(CONCAT(x'FF','a') AS CHAR))")
	require.True(t, isNull, "nested hex literal inside CONCAT must keep binary domain")
	require.Contains(t, codes, uint16(1300))

	// Control without any hex/bit leaf: historical binding and value.
	isNull, value, codes := eval("insert into dept values (7,'n',CAST(IFNULL('a','b') AS CHAR))")
	require.False(t, isNull)
	require.Equal(t, "a", value)
	require.Empty(t, codes)

	// Control: nested hex under a NUMERIC-target cast keeps the legacy
	// pre-coercion (innermost explicit cast owns its source subtree).
	isNull, value, codes = eval("insert into dept values (8,'n',CAST(IFNULL(x'3132','0') AS SIGNED))")
	require.False(t, isNull)
	require.Equal(t, "12", value, "numeric-target nested cast keeps legacy pre-coercion of the hex literal")
	require.Empty(t, codes)
}
