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
