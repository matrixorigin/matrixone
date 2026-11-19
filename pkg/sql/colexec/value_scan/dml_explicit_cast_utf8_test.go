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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
		p, err := plan2.BuildPlan(ctx, stmt, false, nil)
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
		p, err := plan2.BuildPlan(ctx, stmt, false, nil)
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
		p, err := plan2.BuildPlan(ctx, stmt, false, nil)
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

// review5809254373: the binary-domain source protection must also cover a
// NULLABLE NUMERIC destination. Previously only hex/bit NumVal leaves skipped
// the INSERT/REPLACE destination pre-coercion (numValDefaultBindType); the
// _binary introducer forms (_binary X'FF' / _binary 'FF') went through
// returnBinaryIntroducerExpr, which appended the destination column cast to
// the nonempty default bind type and erased the binary domain before the
// explicit text cast could apply its invalid-UTF-8 -> NULL + Warning 1300
// contract. The charset-explicit form (CHAR CHARACTER SET utf8mb4) bound its
// CONVERT source outside the protected scope with the same effect. Numeric
// coercion is irreversible, so the explicit non-binary-text source subtree —
// including the charset path — must be protected at the leaf.
func TestInsertValuesExplicitBinaryCastProtectsNumericDestination(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := plan2.NewMockCompilerContext(true, proc)

	evalCol0 := func(sql string) (bool, uint32, []uint16) {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		p, err := plan2.BuildPlan(ctx, stmt, false, nil)
		require.NoError(t, err)
		var valueExpr *planpb.Expr
		for _, n := range p.GetQuery().GetNodes() {
			if n.RowsetData != nil {
				valueExpr = n.RowsetData.Cols[0].Data[0].Expr
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
		if vec.GetNulls().Contains(0) {
			return true, 0, sink.codes
		}
		return false, vector.MustFixedColNoTypeCheck[uint32](vec)[0], sink.codes
	}

	// _binary X'FF' under explicit CHAR cast into nullable numeric deptno:
	// the explicit cast must own the conversion -> NULL + exactly Warning 1300.
	isNull, _, codes := evalCol0("insert into dept values (CAST(_binary X'FF' AS CHAR),'n','l')")
	require.True(t, isNull, "binary-introducer hex under explicit text cast must yield NULL, not a coerced number")
	require.Equal(t, []uint16{1300}, codes)

	// _binary '<raw 0xFF>' string form: same contract.
	isNull, _, codes = evalCol0("insert into dept values (CAST(_binary '\xff' AS CHAR),'n','l')")
	require.True(t, isNull, "binary-introducer string under explicit text cast must yield NULL")
	require.Equal(t, []uint16{1300}, codes)

	// Charset-explicit form: the CONVERT source binding is equally protected.
	isNull, _, codes = evalCol0("insert into dept values (CAST(_binary X'FF' AS CHAR CHARACTER SET utf8mb4),'n','l')")
	require.True(t, isNull, "charset-explicit text cast must keep the NULL + 1300 contract")
	require.Equal(t, []uint16{1300}, codes)

	// REPLACE shares the INSERT value binder: same contract.
	isNull, _, codes = evalCol0("replace into dept values (CAST(_binary X'FF' AS CHAR),'n','l')")
	require.True(t, isNull, "REPLACE nullable numeric destination keeps NULL + 1300")
	require.Equal(t, []uint16{1300}, codes)

	// Warning-then-next-statement closure: after the actual Warning 1300 path
	// above, the next statement is planned and evaluated fresh (a new
	// BuildPlan plus a new expression executor) on the same process and still
	// produces the correct value. This is native in-process verification of
	// binder/executor state only — it is not a real client connection,
	// prepared-statement, or session reuse check.
	isNull, num, codes := evalCol0("insert into dept values (CAST(_binary X'3132' AS CHAR),'n','l')")
	require.False(t, isNull, "valid UTF-8 binary source converts and assigns")
	require.Equal(t, uint32(12), num, "valid bytes '12' reach the numeric destination as 12")
	require.Empty(t, codes)

	// Plain numeric literal control: historical destination coercion intact.
	isNull, num, codes = evalCol0("insert into dept values (12,'n','l')")
	require.False(t, isNull)
	require.Equal(t, uint32(12), num)
	require.Empty(t, codes)

	// NULL control: untyped NULL under the explicit cast stays NULL silently.
	isNull, _, codes = evalCol0("insert into dept values (CAST(NULL AS CHAR),'n','l')")
	require.True(t, isNull)
	require.Empty(t, codes)

	// Implicit control: a bare _binary introducer literal (no explicit cast)
	// keeps historical assignment binding and emits no Warning 1300.
	_, _, codes = evalCol0("insert into dept values (_binary X'3132','n','l')")
	require.Empty(t, codes, "implicit assignment must not produce Warning 1300")
}

// doubleDeptMockContext is a test-local CompilerContext wrapper: MockCompilerContext
// resolve() returns a fresh DeepCopyTableDef per call, so retyping the copy's
// deptno column to a nullable SQL DOUBLE (types.T_float64, 8 bytes — not
// T_float8/FP8 like vecblock_t.g) rewrites exactly what the planner sees for
// this test and nothing else. The global mock schema, other tests and every
// other column/primary-key/binder behavior stay untouched.
type doubleDeptMockContext struct {
	*plan2.MockCompilerContext
}

func (c *doubleDeptMockContext) retypeDeptno(tbl *planpb.TableDef) {
	if tbl == nil || len(tbl.Cols) == 0 || tbl.Cols[0].Name != "deptno" {
		return
	}
	doubleTyp := types.T_float64.ToType()
	tbl.Cols[0].Typ = planpb.Type{Id: int32(doubleTyp.Oid), Width: doubleTyp.Width, Scale: doubleTyp.Scale}
}

// The wrapper can change the descriptor, so neither resolver route publishes
// an underlying borrowed relation as evidence for that descriptor.
func (c *doubleDeptMockContext) Resolve(dbName string, tableName string, snapshot *plan2.Snapshot, outputs *plan2.TableResolution) (*planpb.ObjectRef, *planpb.TableDef, error) {
	outputs.Reset()
	obj, tbl, err := c.MockCompilerContext.Resolve(dbName, tableName, snapshot, nil)
	if err == nil && strings.EqualFold(tableName, "dept") {
		c.retypeDeptno(tbl)
	}
	return obj, tbl, err
}

func (c *doubleDeptMockContext) ResolveWithoutTemporaryAlias(dbName string, tableName string, snapshot *plan2.Snapshot, outputs *plan2.TableResolution) (*planpb.ObjectRef, *planpb.TableDef, error) {
	outputs.Reset()
	obj, tbl, err := c.MockCompilerContext.ResolveWithoutTemporaryAlias(dbName, tableName, snapshot, nil)
	if err == nil && strings.EqualFold(tableName, "dept") {
		c.retypeDeptno(tbl)
	}
	return obj, tbl, err
}

// review5810535982: binary-domain source protection must pass through
// byte-preserving nested casts. CAST(CAST(X'FF' AS BINARY) AS CHAR) into a
// nullable DOUBLE destination used to lose protection at the inner BINARY
// cast: the hex leaf was pre-coerced to the DOUBLE destination (255) before
// the outer text conversion, storing 255 without Warning 1300 instead of
// NULL. Only casts that keep the value in the binary domain (binary-charset
// char/varchar/binary/varbinary targets) are transparent to the guard;
// numeric or other substantive conversion targets keep their own boundary.
//
// Fixture contract (corrected after the 102 fixture used vecblock_t.g, which
// is T_float8 = FP8 e4m3, not DOUBLE): the destination must be a real
// nullable SQL DOUBLE, i.e. types.T_float64. The destination metadata, the
// final VALUES assignment expression type and the evaluated vector type are
// all asserted to be T_float64 before any float64 value is read.
func TestInsertValuesNestedBinaryPreservingCastKeepsConversionContract(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := &doubleDeptMockContext{MockCompilerContext: plan2.NewMockCompilerContext(true, proc)}

	// Verify the planner-visible destination metadata through the wrapper
	// before any statement is built: real T_float64, still nullable.
	_, checked, err := ctx.Resolve("", "dept", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "deptno", checked.Cols[0].Name)
	require.Equal(t, int32(types.T_float64), checked.Cols[0].Typ.Id,
		"destination must be real SQL DOUBLE (T_float64), not T_float8/FP8")
	require.False(t, checked.Cols[0].Typ.NotNullable, "destination stays nullable")

	evalG := func(sql string) (bool, float64, []uint16) {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		p, err := plan2.BuildPlan(ctx, stmt, false, nil)
		require.NoError(t, err)
		var valueExpr *planpb.Expr
		for _, n := range p.GetQuery().GetNodes() {
			if n.RowsetData != nil {
				valueExpr = n.RowsetData.Cols[0].Data[0].Expr
			}
		}
		require.NotNil(t, valueExpr, "plan has no rowset data")
		// The final destination assignment expression must be typed DOUBLE:
		// the value is read as float64 only when the plan expression and the
		// evaluated vector agree on T_float64.
		require.Equal(t, int32(types.T_float64), valueExpr.GetTyp().Id,
			"VALUES destination assignment expression must be typed DOUBLE")
		sink := &dmlCastWarningSink{}
		proc.WarningSink = sink
		exec, err := colexec.NewExpressionExecutor(proc, valueExpr)
		require.NoError(t, err)
		defer exec.Free()
		vec, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		require.NoError(t, err)
		// The evaluated vector is borrowed: the executor owns and frees it.
		require.Equal(t, int32(types.T_float64), int32(vec.GetType().Oid),
			"evaluated vector must be DOUBLE before any float64 read")
		if vec.GetNulls().Contains(0) {
			return true, 0, sink.codes
		}
		return false, vector.MustFixedColNoTypeCheck[float64](vec)[0], sink.codes
	}

	// The review case: nested BINARY cast is byte-preserving, so the outer
	// text cast still owns the conversion -> NULL + exactly one Warning 1300.
	isNull, _, codes := evalG("insert into dept values (CAST(CAST(x'FF' AS BINARY) AS CHAR),'n','l')")
	require.True(t, isNull, "nested binary-preserving cast must keep NULL, not a numerically coerced 255")
	require.Equal(t, []uint16{1300}, codes)

	// REPLACE shares the INSERT value binder: same contract.
	isNull, _, codes = evalG("replace into dept values (CAST(CAST(x'FF' AS BINARY) AS CHAR),'n','l')")
	require.True(t, isNull, "REPLACE nullable DOUBLE destination keeps NULL + 1300")
	require.Equal(t, []uint16{1300}, codes)

	// Charset-explicit outer form: the CONVERT source binding is equally
	// protected through the nested BINARY cast.
	isNull, _, codes = evalG("insert into dept values (CAST(CAST(x'FF' AS BINARY) AS CHAR CHARACTER SET utf8mb4),'n','l')")
	require.True(t, isNull, "charset-explicit text cast keeps NULL + 1300 through nested BINARY")
	require.Equal(t, []uint16{1300}, codes)

	// Byte length boundary: BINARY(2) pads with a zero byte but stays in the
	// binary domain; the outer text conversion still applies (FF 00 is
	// invalid UTF-8) -> NULL + 1300.
	isNull, _, codes = evalG("insert into dept values (CAST(CAST(x'FF' AS BINARY(2)) AS CHAR),'n','l')")
	require.True(t, isNull, "binary-preserving cast with padding keeps NULL + 1300")
	require.Equal(t, []uint16{1300}, codes)

	// Inner charset-explicit binary cast (CHAR CHARACTER SET binary): this is
	// the other binary-domain branch of the guard helper and must pass an
	// enclosing text cast's protection through unchanged.
	isNull, _, codes = evalG("insert into dept values (CAST(CAST(x'FF' AS CHAR CHARACTER SET binary) AS CHAR),'n','l')")
	require.True(t, isNull, "inner binary-charset CHAR cast is byte-preserving: NULL + 1300")
	require.Equal(t, []uint16{1300}, codes)

	// Nearest legal control: valid-UTF8 bytes through the nested BINARY cast
	// convert to text '12' and assign to DOUBLE as exactly 12 with no
	// warning.
	isNull, num, codes := evalG("insert into dept values (CAST(CAST(x'3132' AS BINARY) AS CHAR),'n','l')")
	require.False(t, isNull, "valid bytes '12' through nested binary-preserving cast must not yield NULL")
	require.Equal(t, float64(12), num, "valid bytes '12' reach the DOUBLE destination as exactly 12")
	require.Empty(t, codes)

	// Numeric barrier control: the guard must NOT pass through a substantive
	// numeric conversion. The inner SIGNED cast owns its source subtree, so
	// the same invalid-UTF8 hex literal keeps the historical destination
	// pre-coercion (FF interpreted numerically as 255, no NULL, no Warning
	// 1300) — the decisive contrast with the byte-preserving cases above.
	isNull, num, codes = evalG("insert into dept values (CAST(CAST(x'FF' AS SIGNED) AS CHAR),'n','l')")
	require.False(t, isNull, "numeric-target inner cast keeps its own conversion boundary (no NULL)")
	require.Equal(t, float64(255), num, "inner numeric cast keeps legacy numeric interpretation of FF as exactly 255")
	require.Empty(t, codes, "numeric barrier emits no Warning 1300")
}
