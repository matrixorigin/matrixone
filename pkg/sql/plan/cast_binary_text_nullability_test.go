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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// #31921 consumer follow-up: the explicit binary->text CAST executor
// (castModeExplicit, overload 1) synthesizes NULL + Warning 1300 when the
// source bytes are not valid UTF-8 (func_cast.go binarySourceToText guard),
// even for a NOT NULL binary source. The shared nullability inference owner
// (function.deduceNotNullable) must therefore never annotate such a cast
// NOT NULL. Real consumers read that annotation: view/derived schema
// Default.NullAbility (build_ddl.go) and the wire NOT_NULL_FLAG
// (mysql_protocol.go), and query_builder re-infers through the same owner.

func buildPlanForNullability(t *testing.T, ctx *MockCompilerContext, sql string) *plan.Plan {
	t.Helper()
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	p, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	return p
}

func findProjectionFn(t *testing.T, p *plan.Plan, objName string) *plan.Expr {
	t.Helper()
	q := p.GetQuery()
	require.NotNil(t, q, "expected a query plan")
	for _, node := range q.Nodes {
		for _, e := range node.ProjectList {
			if fn := e.GetF(); fn != nil && fn.Func != nil && fn.Func.GetObjName() == objName {
				return e
			}
		}
	}
	t.Fatalf("no %q function found in any projection", objName)
	return nil
}

func TestExplicitBinaryToTextCastNullabilityInference(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))

	// Primary: real BuildPlan over a NOT NULL binary column
	// (mo_tables.partition_info is T_blob NOT NULL in the mock catalog).
	castExpr := findProjectionFn(t,
		buildPlanForNullability(t, ctx, "select cast(partition_info as char) c from mo_tables"), "cast")
	fn := castExpr.GetF()
	fid, oid := function.DecodeOverloadID(fn.Func.Obj)
	require.Equal(t, int32(function.CAST), fid, "bound function must be CAST")
	require.Equal(t, int32(1), oid, "user explicit CAST binds the explicit (castModeExplicit) overload 1")
	src, dst := fn.Args[0].Typ, fn.Args[1].Typ
	t.Logf("bound cast: fid=%d oid=%d srcId=%d srcCharset=%d srcNotNullable=%v dstId=%d dstCharset=%d resultNotNullable=%v",
		fid, oid, src.Id, src.Charset, src.NotNullable, dst.Id, dst.Charset, castExpr.Typ.NotNullable)
	require.True(t, types.T(src.Id) == types.T_blob || types.T(src.Id) == types.T_varbinary || types.T(src.Id) == types.T_binary,
		"source must be a binary-domain string type, got id=%d", src.Id)
	require.True(t, src.NotNullable, "probe source column must be NOT NULL so the result annotation cannot be inherited")
	dstT := types.T(dst.Id)
	require.True(t, dstT == types.T_char || dstT == types.T_varchar || dstT == types.T_text,
		"CAST ... AS CHAR target must be the char/varchar/text family, got id=%d", dst.Id)
	require.NotEqual(t, uint32(types.CharsetBinary), dst.Charset, "text target must carry a non-binary charset")
	require.False(t, castExpr.Typ.NotNullable,
		"explicit binary->text CAST synthesizes NULL on invalid UTF-8 even from NOT NULL input: plan must not advertise NOT NULL")

	// Same inference through a NOT NULL binary literal (x'FF' binds as a
	// binary const, NotNullable=true; not masked by UNHEX's nullable input).
	litCast := findProjectionFn(t,
		buildPlanForNullability(t, ctx, "select cast(x'FF' as char) c"), "cast")
	lfn := litCast.GetF()
	lfid, loid := function.DecodeOverloadID(lfn.Func.Obj)
	require.Equal(t, int32(function.CAST), lfid)
	require.Equal(t, int32(1), loid)
	require.True(t, lfn.Args[0].Typ.NotNullable, "hex literal source is a NOT NULL const")
	t.Logf("literal cast: srcId=%d srcNotNullable=%v resultNotNullable=%v",
		lfn.Args[0].Typ.Id, lfn.Args[0].Typ.NotNullable, litCast.Typ.NotNullable)
	require.False(t, litCast.Typ.NotNullable)

	// Real consumer: CREATE VIEW derived schema Default.NullAbility consumes
	// expr.Typ.NotNullable directly (build_ddl.go).
	vp := buildPlanForNullability(t, ctx,
		"create view v_cast_null_probe as select cast(partition_info as char) c from mo_tables")
	viewDef := vp.GetDdl().GetCreateView().GetTableDef()
	require.NotNil(t, viewDef, "expected a create-view plan")
	var probeCol *plan.ColDef
	for _, c := range viewDef.Cols {
		if c.Name == "c" {
			probeCol = c
		}
	}
	require.NotNil(t, probeCol, "view column c must exist")
	require.NotNil(t, probeCol.Default)
	require.True(t, probeCol.Default.NullAbility,
		"view schema must mark the explicit binary->text CAST column nullable (Default.NullAbility)")

	// Nested expression: nullability must propagate through a strict wrapper
	// (UPPER) that re-infers from its argument via the same owner.
	upperExpr := findProjectionFn(t,
		buildPlanForNullability(t, ctx, "select upper(cast(partition_info as char)) c from mo_tables"), "upper")
	require.False(t, upperExpr.Typ.NotNullable,
		"UPPER over a NULL-synthesizing cast must be inferred nullable")

	// Corrected expectation (root tick123 runtime counterexample,
	// TestExplicitCastRuntimeBinaryNullabilityEval): a statically UTF8 NOT NULL
	// text source can still carry legitimate runtime binary state (uniform
	// marker / per-row sidecar), so an explicit CAST into text is
	// conservatively nullable. The earlier "text source keeps NOT NULL"
	// expectation was wrong: it ignored runtime markers.
	textCast := findProjectionFn(t,
		buildPlanForNullability(t, ctx, "select cast(relname as char) c from mo_tables"), "cast")
	require.False(t, textCast.Typ.NotNullable,
		"runtime-binary-capable text source: explicit CAST into text must not be annotated NOT NULL")

	numCast := findProjectionFn(t,
		buildPlanForNullability(t, ctx, "select cast(rel_id as char) c from mo_tables"), "cast")
	require.True(t, numCast.Typ.NotNullable,
		"numeric-source explicit CAST keeps NOT NULL inference")

	binCast := findProjectionFn(t,
		buildPlanForNullability(t, ctx, "select cast(relname as binary) c from mo_tables"), "cast")
	require.True(t, binCast.Typ.NotNullable,
		"binary-target CAST keeps NOT NULL inference")
}
