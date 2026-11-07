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

package value_scan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// #31882 vector-literal consumer regression: INSERT VALUES rows that mix a
// bare literal, a scalar subquery and bare NULLs against a vecf32 destination
// column.
//
// The value binder materializes a vector literal into its physical float32
// bytes inside a string-domain carrier constant (MakeInsertValueConstExpr ->
// SetInsertValueString -> arrayUserVariableValueToBytes), which is the
// existing ordinary all-literal VALUE_SCAN contract: the scan executor copies
// those bytes into a column vector typed by the destination declaration. The
// subquery lowering must keep that carrier's physical bytes in the array
// domain instead of binding a cast over them: the cast re-parses
// already-materialized bytes as text and fails with Error 20101 "malformed
// vector input", the real fixture signature reproduced against the pre-fix
// source (return21 red-run3: four shapes, identical byte payload).
//
// docs_vec_raw rows: (id int32 PK, embedding vecf32(3) nullable, payload int32);
// the embedding column sits at position 1 and multi-row statements add a
// trailing _values_ordinal projection at position 3.
//
// Every value assertion below evaluates the plan's own expression through the
// real expression executor, the same way evalRowsetData and the projection
// executor consume branch constants. The executor owns and frees its result
// vector, so evalConst snapshots the row before releasing the executor.
func TestInsertValuesSubqueryVectorLiteralMaterialization(t *testing.T) {
	f123 := []byte{0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40} // [1,2,3]
	f456 := []byte{0x00, 0x00, 0x80, 0x40, 0x00, 0x00, 0xa0, 0x40, 0x00, 0x00, 0xc0, 0x40} // [4,5,6]
	f789 := []byte{0x00, 0x00, 0xe0, 0x40, 0x00, 0x00, 0x00, 0x41, 0x00, 0x00, 0x10, 0x41} // [7,8,9]
	f234 := []byte{0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40, 0x00, 0x00, 0x80, 0x40} // [2,3,4]

	const embeddingCol = 1

	for _, tc := range []struct {
		name  string
		sql   string
		subq  interface{} // subquery-row branch embedding: []byte or nil (typed array NULL)
		rows  []interface{}
		union int // expected number of UNION_ALL nodes
	}{
		{
			// Original failure shape: vector literal in the subquery row,
			// bare-NULL literal row behind it.
			name:  "literal-subquery-then-null-row",
			sql:   `insert into docs_vec_raw values (1, '[1,2,3]', (select 3)), (2, null, null)`,
			subq:  f123,
			rows:  []interface{}{nil},
			union: 1,
		},
		{
			// Reversed row order: bare-NULL literal row first, vector literal
			// in the subquery row behind it.
			name:  "null-row-then-literal-subquery",
			sql:   `insert into docs_vec_raw values (-2, null, null), (-1, '[4,5,6]', (select 4))`,
			subq:  f456,
			rows:  []interface{}{nil},
			union: 1,
		},
		{
			// NULL embedding in the subquery row; a vector-literal row and a
			// NULL-only row share the literal branch: two branches in total.
			name:  "subquery-null-then-literals",
			sql:   `insert into docs_vec_raw values (3, null, (select 30)), (-3, '[7,8,9]', 5), (-4, null, null)`,
			subq:  nil,
			rows:  []interface{}{f789, nil},
			union: 1,
		},
		{
			// Single subquery row: no UNION ALL, one PROJECT branch.
			name:  "single-subquery-row",
			sql:   `insert into docs_vec_raw values (5, '[2,3,4]', (select 2))`,
			subq:  f234,
			rows:  nil,
			union: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			ctx := plan2.NewMockCompilerContext(true, proc)

			p := buildInsertPlan(t, ctx, tc.sql)
			nodes := p.GetQuery().GetNodes()

			// Literal-only branch: the VALUE_SCAN carrying RowsetData must
			// declare the destination array type for the embedding column, and
			// each of its embedding rowset expressions must evaluate to that
			// destination type's value (literal bytes or typed NULL).
			var rowsetNode *planpb.Node
			for i := range nodes {
				if nodes[i].NodeType == planpb.Node_VALUE_SCAN && nodes[i].RowsetData != nil {
					rowsetNode = nodes[i]
				}
			}
			if len(tc.rows) > 0 {
				require.NotNil(t, rowsetNode, "mixed VALUES must lower a literal-only VALUE_SCAN branch")
				require.EqualValues(t, int32(types.T_array_float32),
					rowsetNode.TableDef.Cols[embeddingCol].Typ.Id,
					"literal branch embedding column must carry the destination array type")
				require.EqualValues(t, 3,
					rowsetNode.TableDef.Cols[embeddingCol].Typ.Width,
					"literal branch embedding column must keep dimension 3")
				require.Equal(t, len(tc.rows), len(rowsetNode.RowsetData.Cols[embeddingCol].Data))
				for r, want := range tc.rows {
					res := evalConst(t, proc, rowsetNode.RowsetData.Cols[embeddingCol].Data[r].Expr)
					require.Equal(t, types.T_array_float32, res.oid,
						"rowset embedding row %d: evaluated vector must be array_float32", r)
					require.EqualValues(t, 3, res.width,
						"rowset embedding row %d: evaluated vector must keep dimension 3", r)
					if want == nil {
						require.True(t, res.isNull,
							"rowset embedding row %d must evaluate to a typed array NULL", r)
					} else {
						require.False(t, res.isNull,
							"rowset embedding row %d must not be NULL", r)
						require.Equal(t, want.([]byte), res.payload,
							"rowset embedding row %d must decode to the literal's float32 bytes", r)
					}
				}
			}

			// Subquery-row branch: the PROJECT over the single-row DUAL scan
			// must carry the row's own array-domain embedding constant, and it
			// must evaluate to the same destination contract.
			subqSeen := false
			for i := range nodes {
				n := nodes[i]
				if n.NodeType != planpb.Node_PROJECT || !n.NotCacheable || len(n.ProjectList) <= embeddingCol {
					continue
				}
				e := n.ProjectList[embeddingCol]
				if e.GetCol() != nil || exprHasColRef(e) || !isArrayDomainType(e.Typ.Id) {
					continue
				}
				subqSeen = true
				res := evalConst(t, proc, e)
				require.Equal(t, types.T_array_float32, res.oid,
					"subquery-row embedding projection must evaluate to array_float32")
				require.EqualValues(t, 3, res.width,
					"subquery-row embedding projection must keep dimension 3")
				if tc.subq == nil {
					require.True(t, res.isNull,
						"subquery-row embedding must evaluate to a typed array NULL")
				} else {
					require.False(t, res.isNull)
					require.Equal(t, tc.subq.([]byte), res.payload,
						"subquery-row embedding must decode to the literal's float32 bytes")
				}
			}
			require.True(t, subqSeen,
				"mixed VALUES must lower a PROJECT branch whose embedding projection carries the array-domain constant")

			// UNION ALL combination must keep the array domain at the embedding
			// position.
			unionSeen := 0
			for i := range nodes {
				n := nodes[i]
				if n.NodeType != planpb.Node_UNION_ALL {
					continue
				}
				unionSeen++
				require.EqualValues(t, int32(types.T_array_float32),
					n.ProjectList[embeddingCol].Typ.Id,
					"union node[%d] embedding output column must keep the array type", i)
			}
			require.Equal(t, tc.union, unionSeen, "unexpected UNION_ALL lowering")
		})
	}
}

// TestInsertValuesSubqueryVectorLiteralErrorControls proves the binder's
// vector conversion checks stay enforced for the lowered shape, and that one
// context and process keep binding correct plans after such bind-time errors:
// the fast-path conversion runs before the branch lowering, so dimension and
// parse rejections must reject the whole statement exactly like the ordinary
// all-literal path.
func TestInsertValuesSubqueryVectorLiteralErrorControls(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := plan2.NewMockCompilerContext(true, proc)
	f123 := []byte{0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40} // [1,2,3]

	// Dimension control: a 2-element literal against vecf32(3) must be
	// rejected by the existing fixed-dimension validation instead of
	// reaching any branch.
	p, err := buildInsertPlanErr(t, ctx,
		`insert into docs_vec_raw values (1, '[1,2]', (select 3)), (2, null, null)`)
	require.Error(t, err, "the fixed-dimension check must stay enforced for lowered VALUES rows")
	require.Nil(t, p)
	t.Logf("dimension-mismatch rejection: %v", err)

	// Parse control: a malformed vector literal must be rejected.
	p, err = buildInsertPlanErr(t, ctx,
		`insert into docs_vec_raw values (1, '[1,x]', (select 3))`)
	require.Error(t, err, "malformed vector text must stay rejected for lowered VALUES rows")
	require.Nil(t, p)
	t.Logf("malformed-vector rejection: %v", err)

	// Reuse control: the same context and process bind and evaluate the next
	// valid statement correctly: the two branches still materialize the
	// expected typed payload and NULL.
	p, err = buildInsertPlanErr(t, ctx,
		`insert into docs_vec_raw values (1, '[1,2,3]', (select 3)), (2, null, null)`)
	require.NoError(t, err)
	nodes := p.GetQuery().GetNodes()

	var rowsetNode *planpb.Node
	for i := range nodes {
		if nodes[i].NodeType == planpb.Node_VALUE_SCAN && nodes[i].RowsetData != nil {
			rowsetNode = nodes[i]
		}
	}
	require.NotNil(t, rowsetNode, "valid statement after rejections must lower a literal branch")
	res := evalConst(t, proc, rowsetNode.RowsetData.Cols[1].Data[0].Expr)
	require.True(t, res.isNull, "reused context must still lower a typed array NULL row")
	require.Equal(t, types.T_array_float32, res.oid)

	reused := false
	for i := range nodes {
		n := nodes[i]
		if n.NodeType != planpb.Node_PROJECT || !n.NotCacheable || len(n.ProjectList) <= 1 {
			continue
		}
		e := n.ProjectList[1]
		if e.GetCol() != nil || exprHasColRef(e) || !isArrayDomainType(e.Typ.Id) {
			continue
		}
		res := evalConst(t, proc, e)
		require.Equal(t, types.T_array_float32, res.oid,
			"reused context must still bind the array-domain constant")
		require.False(t, res.isNull)
		require.Equal(t, f123, res.payload,
			"reused context must still materialize the literal's float32 bytes")
		reused = true
	}
	require.True(t, reused, "reused context must lower the subquery-row branch")
}

func buildInsertPlan(t *testing.T, ctx *plan2.MockCompilerContext, sql string) *planpb.Plan {
	t.Helper()
	p, err := buildInsertPlanErr(t, ctx, sql)
	require.NoError(t, err, "build %s", sql)
	return p
}

func buildInsertPlanErr(t *testing.T, ctx *plan2.MockCompilerContext, sql string) (*planpb.Plan, error) {
	t.Helper()
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	if err != nil {
		return nil, err
	}
	return plan2.BuildPlan(ctx, stmt, false)
}

// evalResult snapshots the evaluated vector before the executor is freed: the
// executor owns the vector and frees it with exec.Free, so the caller must not
// keep a reference to it (the borrowed-vector rule of evalRowsetData's
// expression executor).
type evalResult struct {
	oid     types.T
	width   int32
	isNull  bool
	payload []byte
}

// evalConst evaluates a constant plan expression through the real expression
// executor, the way evalRowsetData and the projection executor evaluate
// row-local and projected constants. Null detection mirrors the runtime
// consumer: vec.Copy reads w.IsConstNull() || (!w.IsConst() &&
// w.GetNulls().Contains(wi)); constant NULL vectors carry null via the
// const-null flag and an empty data area, without a nulls bitmap row.
func evalConst(t *testing.T, proc *process.Process, e *planpb.Expr) evalResult {
	t.Helper()
	exec, err := colexec.NewExpressionExecutor(proc, e)
	require.NoError(t, err, "executor setup for expression type id %d", e.Typ.Id)
	defer exec.Free()
	v, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	require.NoError(t, err, "eval expression type id %d", e.Typ.Id)
	require.Greater(t, v.Length(), 0, "eval returned an empty vector")
	res := evalResult{
		oid:    v.GetType().Oid,
		width:  v.GetType().Width,
		isNull: v.IsConstNull() || (!v.IsConst() && v.GetNulls().Contains(0)),
	}
	if !res.isNull {
		res.payload = append([]byte(nil), v.GetBytesAt(0)...)
	}
	return res
}

func isArrayDomainType(id int32) bool {
	return types.T(id) == types.T_array_float32 || types.T(id) == types.T_array_float64
}

// exprHasColRef reports whether the expression reads a column reference
// anywhere. A VALUES branch projection over the single-row DUAL scan carries
// only the row's own constants; a projection that re-casts UNION output
// columns is not a constant expression and is excluded from const-eval here.
func exprHasColRef(e *planpb.Expr) bool {
	if e == nil {
		return false
	}
	switch impl := e.Expr.(type) {
	case *planpb.Expr_Col:
		return true
	case *planpb.Expr_F:
		for _, a := range impl.F.Args {
			if exprHasColRef(a) {
				return true
			}
		}
	case *planpb.Expr_List:
		for _, item := range impl.List.List {
			if exprHasColRef(item) {
				return true
			}
		}
	}
	return false
}
