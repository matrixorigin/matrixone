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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
// already-materialized bytes as text and fails with Error 20301 "malformed
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
	return plan2.BuildPlan(ctx, stmt, false, nil)
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

// TestInsertValuesSubqueryBitLiteralBoundary: the PR32350 review P2 witness.
// A BIT payload is never a parsed vector carrier: SetInsertValueString's P_bit
// arm returns raw decoded bits and bypasses the array parser, so the binder
// fast path must not hand them to this lowering as a pre-parsed constant.
// Every BIT row below must be rejected by the checked string-to-array
// conversion (moerr 20301 "malformed vector input") at bind time or when the
// branch's own embedding expression is evaluated through the real expression
// executor — never re-emitted as a bare VecVal, never silently decoded, and
// never crashing the constant decoder on an unaligned payload. The
// aligned-payload cases prove a length-alignment gate cannot substitute the
// origin check: 32/64 payload bits decode to a float32/float64-aligned byte
// length yet were never parsed by the vector parser. Both destinations run
// through the same BuildPlan -> original colexec executor consumer, for
// INSERT and REPLACE lowered branches and for the ordinary all-literal
// VALUES row the same guard reaches.
func TestInsertValuesSubqueryBitLiteralBoundary(t *testing.T) {
	f32 := bitDest{
		name:       "vecf32",
		table:      "docs_vec_raw",
		alignedBit: "00111111100000000000000000000000",
		oid:        types.T_array_float32,
		validBytes: []byte{0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40},
	}
	f64 := bitDest{
		name:       "vecf64",
		table:      bitF64TwinTable,
		alignedBit: "0011111111110000000000000000000000000000000000000000000000000000",
		oid:        types.T_array_float64,
		validBytes: []byte{
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40,
		},
	}

	run := func(t *testing.T, d bitDest, proc *process.Process, build plan2.CompilerContext, mock *plan2.MockCompilerContext) {
		t.Run("insert-bit-b1-unaligned", func(t *testing.T) {
			checkBitBoundaryCase(t, proc, build, mock,
				fmt.Sprintf(`insert into %s values (1, b'1', (select 3)), (2, null, null)`, d.table), false)
		})
		t.Run("replace-bit-b1-unaligned", func(t *testing.T) {
			checkBitBoundaryCase(t, proc, build, mock,
				fmt.Sprintf(`replace into %s values (1, b'1', (select 3))`, d.table), false)
		})
		t.Run("insert-bit-aligned-unparsed", func(t *testing.T) {
			checkBitBoundaryCase(t, proc, build, mock,
				fmt.Sprintf(`insert into %s values (2, b'%s', (select 3)), (3, null, null)`, d.table, d.alignedBit), false)
		})
		t.Run("insert-all-literal-bit-b1", func(t *testing.T) {
			checkBitBoundaryCase(t, proc, build, mock,
				fmt.Sprintf(`insert into %s values (9, b'1', 9)`, d.table), true)
		})
		t.Run("insert-bit-b1-decoder-panic-isolated", func(t *testing.T) {
			checkBitDecoderPanicIsolated(t, proc, build, mock,
				fmt.Sprintf(`insert into %s values (1, b'1', (select 3)), (2, null, null)`, d.table))
		})
		t.Run("reuse-next-valid-and-typed-null", func(t *testing.T) {
			checkBitReuseNextValid(t, proc, build, mock, d)
		})
	}

	t.Run("vecf32", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		ctx := plan2.NewMockCompilerContext(true, proc)
		run(t, f32, proc, ctx, ctx)
	})
	t.Run("vecf64", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mock := plan2.NewMockCompilerContext(true, proc)
		run(t, f64, proc, &bitF64TwinContext{MockCompilerContext: mock}, mock)
	})
}

// bitDest parameterizes the f32/f64 destinations of the BIT boundary test.
type bitDest struct {
	name       string
	table      string
	alignedBit string // destination-aligned but never parsed BIT literal
	oid        types.T
	validBytes []byte // physical bytes of the genuine '[1,2,3]' literal
}

// bitF64TwinTable is a vecf64(3) twin of the mock docs_vec_raw table. The
// shared mock schema exposes no array_float64 destination and its table maps
// are private, so the twin is resolved by a small test-local embedding of the
// real mock context: every resolution except the twin name delegates to the
// real resolver. The twin keeps the resolved object reference, every column
// and the dimension of the base definition; only the embedding column's
// element type is retyped. The shared mock schema stays untouched.
const bitF64TwinTable = "docs_vec64_bit_exec"

type bitF64TwinContext struct {
	*plan2.MockCompilerContext
}

func (c *bitF64TwinContext) resolveTwin(dbName, tableName string, snapshot *plan2.Snapshot) (*plan2.ObjectRef, *plan2.TableDef, error) {
	if strings.ToLower(tableName) != bitF64TwinTable {
		return c.MockCompilerContext.Resolve(dbName, tableName, snapshot, nil)
	}
	baseRef, baseDef, err := c.MockCompilerContext.Resolve("constraint_test", "docs_vec_raw", snapshot, nil)
	if err != nil || baseRef == nil || baseDef == nil {
		return baseRef, baseDef, err
	}
	twinRef := *baseRef
	twinRef.ObjName = bitF64TwinTable
	twin := plan2.DeepCopyTableDef(baseDef, true)
	twin.Name = bitF64TwinTable
	for _, col := range twin.Cols {
		if col.Name == "embedding" {
			col.Typ.Id = int32(types.T_array_float64)
			col.Typ.Width = 3
		}
	}
	return &twinRef, twin, nil
}

// The twin changes the resolved descriptor, so neither override can return a
// borrowed relation from the underlying mock.
func (c *bitF64TwinContext) Resolve(dbName, tableName string, snapshot *plan2.Snapshot, output *plan2.TableResolution) (*plan2.ObjectRef, *plan2.TableDef, error) {
	output.Reset()
	return c.resolveTwin(dbName, tableName, snapshot)
}

func (c *bitF64TwinContext) ResolveWithoutTemporaryAlias(dbName, tableName string, snapshot *plan2.Snapshot, output *plan2.TableResolution) (*plan2.ObjectRef, *plan2.TableDef, error) {
	output.Reset()
	return c.resolveTwin(dbName, tableName, snapshot)
}

// requireCheckedVectorRejection accepts only the checked vector conversion
// rejection: the array parser's moerr.ErrInvalidInput (20301) "malformed vector
// input". Any other bind/setup/evaluation error (missing object, harness or
// internal failure) fails the test instead of silently satisfying it.
func requireCheckedVectorRejection(t *testing.T, err error, stage string) {
	t.Helper()
	require.Error(t, err, "stage %s: a BIT payload must be rejected by the checked vector conversion", stage)
	if !moerr.IsMoErrCode(err, moerr.ErrInvalidInput) || !strings.Contains(err.Error(), "malformed vector input") {
		t.Fatalf("stage %s: rejection is not the checked vector conversion class (20301 \"malformed vector input\"): %v",
			stage, err)
	}
}

// requireBitEvalRejected evaluates a branch expression through the real
// expression executor — the same consumer as evalRowsetData and the
// projection executor — and requires the checked vector conversion
// rejection. A successful evaluation means unvalidated bits were decoded.
func requireBitEvalRejected(t *testing.T, proc *process.Process, e *planpb.Expr, stage string) {
	t.Helper()
	exec, setupErr := colexec.NewExpressionExecutor(proc, e)
	if setupErr != nil {
		requireCheckedVectorRejection(t, setupErr, stage+" executor setup")
		return
	}
	defer exec.Free()
	v, evalErr := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if evalErr != nil {
		requireCheckedVectorRejection(t, evalErr, stage+" evaluation")
		return
	}
	t.Fatalf("stage %s: unchecked BIT payload evaluated without a conversion error: length %d, type %v",
		stage, v.Length(), v.GetType())
}

// requireNoBareVecVal asserts a branch expression is not re-emitted as a
// pre-parsed vector literal: a BIT payload is raw decoded bits, and handing
// them to the array-domain constant decoder either panics on an unaligned
// length or silently decodes unvalidated bits.
func requireNoBareVecVal(t *testing.T, e *planpb.Expr, stage string) {
	t.Helper()
	if lit := e.GetLit(); lit != nil && !lit.GetIsnull() {
		if _, ok := lit.GetValue().(*planpb.Literal_VecVal); ok {
			t.Fatalf("stage %s: bit payload re-emitted as a pre-parsed VecVal constant: %v", stage, e)
		}
	}
}

// checkBitBoundaryCase proves one BIT row never reaches an unchecked vector
// constant: bind-time checked rejection, or every lowered branch expression
// keeps the checked conversion through the real executor. rowsetOnly selects
// the ordinary all-literal VALUES row (no lowered subquery branch).
func checkBitBoundaryCase(t *testing.T, proc *process.Process, build plan2.CompilerContext, mock *plan2.MockCompilerContext, sql string, rowsetOnly bool) {
	t.Helper()
	stmt, err := parsers.ParseOne(mock.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, "grammar must accept BIT literals")
	p, err := plan2.BuildPlan(build, stmt, false, nil)
	if err != nil {
		requireCheckedVectorRejection(t, err, "bind")
		return
	}
	checked := 0
	nodes := p.GetQuery().GetNodes()
	if !rowsetOnly {
		// Lowered subquery-row branch: the embedding projection may keep a
		// checked cast over the string carrier, never a bare VecVal.
		for i := range nodes {
			n := nodes[i]
			if n.NodeType != planpb.Node_PROJECT || !n.NotCacheable || len(n.ProjectList) <= 1 {
				continue
			}
			e := n.ProjectList[1]
			if e.GetCol() != nil || exprHasColRef(e) || !isArrayDomainType(e.Typ.Id) {
				continue
			}
			requireNoBareVecVal(t, e, "subquery-row branch")
			checked++
			requireBitEvalRejected(t, proc, e, "subquery-row branch")
		}
	}
	// Rowset (all-literal) branch: the same contract on the VALUE_SCAN row
	// data; typed NULL rows are skipped.
	for i := range nodes {
		n := nodes[i]
		if n.NodeType != planpb.Node_VALUE_SCAN || n.RowsetData == nil || n.TableDef == nil ||
			len(n.TableDef.Cols) <= 1 || len(n.RowsetData.Cols) <= 1 {
			continue
		}
		if !isArrayDomainType(n.TableDef.Cols[1].Typ.Id) {
			continue
		}
		for row := range n.RowsetData.Cols[1].Data {
			e := n.RowsetData.Cols[1].Data[row].Expr
			if lit := e.GetLit(); lit != nil && lit.GetIsnull() {
				continue
			}
			requireNoBareVecVal(t, e, "rowset branch")
			checked++
			requireBitEvalRejected(t, proc, e, "rowset branch")
		}
	}
	require.Greater(t, checked, 0,
		"the BIT row must surface at least one lowered branch expression or a bind-time checked rejection")
}

// checkBitDecoderPanicIsolated keeps the unaligned-payload counterfactual
// crash apart from the aligned-payload counterexamples: under the pre-fix
// producer the branch carries a bare VecVal whose length is not a multiple of
// the element width, and evaluating it panics the constant decoder. This
// subcase skips the VecVal pre-check so the panic path is exercised in its
// own test invocation.
func checkBitDecoderPanicIsolated(t *testing.T, proc *process.Process, build plan2.CompilerContext, mock *plan2.MockCompilerContext, sql string) {
	t.Helper()
	stmt, err := parsers.ParseOne(mock.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, "grammar must accept BIT literals")
	p, err := plan2.BuildPlan(build, stmt, false, nil)
	if err != nil {
		requireCheckedVectorRejection(t, err, "bind")
		return
	}
	embedding := firstArrayBranchEmbedding(p)
	if embedding == nil {
		t.Fatalf("the BIT row lowered no branch embedding expression")
	}
	requireBitEvalRejected(t, proc, embedding, "unaligned decoder isolation")
}

func firstArrayBranchEmbedding(p *planpb.Plan) *planpb.Expr {
	for _, n := range p.GetQuery().GetNodes() {
		if n.NodeType != planpb.Node_PROJECT || !n.NotCacheable || len(n.ProjectList) <= 1 {
			continue
		}
		e := n.ProjectList[1]
		if e.GetCol() != nil || exprHasColRef(e) || !isArrayDomainType(e.Typ.Id) {
			continue
		}
		return e
	}
	return nil
}

// checkBitReuseNextValid proves the same process and context keep binding and
// evaluating genuine vector literals after the BIT rejections: exact physical
// destination bytes, the declared type and dimension, and the typed NULL row.
// Both witnesses must actually select their branch, so a zero-hit loop can
// never fake a pass.
func checkBitReuseNextValid(t *testing.T, proc *process.Process, build plan2.CompilerContext, mock *plan2.MockCompilerContext, d bitDest) {
	t.Helper()
	stmt, err := parsers.ParseOne(mock.GetContext(), dialect.MYSQL,
		fmt.Sprintf(`insert into %s values (1, '[1,2,3]', (select 3)), (2, null, null)`, d.table), 1)
	require.NoError(t, err)
	p, err := plan2.BuildPlan(build, stmt, false, nil)
	require.NoError(t, err, "the same process and context must keep binding genuine vector literals after the BIT rejections")
	nodes := p.GetQuery().GetNodes()

	// The literal row is a typed array NULL on the destination column.
	rowsetNulls := 0
	for i := range nodes {
		n := nodes[i]
		if n.NodeType != planpb.Node_VALUE_SCAN || n.RowsetData == nil || n.TableDef == nil ||
			len(n.TableDef.Cols) <= 1 || len(n.RowsetData.Cols) <= 1 {
			continue
		}
		if n.TableDef.Cols[1].Typ.Id != int32(d.oid) {
			continue
		}
		for row := range n.RowsetData.Cols[1].Data {
			res := evalConst(t, proc, n.RowsetData.Cols[1].Data[row].Expr)
			require.True(t, res.isNull, "the literal row must still lower to a typed array NULL")
			require.Equal(t, d.oid, res.oid)
			require.EqualValues(t, 3, res.width)
			rowsetNulls++
		}
	}
	require.Greater(t, rowsetNulls, 0, "the reuse witness must select the typed NULL row")

	// The subquery-row branch carries the genuine vector carrier with its
	// exact physical bytes through the real expression executor.
	seen := 0
	for i := range nodes {
		n := nodes[i]
		if n.NodeType != planpb.Node_PROJECT || !n.NotCacheable || len(n.ProjectList) <= 1 {
			continue
		}
		e := n.ProjectList[1]
		if e.GetCol() != nil || exprHasColRef(e) || e.Typ.Id != int32(d.oid) {
			continue
		}
		res := evalConst(t, proc, e)
		require.False(t, res.isNull)
		require.Equal(t, d.oid, res.oid)
		require.EqualValues(t, 3, res.width)
		require.Equal(t, d.validBytes, res.payload,
			"the genuine vector carrier must materialize its exact destination bytes")
		seen++
	}
	require.Greater(t, seen, 0, "the reuse witness must select at least one valid embedding branch")
}
