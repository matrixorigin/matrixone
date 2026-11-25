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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// Issue #31882 regression. INSERT/REPLACE VALUES rows that contain a scalar
// subquery are lowered into UNION ALL branches: one non-cacheable PROJECT
// per subquery row (taken over the single-row DUAL VALUE_SCAN with the
// scalar subquery flattened into it) and one literal-only VALUE_SCAN. A
// bare NULL constant is untyped ANY until the planner assigns the target
// column type, and pSpool.GetCopiedBatch can only union concrete vector
// types. The tests walk the real lowering produced from public SQL and
// assert the typed branch contract for every bare-NULL placement.
func TestValuesWithSubqueriesTypesBareNullBranches(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		column    int
	}{
		// NULL in a literal-only row: that branch widens its VALUE_SCAN
		// columns from its first literal row, which is the untyped NULL.
		{"insert-null-literal-row", "insert into dept(deptno,dname,loc) values (1,(select 'one'),'x'),(2,null,'y')", 1},
		{"insert-null-first", "insert into dept(deptno,dname,loc) values (2,null,'y'),(1,(select 'one'),'x')", 1},
		// NULL in a literal-only row behind a typed literal row: control that
		// keeps passing because the first literal row is already typed.
		{"insert-null-after-literal", "insert into dept(deptno,dname,loc) values (1,(select 'one'),'x'),(2,'two','y'),(3,null,'z')", 1},
		// NULL inside the same row as a subquery: the projected branch
		// carries the untyped NULL.
		{"insert-null-subquery-row", "insert into dept(deptno,dname,loc) values (1,(select 'one'),null),(2,'two','y')", 2},
		{"insert-null-subquery-row-last", "insert into dept(deptno,dname,loc) values (2,'two','y'),(1,(select 'one'),null)", 2},
		// Single-row statements lower to the branch projection with no
		// UNION ALL and no ordinal.
		{"insert-single-subquery-row-null", "insert into dept(deptno,dname,loc) values (1,(select 'one'),null)", 2},
		{"replace-single-subquery-row-null", "replace into dept(deptno,dname,loc) values (1,(select 'one'),null)", 2},
		// REPLACE lowers through the same branch machinery.
		{"replace-null-literal-row", "replace into dept(deptno,dname,loc) values (1,(select 'one'),'x'),(2,null,'y')", 1},
		{"replace-null-first", "replace into dept(deptno,dname,loc) values (2,null,'y'),(1,(select 'one'),'x')", 1},
		{"replace-null-subquery-row", "replace into dept(deptno,dname,loc) values (1,(select 'one'),null),(2,'two','y')", 2},
		{"replace-null-subquery-row-last", "replace into dept(deptno,dname,loc) values (2,'two','y'),(1,(select 'one'),null)", 2},
		// An explicitly typed NULL is already correct: control.
		{"typed-null-control", "insert into dept(deptno,dname,loc) values (1,(select 'one'),'x'),(2,cast(null as varchar(15)),'y')", 1},
		// A scalar subquery that returns NULL or no row must not reopen the
		// untyped-branch defect: the destination cast of the subquery cell
		// and the typed literal branch are both asserted.
		{"insert-subquery-null-result", "insert into dept(deptno,dname,loc) values (1,(select null),'x'),(2,null,'y')", 1},
		{"insert-empty-scalar-result", "insert into dept(deptno,dname,loc) values (1,(select dname from dept where false),'x'),(2,null,'y')", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			query := p.GetQuery()

			// Anchor 1 (subquery rows, including single-row statements): the
			// VALUES branch projection that carries a bare NULL constant must
			// expose the destination column type. It is recognizable as the
			// non-cacheable lowered branch with the NULL cell at the column.
			anchored := false
			for _, node := range query.Nodes {
				if node.NodeType != plan.Node_PROJECT || !node.NotCacheable {
					continue
				}
				require.Greater(t, len(node.ProjectList), tc.column)
				lit := node.ProjectList[tc.column].GetLit()
				if lit == nil || !lit.Isnull {
					continue
				}
				require.Equal(t, int32(types.T_varchar), node.ProjectList[tc.column].Typ.Id,
					"the VALUES branch projection of a bare NULL must carry the column type")
				anchored = true
			}

			// Anchor 2 (multi-row statements): the branches are combined under
			// the ordinal SORT, and the UNION ALL plus every child branch must
			// expose a typed column for pSpool to copy.
			if valuesUnion := findValuesUnionAll(query); valuesUnion != nil {
				anchored = true
				require.Greater(t, len(valuesUnion.ProjectList), tc.column)
				require.Equal(t, int32(types.T_varchar), valuesUnion.ProjectList[tc.column].Typ.Id)
				for _, childID := range valuesUnion.Children {
					child := query.Nodes[childID]
					require.Greater(t, len(child.ProjectList), tc.column)
					require.Equal(t, int32(types.T_varchar), child.ProjectList[tc.column].Typ.Id,
						"every VALUES branch must supply a typed vector")
				}
			}
			require.True(t, anchored,
				"the statement must lower to a typed VALUES branch or VALUES UNION ALL")
		})
	}
}

func findValuesUnionAll(query *plan.Query) *plan.Node {
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_SORT || len(node.Children) != 1 || len(node.OrderBy) != 1 {
			continue
		}
		child := query.Nodes[node.Children[0]]
		if child.NodeType == plan.Node_UNION_ALL && len(child.ProjectList) > 3 {
			return child
		}
	}
	return nil
}

// Issue #31882 typed-array regression. A vector literal in a VALUES row that is
// lowered through the subquery branch machinery must reach its branch with the
// destination typed-array assignment cast already applied, exactly like the
// ordinary rowset path where the VALUE_SCAN executor casts each row to the
// declared column type. Otherwise the projected branch carries a raw VARCHAR
// literal, the UNION ALL exposes VARCHAR for a T_array destination, and the
// late insert-layer cast parses an already-materialized vector again
// (Error 20301 "malformed vector input" with float32 bytes as the input text).
func TestValuesWithSubqueriesTypedArrayBranches(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		column    int
	}{
		// Vector literal in the subquery row, all-NULL literal row behind.
		{"vector-literal-subquery-row", "insert into docs_vec_raw(id, embedding, payload) values (1,'[1,2,3]',(select 3)),(2,null,null)", 1},
		// Reversed row order: the literal branch still leads no projection.
		{"vector-literal-subquery-row-reversed", "insert into docs_vec_raw(id, embedding, payload) values (2,null,null),(1,'[1,2,3]',(select 3))", 1},
		// Vector literal inside a literal-only row of a multi-row statement.
		{"vector-literal-literal-row", "insert into docs_vec_raw(id, payload, embedding) values (3,(select 30),null),(-3,5,'[7,8,9]'),(-4,null,null)", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			query := p.GetQuery()
			arrayID := int32(types.T_array_float32)
			anchored := false
			for _, node := range query.Nodes {
				switch node.NodeType {
				case plan.Node_PROJECT:
					if !node.NotCacheable {
						continue
					}
					require.Greater(t, len(node.ProjectList), tc.column)
					require.Equal(t, arrayID, node.ProjectList[tc.column].Typ.Id,
						"every lowered VALUES branch projection must carry the destination array type")
					anchored = true
				case plan.Node_VALUE_SCAN:
					if node.RowsetData == nil || node.TableDef == nil {
						continue
					}
					require.Greater(t, len(node.TableDef.Cols), tc.column)
					require.Equal(t, arrayID, node.TableDef.Cols[tc.column].Typ.Id,
						"the literal VALUES branch must declare the destination array type")
					for row, data := range node.RowsetData.Cols[tc.column].Data {
						require.Equal(t, arrayID, data.Expr.Typ.Id,
							"literal row %d must carry the destination array type", row)
					}
					anchored = true
				}
			}
			require.True(t, anchored, "the statement must lower to typed VALUES branches")
			if valuesUnion := findValuesUnionAll(query); valuesUnion != nil {
				require.Greater(t, len(valuesUnion.ProjectList), tc.column)
				require.Equal(t, arrayID, valuesUnion.ProjectList[tc.column].Typ.Id,
					"the VALUES UNION ALL must expose the destination array type")
			}
		})
	}
}

// Issue #31882 vecf64 twin control. The lowered typed-value contract must hold
// for a T_array_float64 destination exactly as for vecf32: the binder already
// materialized the literal's physical float64 bytes inside a string-domain
// carrier, and the lowering must re-emit them as an array-domain constant
// instead of binding a text re-parse cast over them. The twin is a local
// deepcopy of the docs_vec_raw definition with the embedding column retyped
// to T_array_float64 dimension 3, registered only on this statement's private
// context: the shared mock schema and every original expectation stay
// untouched.
func TestValuesWithSubqueriesTypedArrayF64BranchCarrier(t *testing.T) {
	const twinName = "docs_vec64_raw_local"
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(true, proc)

	baseRef, baseDef, err := ctx.Resolve("constraint_test", "docs_vec_raw", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, baseRef, "docs_vec_raw must resolve with an object reference")
	require.NotNil(t, baseDef, "docs_vec_raw must resolve in the mock schema")
	twin := DeepCopyTableDef(baseDef, true)
	twin.Name = twinName
	retenabled := false
	for _, col := range twin.Cols {
		if col.Name == "embedding" {
			col.Typ.Id = int32(types.T_array_float64)
			col.Typ.Width = 3
			retenabled = true
		}
	}
	require.True(t, retenabled, "twin must retype the embedding column")

	qualified := mockQualifiedTableName("constraint_test", twinName)
	ctx.tablesByQualifiedName[qualified] = twin
	ctx.tables[twinName] = twin
	// The mock's default database is tpch, so the unqualified INSERT resolves
	// the twin through the bare-name compatibility maps, like the returned
	// table def already did. The object reference must be seeded the same way
	// from the resolved base reference: with a nil objRef,
	// DMLContext.resolveSingleTable dereferences objRef.PubInfo and panics.
	twinRef := *baseRef
	twinRef.ObjName = twinName
	ctx.objectsByQualifiedName[qualified] = &twinRef
	ctx.objects[twinName] = &twinRef

	sql := "insert into " + twinName + " values (1, '[1,2,3]', (select 3)), (2, null, null)"
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	p, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	query := p.GetQuery()

	// float64 little-endian bytes of [1.0, 2.0, 3.0].
	f64bytes := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40,
	}
	f64ID := int32(types.T_array_float64)

	anchored := false
	for _, node := range query.Nodes {
		switch node.NodeType {
		case plan.Node_PROJECT:
			if !node.NotCacheable {
				continue
			}
			require.Greater(t, len(node.ProjectList), 1)
			proj := node.ProjectList[1]
			require.Equal(t, f64ID, proj.Typ.Id,
				"every lowered VALUES branch projection must carry the vecf64 destination type")
			require.EqualValues(t, 3, proj.Typ.Width,
				"every lowered VALUES branch projection must keep dimension 3")
			if lit := proj.GetLit(); lit != nil && !lit.GetIsnull() {
				if vec, ok := lit.GetValue().(*plan.Literal_VecVal); ok {
					require.Equal(t, f64bytes, []byte(vec.VecVal),
						"the branch constant must keep the physical float64 bytes in the array domain")
				}
			}
			anchored = true
		case plan.Node_VALUE_SCAN:
			if node.RowsetData == nil || node.TableDef == nil {
				continue
			}
			require.Greater(t, len(node.TableDef.Cols), 1)
			require.Equal(t, f64ID, node.TableDef.Cols[1].Typ.Id,
				"the literal VALUES branch must declare the vecf64 destination type")
			require.EqualValues(t, 3, node.TableDef.Cols[1].Typ.Width,
				"the literal VALUES branch must keep dimension 3")
			for row, data := range node.RowsetData.Cols[1].Data {
				require.Equal(t, f64ID, data.Expr.Typ.Id,
					"literal row %d must carry the vecf64 destination type", row)
			}
			anchored = true
		}
	}
	require.True(t, anchored, "the twin statement must lower to typed VALUES branches")
	if valuesUnion := findValuesUnionAll(query); valuesUnion != nil {
		require.Greater(t, len(valuesUnion.ProjectList), 1)
		require.Equal(t, f64ID, valuesUnion.ProjectList[1].Typ.Id,
			"the VALUES UNION ALL must expose the vecf64 destination type")
	}
}

// TestValuesWithSubqueriesTypedArrayBitBoundary: the PR32350 review P2 witness
// at plan level, for INSERT, REPLACE and the vecf64 destination. A bit-literal
// payload is never a parsed vector carrier: no lowered branch may re-emit it
// as a bare Literal_VecVal. Uniformly with the vecf32 lowering, the row keeps
// the checked string-to-array assignment conversion in its branch expression.
func TestValuesWithSubqueriesTypedArrayBitBoundary(t *testing.T) {
	for _, stmt := range []struct {
		name, sql string
	}{
		{"insert-vecf32-bit", `insert into docs_vec_raw values (1, b'1', (select 3)), (2, null, null)`},
		{"replace-vecf32-bit", `replace into docs_vec_raw values (1, b'1', (select 3))`},
		// Destination-aligned but never parsed: 32 payload bits cannot pass a
		// length-alignment gate either.
		{"insert-vecf32-bit-aligned4", `insert into docs_vec_raw values (2, b'00111111100000000000000000000000', (select 3)), (3, null, null)`},
		// Ordinary all-literal VALUES row: the same guard consumer on the
		// rowset path, with no lowered subquery branch.
		{"insert-vecf32-all-literal-bit", `insert into docs_vec_raw values (9, b'1', 9)`},
	} {
		t.Run(stmt.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, stmt.sql)
			require.NoError(t, err, "the bit row must plan like any VALUES row")
			anchored := false
			for _, node := range p.GetQuery().GetNodes() {
				switch node.NodeType {
				case plan.Node_PROJECT:
					if !node.NotCacheable || len(node.ProjectList) <= 1 {
						continue
					}
					proj := node.ProjectList[1]
					require.Equal(t, int32(types.T_array_float32), proj.Typ.Id,
						"the lowered bit row keeps the destination array type")
					requireNoBareVecValConst(t, proj, "lowered bit row")
					anchored = true
				case plan.Node_VALUE_SCAN:
					if node.RowsetData == nil || node.TableDef == nil ||
						len(node.TableDef.Cols) <= 1 || len(node.RowsetData.Cols) <= 1 {
						continue
					}
					require.Equal(t, int32(types.T_array_float32), node.TableDef.Cols[1].Typ.Id,
						"the literal VALUES branch declares the destination array type")
					for row := range node.RowsetData.Cols[1].Data {
						e := node.RowsetData.Cols[1].Data[row].Expr
						require.Equal(t, int32(types.T_array_float32), e.Typ.Id,
							"literal row %d keeps the destination array type", row)
						requireNoBareVecValConst(t, e, "literal VALUES branch")
					}
					anchored = true
				}
			}
			require.True(t, anchored, "the bit row must lower a typed VALUES branch")
		})
	}

	// vecf64 destination through a local deepcopy twin of docs_vec_raw (the
	// same private-context resolver as the f64 carrier test; the shared mock
	// schema stays untouched).
	const twinName = "docs_vec64_bit_local"
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(true, proc)
	baseRef, baseDef, err := ctx.Resolve("constraint_test", "docs_vec_raw", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, baseRef, "docs_vec_raw must resolve with an object reference")
	require.NotNil(t, baseDef, "docs_vec_raw must resolve in the mock schema")
	twin := DeepCopyTableDef(baseDef, true)
	twin.Name = twinName
	retyped := false
	for _, col := range twin.Cols {
		if col.Name == "embedding" {
			col.Typ.Id = int32(types.T_array_float64)
			col.Typ.Width = 3
			retyped = true
		}
	}
	require.True(t, retyped, "twin must retype the embedding column")
	qualified := mockQualifiedTableName("constraint_test", twinName)
	ctx.tablesByQualifiedName[qualified] = twin
	ctx.tables[twinName] = twin
	twinRef := *baseRef
	twinRef.ObjName = twinName
	ctx.objectsByQualifiedName[qualified] = &twinRef
	ctx.objects[twinName] = &twinRef

	for _, stmt := range []struct {
		name, sql string
	}{
		{"vecf64-insert-bit-b1", "insert into " + twinName + " values (1, b'1', (select 3)), (2, null, null)"},
		{"vecf64-replace-bit-b1", "replace into " + twinName + " values (1, b'1', (select 3))"},
		// 64 destination-aligned payload bits: aligned for float64, still
		// never parsed by the vector parser.
		{"vecf64-insert-bit-aligned8", "insert into " + twinName + " values (2, b'0011111111110000000000000000000000000000000000000000000000000000', (select 3)), (3, null, null)"},
	} {
		t.Run(stmt.name, func(t *testing.T) {
			parsed, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, stmt.sql, 1)
			require.NoError(t, err, "grammar must accept bit literals")
			p, err := BuildPlan(ctx, parsed, false, nil)
			require.NoError(t, err, "the vecf64 bit row must plan like any VALUES row")
			anchored := false
			for _, node := range p.GetQuery().GetNodes() {
				if node.NodeType != plan.Node_PROJECT || !node.NotCacheable || len(node.ProjectList) <= 1 {
					continue
				}
				proj := node.ProjectList[1]
				require.Equal(t, int32(types.T_array_float64), proj.Typ.Id,
					"the lowered vecf64 bit row keeps the destination array type")
				requireNoBareVecValConst(t, proj, "lowered vecf64 bit row")
				anchored = true
			}
			require.True(t, anchored, "the vecf64 bit row must lower a NotCacheable branch")
		})
	}
}

// requireNoBareVecValConst asserts a branch expression was not re-emitted as
// a pre-parsed vector literal: a BIT payload is raw decoded bits, and handing
// them to the array-domain constant decoder either panics on an unaligned
// length or silently decodes unvalidated bits.
func requireNoBareVecValConst(t *testing.T, e *plan.Expr, stage string) {
	t.Helper()
	if lit := e.GetLit(); lit != nil && !lit.GetIsnull() {
		if _, ok := lit.GetValue().(*plan.Literal_VecVal); ok {
			t.Fatalf("%s re-emitted a bare VecVal constant: %v", stage, e)
		}
	}
}
