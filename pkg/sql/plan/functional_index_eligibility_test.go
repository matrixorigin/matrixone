// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// issue32437Context builds a mock DDL context with the functional-index
// authoring gates enabled: the persisted-extract admission floor covered by
// the pre-existing JSON-key tests (MORPCLatestVersion), which also satisfies
// the protocol-107 functional-index authoring gate used by lowerFunctionalIndex.
func issue32437Context(t *testing.T) *MockCompilerContext {
	t.Helper()
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	rt := runtime.ServiceRuntime(ctx.GetProcess().GetService())
	old, had := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	return ctx
}

func issue32437Build(t *testing.T, ctx CompilerContext, sql string) (*Plan, error) {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	if err != nil {
		return nil, err
	}
	defer stmt.Free()
	return BuildPlan(ctx, stmt, false, nil)
}

// issue32437Schema is the shared authoring surface: varchar and CHAR text
// columns, a signed and an unsigned position column, a JSON column and the
// temporal controls. Callers append index definitions and close the paren.
const issue32437Schema = "create table fi (id int primary key, name varchar(40), cc char(10), pos int, upos int unsigned, doc json, ts timestamp, dt datetime, d date, ds varchar(30)"

// issue32437Admit asserts admission with exact replay preservation: one hidden
// owner column, the asserted hidden key type, validateFunctionalTable and a
// ConstructCreateTableSQL replay that keeps the key type and the persisted
// spelling.
func issue32437Admit(t *testing.T, ctx CompilerContext, expression string, keyId types.T, keyWidth int32) *pb.ColDef {
	t.Helper()
	p, err := issue32437Build(t, ctx, issue32437Schema+",index ix (("+expression+")))")
	require.NoError(t, err)
	table := p.GetDdl().GetCreateTable().TableDef
	require.NoError(t, validateFunctionalTable(t.Context(), table))
	owners := functionalIndexColumns(table, table.Indexes[0])
	require.Len(t, owners, 1, "exactly one hidden owner column")
	require.Equal(t, int32(keyId), owners[0].Typ.Id, "hidden key type")
	if keyWidth >= 0 {
		require.Equal(t, keyWidth, owners[0].Typ.Width, "hidden key width derives from the authored expression")
	}
	ddl, _, err := ConstructCreateTableSQL(ctx, table, nil, false, nil)
	require.NoError(t, err)
	require.NoError(t, validateFunctionalTable(t.Context(), table))
	rebuilt, err := issue32437Build(t, ctx, ddl)
	require.NoError(t, err)
	rebuiltTable := rebuilt.GetDdl().GetCreateTable().TableDef
	require.NoError(t, validateFunctionalTable(t.Context(), rebuiltTable))
	rebuiltOwners := functionalIndexColumns(rebuiltTable, rebuiltTable.Indexes[0])
	require.Len(t, rebuiltOwners, 1)
	require.Equal(t, owners[0].Typ.Id, rebuiltOwners[0].Typ.Id, "replay keeps the hidden key type")
	require.Equal(t, owners[0].Typ.Width, rebuiltOwners[0].Typ.Width, "replay keeps the derived width")
	require.Equal(t, owners[0].Typ.Scale, rebuiltOwners[0].Typ.Scale, "replay keeps the scale")
	require.Equal(t, owners[0].GeneratedCol.OriginString, rebuiltOwners[0].GeneratedCol.OriginString,
		"replay keeps the persisted spelling")
	return owners[0]
}

func TestFunctionalIndexScalarAdmission(t *testing.T) {
	ctx := issue32437Context(t)
	for _, tc := range []struct {
		expression string
		keyId      types.T
		keyWidth   int32
	}{
		{"substring(name,1,8)", types.T_varchar, 8},
		{"substr(name,1)", types.T_varchar, 40},
		// The engine keeps the lengthless spellings at the source width: the
		// derived bound is the type-level envelope, not max(0, width-pos).
		{"mid(name,2)", types.T_varchar, 40},
		{"substring(name,pos,2)", types.T_varchar, 2},
		{"left(name,4)", types.T_varchar, 4},
		{"left(name,pos)", types.T_varchar, 40},
		{"reverse(name)", types.T_varchar, 40},
		{"REVERSE(name)", types.T_varchar, 40},
		{"md5(name)", types.T_varchar, 32},
		{"md5(cc)", types.T_varchar, 32},
		{"crc32(name)", types.T_uint64, 0},
		{"crc32(cc)", types.T_uint64, 0},
		{"hex(name)", types.T_varchar, 320},
		{"length(name)", types.T_int64, 0},
		{"concat(name)", types.T_varchar, 40},
		{"concat(name,'x')", types.T_varchar, 41},
		// Authored signed position/count literals bind persisted SQL through
		// unary_minus over the constant; the derived key width follows the
		// same constant-derivation as the positive spellings, while a
		// negated count falls back to the source envelope because the
		// persisted bind keeps the negation as a function node.
		{"substring(name,-2,2)", types.T_varchar, 2},
		{"left(name,-1)", types.T_varchar, 40},
		// SQL text 'a\\b' unescapes to the three-character value containing
		// the backslash: a varchar(40) subject plus three characters is 43.
		{"concat(name,'a\\\\b')", types.T_varchar, 43},
		{"concat(ds,name)", types.T_varchar, 70},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			issue32437Admit(t, ctx, tc.expression, tc.keyId, tc.keyWidth)
		})
	}
}

func TestFunctionalIndexScalarNegativeControls(t *testing.T) {
	ctx := issue32437Context(t)
	for _, tc := range []struct {
		expression string
		contains   string
	}{
		// One formatted scalar argument disqualifies concat.
		{"concat(id,'x')", "functional index expression is not supported"},
		{"concat(doc,'x')", "functional index expression is not supported"},
		// Unknown functions stay out of the audited set.
		{"sha1(name)", "functional index expression is not supported"},
		// Wrong argument domains.
		{"crc32(id)", "functional index expression is not supported"},
		{"length(id)", "functional index expression is not supported"},
		// Deterministic temporal conversions are rejected by coverage with
		// the new message, not mislabeled as session-dependent.
		{"cast(d as char(10))", "functional index expression is not supported"},
		{"cast(dt as varchar(19))", "functional index expression is not supported"},
		// Unsigned position columns fail closed; no new conversion rule
		// covers the unsigned-to-signed wrapper the binder inserts.
		{"substring(name,upos,2)", "functional index expression is not supported"},
		{"left(name,upos)", "functional index expression is not supported"},
		// Negated columns and non-integer negations keep failing closed:
		// only the signed-integer-literal constant form of unary_minus is
		// admitted, so the unsigned-column boundary above stays intact.
		{"substring(name,-pos,2)", "functional index expression is not supported"},
		{"substring(name,-upos,2)", "functional index expression is not supported"},
		{"substring(name,-2.5,1)", "functional index expression is not supported"},
		// An explicit CHAR-to-VARCHAR cast can truncate and stays excluded,
		// both as a standalone head and inside an admitted function.
		{"cast(cc as varchar(2))", "functional index expression is not supported"},
		{"md5(cast(cc as varchar(2)))", "functional index expression is not supported"},
		// Demonstrably session-dependent conversions keep the retained
		// session-independence message.
		{"cast(name as float)", "must have session-independent semantics"},
		{"cast('2020-01-01' as datetime)", "must have session-independent semantics"},
		{"cast(ds as date)", "must have session-independent semantics"},
		{"cast(ds as time)", "must have session-independent semantics"},
		{"cast(ds as timestamp)", "must have session-independent semantics"},
		{"cast(ts as char(19))", "must have session-independent semantics"},
		{"cast(ts as bigint)", "must have session-independent semantics"},
		// Genuinely non-deterministic functions report the generated-column
		// message before eligibility (category 1).
		{"rand()", "non-deterministic"},
		{"upper(now())", "non-deterministic"},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			p, err := issue32437Build(t, ctx, issue32437Schema+",index bad (("+tc.expression+")))")
			require.Error(t, err, "expression must fail closed")
			require.Nil(t, p)
			require.ErrorContains(t, err, tc.contains)
		})
	}
	// CREATE TABLE is atomic: one failing index part fails the whole
	// statement, so a bad sibling can never leave hidden columns behind.
	p, err := issue32437Build(t, ctx, issue32437Schema+",index bad ((sha1(name))),index ok ((md5(name))))")
	require.Error(t, err, "a failing index part fails the whole CREATE")
	require.Nil(t, p)
	require.ErrorContains(t, err, "sha1(name)")
	// The admitted control on its own builds with exactly one hidden column.
	p, err = issue32437Build(t, ctx, issue32437Schema+",index ok ((md5(name))))")
	require.NoError(t, err)
	table := p.GetDdl().GetCreateTable().TableDef
	hidden := 0
	for _, col := range table.Cols {
		if isFunctionalColumn(col) {
			hidden++
		}
	}
	require.Equal(t, 1, hidden, "only the admitted index owns a hidden column")
	require.NoError(t, validateFunctionalTable(t.Context(), table))
}

func TestFunctionalIndexScalarIdentityCastControls(t *testing.T) {
	ctx := issue32437Context(t)
	// Retained-P3 controls: a value-identical explicit cast admits through
	// the identity arm of the walk, while a width-declaring explicit cast
	// stays excluded.
	owner := issue32437Admit(t, ctx, "md5(cast(name as varchar(40)))", types.T_varchar, 32)
	require.NotNil(t, owner)
	_, err := issue32437Build(t, ctx, issue32437Schema+",index bad ((md5(cast(name as varchar(38)))))")
	require.Error(t, err, "a width-changing explicit cast is not value-identical")
	_, err = issue32437Build(t, ctx, issue32437Schema+",index bad ((cast(cc as varchar(10))))")
	require.Error(t, err, "explicit CHAR to VARCHAR stays excluded even with equal width")
}

func TestFunctionalIndexCharSubjectBindings(t *testing.T) {
	ctx := issue32437Context(t)
	for _, tc := range []struct {
		expression string
		adapter    bool
	}{
		{"md5(cc)", true},
		{"reverse(cc)", false},
		{"length(cc)", false},
		{"hex(cc)", false},
		{"substring(cc,1,4)", false},
		{"left(cc,4)", false},
		{"crc32(cc)", false},
		{"concat(cc,'x')", false},
		{"lower(cc)", false},
		{"upper(cc)", false},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			p, err := issue32437Build(t, ctx, issue32437Schema+",index ix (("+tc.expression+")))")
			require.NoError(t, err)
			table := p.GetDdl().GetCreateTable().TableDef
			owner := functionalIndexColumn(table, table.Indexes[0])
			require.NotNil(t, owner)
			fn := owner.GeneratedCol.Expr.GetF()
			require.NotNil(t, fn)
			adapter := fn.Args[0].GetF()
			require.Equal(t, tc.adapter, adapter != nil, "only md5 needs the implicit CHAR-to-VARCHAR adapter")
			if adapter == nil {
				require.NotNil(t, fn.Args[0].GetCol(), "native CHAR subjects bind the column directly")
			} else {
				require.NotNil(t, adapter.Args[0].GetCol())
				target := adapter.Args[1]
				require.NotNil(t, target.GetT())
				require.Equal(t, int32(types.T_char), adapter.Args[0].Typ.Id)
				require.Equal(t, int32(types.T_varchar), fn.Args[0].Typ.Id)
				require.Equal(t, int32(types.MaxVarcharLen), fn.Args[0].Typ.Width)
				require.False(t, adapter.SyntaxExplicitCast, "the adapter is the implicit ordinary cast")
			}
		})
	}
}

func TestFunctionalIndexNumericJSONAdmission(t *testing.T) {
	ctx := issue32437Context(t)
	for _, tc := range []struct {
		spelling string
		keyId    types.T
	}{
		{"int", types.T_int32},
		{"integer", types.T_int32},
		{"int4", types.T_int32},
		{"tinyint", types.T_int8},
		{"int1", types.T_int8},
		{"smallint", types.T_int16},
		{"int2", types.T_int16},
		{"bigint", types.T_int64},
		{"int8", types.T_int64},
		{"tinyint unsigned", types.T_uint8},
		{"smallint unsigned", types.T_uint16},
		{"int unsigned", types.T_uint32},
		{"integer unsigned", types.T_uint32},
		{"bigint unsigned", types.T_uint64},
		{"int zerofill", types.T_int32},
		{"signed", types.T_int64},
		{"signed integer", types.T_int64},
		{"unsigned", types.T_uint64},
		{"unsigned integer", types.T_uint64},
	} {
		t.Run("cast(json_extract(doc,'$.a') as "+tc.spelling+")", func(t *testing.T) {
			issue32437Admit(t, ctx, "cast(json_extract(doc,'$.a') as "+tc.spelling+")", tc.keyId, -1)
		})
	}
	// Per-segment single-location paths beyond the plain key stay admitted.
	for _, path := range []string{"$.a[1]", "$.a[1][2]"} {
		expression := "cast(json_extract(doc,'" + path + "') as int)"
		t.Run(expression, func(t *testing.T) {
			issue32437Admit(t, ctx, expression, types.T_int32, -1)
		})
	}
	// Out-of-surface targets and raw keys stay rejected.
	for _, tc := range []struct {
		expression string
		contains   string
	}{
		{"cast(json_extract(doc,'$.a') as decimal(10,2))", "functional index expression is not supported"},
		{"cast(json_extract(doc,'$.a') as varchar(16))", "functional index expression is not supported"},
		{"json_extract(doc,'$.a')", "functional index expression is not supported"},
		{"doc", "functional index result type"},
		{"doc ->> '$.a'", "functional index expression is not supported"},
		{"cast(doc->>'$.a' as int)", "functional index expression is not supported"},
		{"cast(json_extract(doc,'$.a[*]') as int)", "functional index expression is not supported"},
		{"cast(json_extract(doc,'$.**') as int)", "functional index expression is not supported"},
		{"cast(json_extract(doc,'$.a[*].b') as int)", "functional index expression is not supported"},
		{"cast(json_extract(doc,'$.a[1] to last') as int)", "functional index expression is not supported"},
		{"cast(json_extract(pos,'$.a') as int)", "functional index expression is not supported"},
		{"cast(json_extract(doc,'$.a') as mediumint)", "CAST to MEDIUMINT is not supported"},
		{"cast(json_extract(doc,'$.a') as long)", ""},
		{"md5(doc)", "functional index expression is not supported"},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			_, err := issue32437Build(t, ctx, issue32437Schema+",index bad (("+tc.expression+")))")
			require.Error(t, err)
			if tc.contains != "" {
				require.ErrorContains(t, err, tc.contains)
			}
		})
	}
	// Corrupted metadata for the new class fails the table re-validation.
	p, err := issue32437Build(t, ctx, issue32437Schema+",index ix ((cast(json_extract(doc,'$.a') as int))))")
	require.NoError(t, err)
	table := p.GetDdl().GetCreateTable().TableDef
	bad := DeepCopyTableDef(table, true)
	functionalIndexColumn(bad, bad.Indexes[0]).Typ.Id = int32(types.T_int64)
	require.Error(t, validateFunctionalTable(t.Context(), bad), "tampered hidden key type must fail validation")
}

func TestFunctionalIndexPersistedSpellingCanonicalization(t *testing.T) {
	ctx := issue32437Context(t)
	for _, tc := range []struct {
		expression string
		origin     string
		keyId      types.T
	}{
		// The two mixed family spellings persist canonically so the
		// identical bound target replays losslessly instead of re-binding
		// 32-bit; the pre-existing widening class gets the same repair.
		{"cast(json_extract(doc,'$.a') as signed integer)",
			"cast(json_extract(`doc`, '$.a') as signed)", types.T_int64},
		{"cast(json_extract(doc,'$.a') as unsigned integer)",
			"cast(json_extract(`doc`, '$.a') as unsigned)", types.T_uint64},
		{"cast(id as signed integer)",
			"cast(`id` as signed)", types.T_int64},
		// Every other spelling renders verbatim.
		{"cast(json_extract(doc,'$.a') as signed)",
			"cast(json_extract(`doc`, '$.a') as signed)", types.T_int64},
		{"cast(json_extract(doc,'$.a') as int)",
			"cast(json_extract(`doc`, '$.a') as int)", types.T_int32},
		{"cast(json_extract(doc,'$.a') as bigint unsigned)",
			"cast(json_extract(`doc`, '$.a') as bigint unsigned)", types.T_uint64},
		{"cast(json_extract(doc,'$.a') as int zerofill)",
			"cast(json_extract(`doc`, '$.a') as int zerofill)", types.T_int32},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			owner := issue32437Admit(t, ctx, tc.expression, tc.keyId, -1)
			require.Equal(t, tc.origin, owner.GeneratedCol.OriginString, "persisted spelling")
		})
	}
}

func TestFunctionalIndexNumericJSONMatchPlan(t *testing.T) {
	ctx := issue32437Context(t)
	for _, spelling := range []string{"int", "signed"} {
		t.Run("as "+spelling, func(t *testing.T) {
			p, err := issue32437Build(t, ctx, issue32437Schema+",index ix ((cast(json_extract(doc,'$.a') as "+spelling+"))))")
			require.NoError(t, err)
			table := p.GetDdl().GetCreateTable().TableDef
			ctx.tables["fi"] = table
			ctx.objects["fi"] = &ObjectRef{ObjName: "fi", SchemaName: "tpch"}
			for _, indexTable := range p.GetDdl().GetCreateTable().IndexTables {
				ctx.tables[indexTable.Name] = indexTable
				ctx.objects[indexTable.Name] = &ObjectRef{ObjName: indexTable.Name, SchemaName: "tpch"}
			}
			qp, err := issue32437Build(t, ctx, "select * from fi where cast(json_extract(doc,'$.a') as "+spelling+")=1")
			require.NoError(t, err)
			issue32437ScanShapes(t, qp, table, true)
		})
	}
}

func issue32437ScanShapes(t *testing.T, qp *Plan, table *TableDef, physical bool) {
	t.Helper()
	indexScan, residual := false, false
	for _, n := range qp.GetQuery().Nodes {
		if n.NodeType == pb.Node_TABLE_SCAN && n.TableDef != nil && n.TableDef.Name == "fi" {
			residual = len(n.FilterList) > 0
		}
		if n.NodeType == pb.Node_TABLE_SCAN && n.TableDef != nil && n.TableDef.Name == table.Indexes[0].IndexTableName {
			indexScan = true
		}
	}
	require.True(t, residual, "functional equality always retains the base-row predicate")
	require.Equal(t, physical, indexScan, "physical hidden equality follows the byte-stability rule")
}

// issue32437HiddenKeyEquality reports whether any retained predicate of the
// base-table scan references the index's hidden owner column, i.e. whether
// addFunctionalIndexFilters rewrote the ordinary lookup into a physical
// hidden-key equality in addition to the residual. Whether the cost model
// afterwards selects an index join is a separate decision this helper
// deliberately does not assert.
func issue32437HiddenKeyEquality(t *testing.T, qp *Plan, table *TableDef) bool {
	t.Helper()
	owner := functionalIndexColumn(table, table.Indexes[0])
	require.NotNil(t, owner)
	var hiddenPos int32 = -1
	for i, c := range table.Cols {
		if c.Name == owner.Name {
			hiddenPos = int32(i)
		}
	}
	var references func(*Expr) bool
	references = func(expr *Expr) bool {
		switch e := expr.Expr.(type) {
		case *pb.Expr_Col:
			return e.Col.ColPos == hiddenPos
		case *pb.Expr_F:
			for _, arg := range e.F.Args {
				if references(arg) {
					return true
				}
			}
		}
		return false
	}
	for _, n := range qp.GetQuery().Nodes {
		if n.NodeType == pb.Node_TABLE_SCAN && n.TableDef != nil && n.TableDef.Name == "fi" {
			for _, f := range n.FilterList {
				if references(f) {
					return true
				}
			}
		}
	}
	return false
}

// TestFunctionalIndexOrdinaryLookupRewrites pins the mo-bot-review match
// contract for ordinary literal-equality lookups: admitted substring/left and
// integer/uint64 keys keep gaining the physical hidden-key equality.
//
//   - the query-only prepared-numeric string-domain witness is planner
//     provenance and no longer breaks the structural match (substring and
//     left ordinary lookups);
//   - the persisted unary_minus signed-literal spelling folds into the
//     query-side folded literal, so suffix-position indexes match their own
//     lookups, while the negated-count spelling keeps the source-envelope
//     width on the persisted side and stays residual-only;
//   - a flag-clear integer-to-DECIMAL comparison promotion is stripped when
//     every source value injects exactly into the decimal target, so
//     uint64-keyed and integer-keyed indexes match ordinary numeric-literal
//     comparisons; a user-explicit decimal cast keeps its syntax flag and
//     never matches.
//
// The original base-row predicate always remains as the residual.
func TestFunctionalIndexOrdinaryLookupRewrites(t *testing.T) {
	ctx := issue32437Context(t)
	for _, tc := range []struct {
		expression, query string
		hiddenEQ          bool
	}{
		{"substring(name,1,8)", "select * from fi where substring(name,1,8)='smith123'", true},
		{"left(name,4)", "select * from fi where left(name,4)='smit'", true},
		{"substring(name,-2,2)", "select * from fi where substring(name,-2,2)='ef'", true},
		{"left(name,-1)", "select * from fi where left(name,-1)='g'", false},
		{"crc32(name)", "select * from fi where crc32(name)=123", true},
		{"cast(json_extract(doc,'$.a') as unsigned)", "select * from fi where cast(json_extract(doc,'$.a') as unsigned)=4294967296", true},
		{"cast(json_extract(doc,'$.a') as int)", "select * from fi where cast(json_extract(doc,'$.a') as int)=1.5", true},
		{"crc32(name)", "select * from fi where crc32(name)=18446744073709551616", true},
		{"crc32(name)", "select * from fi where cast(crc32(name) as decimal(38,0))=123", false},
	} {
		t.Run(tc.expression+" | "+tc.query, func(t *testing.T) {
			p, err := issue32437Build(t, ctx, issue32437Schema+",index ix (("+tc.expression+")))")
			require.NoError(t, err)
			table := p.GetDdl().GetCreateTable().TableDef
			require.NoError(t, validateFunctionalTable(t.Context(), table))
			ctx.tables["fi"] = table
			ctx.objects["fi"] = &ObjectRef{ObjName: "fi", SchemaName: "tpch"}
			for _, indexTable := range p.GetDdl().GetCreateTable().IndexTables {
				ctx.tables[indexTable.Name] = indexTable
				ctx.objects[indexTable.Name] = &ObjectRef{ObjName: indexTable.Name, SchemaName: "tpch"}
			}
			qp, err := issue32437Build(t, ctx, tc.query)
			require.NoError(t, err)
			residual := false
			for _, n := range qp.GetQuery().Nodes {
				if n.NodeType == pb.Node_TABLE_SCAN && n.TableDef != nil && n.TableDef.Name == "fi" &&
					len(n.FilterList) > 0 {
					residual = true
				}
			}
			require.True(t, residual, "the ordinary lookup always keeps its base-row predicate")
			require.Equal(t, tc.hiddenEQ, issue32437HiddenKeyEquality(t, qp, table),
				"the hidden-key equality follows the structural match")
		})
	}
}

func TestFunctionalIndexByteStabilityMatchRules(t *testing.T) {
	ctx := issue32437Context(t)
	for _, tc := range []struct {
		expression, query string
		physical          bool
	}{
		// Byte-stable keys keep the physical hidden-column equality:
		// integer or VARCHAR keys with no T_char column reference.
		{"md5(name)", "select * from fi where md5(name)='x'", true},
		{"concat(name,'x')", "select * from fi where concat(name,'x')='x'", true},
		{"cast(json_extract(doc,'$.a') as int)", "select * from fi where cast(json_extract(doc,'$.a') as int)=1", true},
		{"lower(name)", "select * from fi where lower(name)='x'", true},
		{"length(name)", "select * from fi where length(name)=10", true},
		// CHAR subjects are DDL-admitted and residual-only at match time,
		// including the pre-existing lower/upper over CHAR class.
		{"md5(cc)", "select * from fi where md5(cc)='x'", false},
		{"lower(cc)", "select * from fi where lower(cc)='x'", false},
		{"crc32(cc)", "select * from fi where crc32(cc)=1", false},
		{"length(cc)", "select * from fi where length(cc)=1", false},
		// A T_char hidden key (the explicit AS CHAR(n) JSON text form) is
		// residual-only like every other CHAR-keyed index.
		{"cast(doc->>'$.a' as char(20))", "select * from fi where cast(doc->>'$.a' as char(20))='x'", false},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			p, err := issue32437Build(t, ctx, issue32437Schema+",index ix (("+tc.expression+")))")
			require.NoError(t, err)
			table := p.GetDdl().GetCreateTable().TableDef
			require.NoError(t, validateFunctionalTable(t.Context(), table))
			ctx.tables["fi"] = table
			ctx.objects["fi"] = &ObjectRef{ObjName: "fi", SchemaName: "tpch"}
			for _, indexTable := range p.GetDdl().GetCreateTable().IndexTables {
				ctx.tables[indexTable.Name] = indexTable
				ctx.objects[indexTable.Name] = &ObjectRef{ObjName: indexTable.Name, SchemaName: "tpch"}
			}
			qp, err := issue32437Build(t, ctx, tc.query)
			require.NoError(t, err)
			issue32437ScanShapes(t, qp, table, tc.physical)
		})
	}
}

func TestFunctionalIndexDecimalPromotionCapacity(t *testing.T) {
	decimal := func(width, scale int32) Type {
		return Type{Id: int32(types.T_decimal128), Width: width, Scale: scale}
	}
	// uint64 (widest value 18446744073709551615, 20 digits) is covered by
	// DECIMAL128(38,0) and by the minimal 20-digit envelope, never by
	// decimal64's 18-digit ceiling nor by a 19-digit envelope.
	require.True(t, decimalPromotionCoversSource(decimal(38, 0), types.T_uint64))
	require.True(t, decimalPromotionCoversSource(decimal(20, 0), types.T_uint64))
	require.False(t, decimalPromotionCoversSource(decimal(19, 0), types.T_uint64))
	require.False(t, decimalPromotionCoversSource(Type{Id: int32(types.T_decimal64), Width: 18, Scale: 0}, types.T_uint64))
	// int32 (10 signed digits) covers the probe-verified DECIMAL128(11,1);
	// scale consumes integer-digit capacity, so (10,1) is lossy.
	require.True(t, decimalPromotionCoversSource(decimal(11, 1), types.T_int32))
	require.False(t, decimalPromotionCoversSource(decimal(10, 1), types.T_int32))
	// int8 (3 signed digits) is the smallest admitted boundary.
	require.True(t, decimalPromotionCoversSource(decimal(4, 0), types.T_int8))
	require.False(t, decimalPromotionCoversSource(decimal(2, 0), types.T_int8))
	// int64 (19 signed digits) sits between the unsigned extremes.
	require.True(t, decimalPromotionCoversSource(decimal(19, 0), types.T_int64))
	require.False(t, decimalPromotionCoversSource(decimal(18, 0), types.T_int64))
}

func TestFunctionalIndexIdentifierRewritePersistence(t *testing.T) {
	// RENAME COLUMN and the renaming half of CHANGE COLUMN rebuild the
	// persisted text through renameFunctionalColumnDependencies (the ALTER
	// layer's functional-rewrite step); the literal spelling class and the
	// hidden key type must survive the rewrite.
	t.Run("x-form literal through rename keeps its class", func(t *testing.T) {
		ctx := issue32437Context(t)
		p, err := issue32437Build(t, ctx, issue32437Schema+",index ix ((concat(name,'a\\\\b'))))")
		require.NoError(t, err)
		table := p.GetDdl().GetCreateTable().TableDef
		owner := functionalIndexColumn(table, table.Indexes[0])
		require.Equal(t, "concat(`name`, _utf8mb4 X'615c62')", owner.GeneratedCol.OriginString)
		require.Equal(t, int32(types.T_varchar), owner.Typ.Id)
		require.Equal(t, int32(43), owner.Typ.Width)
		require.NoError(t, renameFunctionalColumnDependencies(t.Context(), table, "name", "renameto"))
		// The ALTER layer renames the ColumnDef after the functional
		// rewrite, before any COPY reconstruction runs.
		col := FindColumn(table.Cols, "name")
		col.Name, col.OriginName = "renameto", "renameto"
		rewritten := functionalIndexColumn(table, table.Indexes[0])
		require.Equal(t, "concat(`renameto`, _utf8mb4 X'615c62')", rewritten.GeneratedCol.OriginString,
			"the identifier-rewrite render shares the width-preserving serialization")
		require.Equal(t, int32(types.T_varchar), rewritten.Typ.Id)
		require.Equal(t, int32(43), rewritten.Typ.Width, "the rewritten text re-binds the authored key width")
		require.NoError(t, validateFunctionalTable(t.Context(), table))
		ddl, _, err := ConstructCreateTableSQL(ctx, table, nil, false, nil)
		require.NoError(t, err)
		require.Contains(t, ddl, "concat(`renameto`, _utf8mb4 X'615c62')")
		rebuilt, err := issue32437Build(t, ctx, ddl)
		require.NoError(t, err)
		rebuiltTable := rebuilt.GetDdl().GetCreateTable().TableDef
		require.NoError(t, validateFunctionalTable(t.Context(), rebuiltTable))
		require.Equal(t, int32(43), functionalIndexColumn(rebuiltTable, rebuiltTable.Indexes[0]).Typ.Width)
	})
	t.Run("plain literal rewrites exactly", func(t *testing.T) {
		ctx := issue32437Context(t)
		p, err := issue32437Build(t, ctx, issue32437Schema+",index ix ((concat(name,'abc'))))")
		require.NoError(t, err)
		table := p.GetDdl().GetCreateTable().TableDef
		require.NoError(t, renameFunctionalColumnDependencies(t.Context(), table, "name", "renameto"))
		col := FindColumn(table.Cols, "name")
		col.Name, col.OriginName = "renameto", "renameto"
		rewritten := functionalIndexColumn(table, table.Indexes[0])
		require.Equal(t, "concat(`renameto`, 'abc')", rewritten.GeneratedCol.OriginString)
		require.Equal(t, int32(43), rewritten.Typ.Width)
		require.NoError(t, validateFunctionalTable(t.Context(), table))
	})
	t.Run("old-form widthless text keeps its spelling class", func(t *testing.T) {
		// Model catalog text written by an older binary: the widthless
		// binary-cast literal persisted for a pre-feature backslash index.
		// Its spelling and binding class must stay stable through the
		// rewrite, exactly as on base; the rewrite does not migrate the
		// class this feature no longer writes.
		ctx := issue32437Context(t)
		p, err := issue32437Build(t, ctx, issue32437Schema+",index ix ((concat(name,'a\\\\b'))))")
		require.NoError(t, err)
		table := p.GetDdl().GetCreateTable().TableDef
		require.NoError(t, validateFunctionalTable(t.Context(), table))
		owner := functionalIndexColumn(table, table.Indexes[0])
		owner.GeneratedCol.OriginString = "concat(`name`, cast(0x615c62 as varchar))" // older binary's stored text
		require.NoError(t, renameFunctionalColumnDependencies(t.Context(), table, "name", "renameto"),
			"a pre-feature rename keeps base behavior")
		rewritten := functionalIndexColumn(table, table.Indexes[0])
		require.Equal(t, "concat(`renameto`, cast(0x615c62 as varchar))", rewritten.GeneratedCol.OriginString,
			"the widthless spelling re-renders verbatim in its own class")
		require.Equal(t, int32(types.T_text), rewritten.GeneratedCol.Expr.Typ.Id,
			"the rewritten text keeps the binding class of the text it replaces")
		require.Error(t, validateFunctionalTable(t.Context(), table),
			"base left the old text re-binding TEXT against the authored varchar key; renaming keeps that divergence, never migrates it")
	})
	t.Run("numeric json key through rename", func(t *testing.T) {
		ctx := issue32437Context(t)
		p, err := issue32437Build(t, ctx, issue32437Schema+",index ix ((cast(json_extract(doc,'$.a') as signed integer))))")
		require.NoError(t, err)
		table := p.GetDdl().GetCreateTable().TableDef
		require.Equal(t, int32(types.T_int64), functionalIndexColumn(table, table.Indexes[0]).Typ.Id)
		require.NoError(t, renameFunctionalColumnDependencies(t.Context(), table, "doc", "renamed_doc"))
		col := FindColumn(table.Cols, "doc")
		col.Name, col.OriginName = "renamed_doc", "renamed_doc" // the ALTER layer updates both names
		rewritten := functionalIndexColumn(table, table.Indexes[0])
		require.Equal(t, "cast(json_extract(`renamed_doc`, '$.a') as signed)", rewritten.GeneratedCol.OriginString)
		require.Equal(t, int32(types.T_int64), rewritten.Typ.Id, "the rewrite keeps the bound key type")
		require.NoError(t, validateFunctionalTable(t.Context(), table))
		ddl, _, err := ConstructCreateTableSQL(ctx, table, nil, false, nil)
		require.NoError(t, err)
		require.Contains(t, ddl, "cast(json_extract(`renamed_doc`, '$.a') as signed)")
		rebuilt, err := issue32437Build(t, ctx, ddl)
		require.NoError(t, err)
		rebuiltTable := rebuilt.GetDdl().GetCreateTable().TableDef
		require.NoError(t, validateFunctionalTable(t.Context(), rebuiltTable))
		require.Equal(t, int32(types.T_int64), functionalIndexColumn(rebuiltTable, rebuiltTable.Indexes[0]).Typ.Id)
	})
}

func TestFunctionalIndexRewriteRoundTripGuard(t *testing.T) {
	ctx := issue32437Context(t)
	p, err := issue32437Build(t, ctx, issue32437Schema+",index ix ((concat(name,'abc'))))")
	require.NoError(t, err)
	table := p.GetDdl().GetCreateTable().TableDef
	require.NoError(t, validateFunctionalTable(t.Context(), table))
	names := make([]string, len(table.Cols))
	typs := make([]Type, len(table.Cols))
	for i, col := range table.Cols {
		names[i], typs[i] = col.Name, col.Typ
	}
	owner := functionalIndexColumn(table, table.Indexes[0])
	previous := owner.GeneratedCol.OriginString
	bindText := func(text string) *Expr {
		t.Helper()
		ast, err := parseFunctionalIndexOriginString(t.Context(), text)
		require.NoError(t, err, text)
		bound, err := NewGeneratedColBinder(t.Context(), names, typs).BindExpr(ast, 0, true)
		require.NoError(t, err, text)
		return bound
	}
	// A rewritten text that re-binds a different key type fails the ALTER
	// closed instead of replacing the persisted text.
	degraded := &pb.GeneratedCol{Expr: owner.GeneratedCol.Expr, OriginString: "concat(`name`, 'ab')"}
	require.Error(t, verifyFunctionalIndexRewriteRoundTrip(t.Context(), table, names, names, typs, previous, degraded),
		"a degraded re-render fails the ALTER")
	// The base regression this fix removes: reverting the width-preserving
	// hex-introducer spelling to the widthless cast re-binds a TEXT key, so
	// the guard fails that rewrite instead of corrupting later replays.
	revertedBind := bindText("concat(`name`, cast(0x615c62 as varchar))")
	require.NotEqual(t, owner.Typ.Id, revertedBind.Typ.Id, "the widthless spelling re-binds a degraded key")
	reverted := &pb.GeneratedCol{Expr: revertedBind, OriginString: "concat(`name`, cast(0x615c62 as varchar))"}
	require.Error(t, verifyFunctionalIndexRewriteRoundTrip(t.Context(), table, names, names, typs, "concat(`name`, _utf8mb4 X'615c62')", reverted),
		"reverting to the widthless spelling fails the ALTER")
	// A rewrite whose text re-binds the same key type but stops re-admitting
	// fails closed as well: a signed position column admits, the unsigned
	// position column binds the same width but is rejected by the walk.
	unadmittedText := "substring(`name`, upos, 2)"
	unadmittedBind := bindText(unadmittedText)
	require.False(t, functionalExpressionEligible(unadmittedBind, table.Cols, make(map[int32]bool)))
	require.True(t, functionalExpressionEligible(bindText("substring(`name`, pos, 2)"), table.Cols, make(map[int32]bool)))
	unadmitted := &pb.GeneratedCol{Expr: unadmittedBind, OriginString: unadmittedText}
	require.Error(t, verifyFunctionalIndexRewriteRoundTrip(t.Context(), table, names, names, typs, "substring(`name`, pos, 2)", unadmitted),
		"an unadmitting re-render fails the ALTER")
	// The faithful rendering passes through the shared producer.
	faithful := &pb.GeneratedCol{Expr: owner.GeneratedCol.Expr, OriginString: "concat(`name`, 'abc')"}
	require.NoError(t, verifyFunctionalIndexRewriteRoundTrip(t.Context(), table, names, names, typs, previous, faithful))
	// Older binaries' widthless texts keep their binding class: their
	// pre-rename re-bind does not admit either, so the rewrite of the same
	// class passes every clause and the rename keeps base behavior.
	oldText := "concat(`name`, cast(0x615c62 as varchar))"
	oldBind := bindText(oldText)
	require.False(t, functionalExpressionEligible(oldBind, table.Cols, make(map[int32]bool)))
	oldForm := &pb.GeneratedCol{Expr: oldBind, OriginString: oldText}
	require.NoError(t, verifyFunctionalIndexRewriteRoundTrip(t.Context(), table, names, names, typs, oldText, oldForm),
		"an older binary's widthless text keeps its class through the rewrite")
}

func TestFunctionalIndexPredicatedDiagnostics(t *testing.T) {
	ctx := issue32437Context(t)
	for _, tc := range []struct {
		expression string
		message    string
	}{
		// Category 3, coverage rejections, name the raw SQL text; the
		// bounded text cast with a valid path admits instead. The message
		// rendering joins expression-list items with "," (no space) so the
		// named text keeps the authored word shape of multi-argument calls.
		{"json_extract(doc,'$.kind')",
			"not supported: functional index expression is not supported: json_extract(doc,'$.kind')"},
		{"doc ->> '$.kind'",
			"not supported: functional index expression is not supported: json_unquote(json_extract(doc,'$.kind'))"},
		{"cast(d as char(10))",
			"not supported: functional index expression is not supported: cast(d as char(10))"},
		{"cast(doc ->> '$[4294967296]' as char(20))",
			"not supported: functional index expression is not supported: cast(json_unquote(json_extract(doc,'$[4294967296]')) as char(20))"},
		{"cast(doc ->> '$[999999999999999999999999999999999999]' as char(20))",
			"not supported: functional index expression is not supported: cast(json_unquote(json_extract(doc,'$[999999999999999999999999999999999999]')) as char(20))"},
		// Multi-argument comma class: the message must contain the authored
		// spelling of the argument list (challenge case
		// retained-4-wrong-arity-error: authored ((CONCAT(v,1))) failed while
		// the message rendered `CONCAT(v, 1)`).
		{"concat(name,1)",
			"not supported: functional index expression is not supported: concat(name,1)"},
		{"replace(name,'a','b')",
			"not supported: functional index expression is not supported: replace(name,'a','b')"},
		{"substring(name,upos,2)",
			"not supported: functional index expression is not supported: substring(name,upos,2)"},
		// Bind-stage resolution failure with multi-argument spelling: a
		// no-match arity call names the authored list too.
		{"substring(name,1,2,3)",
			"not supported: functional index expression is not supported: substring(name,1,2,3)"},
		// Bind-stage resolution failures are coverage rejections as well: a
		// wrong-arity call never reaches the eligibility walk, so the
		// authored-expression bind failure must surface as the category-3
		// message naming the raw text instead of leaking the raw registry
		// error (challenge case unsupported_and_error_families: CREATE INDEX
		// ((SUBSTRING(v))) previously escaped as `invalid argument function
		// substring, bad value [VARCHAR]`).
		{"substring(name)",
			"not supported: functional index expression is not supported: substring(name)"},
		{"left(name)",
			"not supported: functional index expression is not supported: left(name)"},
		// Category 2, demonstrated session dependence, keeps the retained
		// message.
		{"cast(ts as char(19))",
			"not supported: functional index expression must have session-independent semantics"},
		{"cast(name as float)",
			"not supported: functional index expression must have session-independent semantics"},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			_, err := issue32437Build(t, ctx, issue32437Schema+",index bad (("+tc.expression+")))")
			require.Error(t, err)
			require.EqualError(t, err, tc.message)
		})
	}
	// The retained NYI cast target keeps its native binder error: the
	// resolution-failure mapping must never absorb a non-resolution bind
	// error into the coverage family (MEDIUMINT/INT3 contract).
	_, nyErr := issue32437Build(t, ctx, issue32437Schema+",index bad ((cast(pos as mediumint))))")
	require.Error(t, nyErr)
	require.ErrorContains(t, nyErr, "CAST to MEDIUMINT is not supported")
	require.False(t, moerr.IsMoErrCode(nyErr, moerr.ErrInvalidArg),
		"the NYI cast target is not a resolution failure")
}

func TestFunctionalIndexVolatileDiagnosticsBeforeEligibility(t *testing.T) {
	ctx := issue32437Context(t)
	// rand()/now() fail closed with the generated-column message that names
	// the offending function, before the eligibility walk runs; the base
	// behavior reported these as session-dependent (probe-base-run.log).
	for _, tc := range []struct {
		expression, fnName string
	}{
		{"rand()", "rand"},
		{"now()", "now"},
		{"concat(name,rand())", "rand"},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			_, err := issue32437Build(t, ctx, issue32437Schema+",index bad (("+tc.expression+")))")
			require.Error(t, err)
			require.ErrorContains(t, err, "non-deterministic function '"+tc.fnName+"'")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
		})
	}
}
