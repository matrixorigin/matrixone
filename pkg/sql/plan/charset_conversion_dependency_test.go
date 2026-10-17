// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestCharsetConversionRetainsUnaffectedSubtrees(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion118)
	for _, tc := range []struct{ name, sourceType, resultType, expression string }{
		{"concat", "varchar(8)", "varchar(64)", "concat(v,a/b)"},
		{"char_dependency", "char(8)", "varchar(64)", "concat(v,a/b)"},
		{"inner_authored_cast", "varchar(8)", "varchar(64)", "concat(v,cast(a/b as char(32)))"},
		{"outer_authored_cast", "varchar(8)", "varchar(64)", "cast(concat(v,a/b) as char(32))"},
		{"numeric_boundary", "varchar(8)", "varchar(64)", "cast(v as decimal(10,2))/b"},
		{"numeric_generated", "varchar(8)", "decimal(30,12)", "length(v)+a/b"},
		{"comparison", "varchar(8)", "varchar(64)", "concat(if(v='a','yes','no'),a/b)"},
		{"list_comparison", "varchar(8)", "varchar(64)", "concat(if('a' in (v,'x'),'yes','no'),a/b)"},
		{"authored_collate", "varchar(8)", "varchar(64)", "concat(v collate utf8mb4_bin='a',a/b)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
			compiler.tables["t"] = &planpb.TableDef{Name: "t"}
			build := func(increment int64, sql string) *planpb.Plan {
				t.Helper()
				stmt, err := mysql.ParseOne(t.Context(), sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				p, err := BuildPlan(divPrecisionCompilerContext{CompilerContext: compiler, increment: increment}, stmt, false)
				require.NoError(t, err)
				return p
			}
			source := build(10, "create table t(v "+tc.sourceType+",a decimal(10,2),b decimal(10,2),g "+tc.resultType+" generated always as("+tc.expression+") stored) collate utf8mb4_bin").GetDdl().GetCreateTable().TableDef
			source.Name2ColIndex = make(map[string]int32, len(source.Cols))
			for i, col := range source.Cols {
				col.ColId = uint64(i + 1)
				source.Name2ColIndex[col.Name] = int32(i)
			}
			compiler.tables["t"] = source
			compiler.objects["t"] = &ObjectRef{SchemaName: "tpch", ObjName: "t"}
			before := proto.Clone(source)
			target := build(0, "alter table t convert to character set utf8mb4 collate utf8mb4_general_ci").GetDdl().GetAlterTable().CopyTableDef
			replay := ddlReplayForTable(WithPersistedDDLReplay(t.Context(), source, target), target.Name)
			require.NotNil(t, replay.columns["g"].generated)
			var division func(*planpb.Expr) *planpb.Expr
			division = func(expr *planpb.Expr) *planpb.Expr {
				if f := expr.GetF(); f != nil {
					if f.Func.GetObjName() == "/" {
						return expr
					}
					for _, arg := range f.Args {
						if found := division(arg); found != nil {
							return found
						}
					}
				}
				return nil
			}
			oldDivision := division(FindColumn(source.Cols, "g").GeneratedCol.Expr)
			newDivision := division(replay.columns["g"].generated.Expr)
			require.NotNil(t, oldDivision)
			require.NotNil(t, newDivision)
			// Numeric result/overload policy survives even when a changed text child
			// is separated from it by an authored numeric CAST boundary.
			require.True(t, proto.Equal(&oldDivision.Typ, &newDivision.Typ))
			require.Equal(t, oldDivision.GetF().Func, newDivision.GetF().Func)
			if tc.name != "numeric_boundary" {
				require.True(t, proto.Equal(oldDivision, newDivision))
			}
			if tc.name == "concat" {
				modified := build(0, "alter table t modify column g varchar(64) generated always as(concat(v,a/b)) stored").GetDdl().GetAlterTable().CopyTableDef
				require.NotEqual(t, oldDivision.Typ.Scale, division(FindColumn(modified.Cols, "g").GeneratedCol.Expr).Typ.Scale, "ordinary authored MODIFY must still use the current session")
			}
			if tc.name == "inner_authored_cast" || tc.name == "outer_authored_cast" {
				var authored func(*planpb.Expr) *planpb.Expr
				authored = func(expr *planpb.Expr) *planpb.Expr {
					if f := expr.GetF(); f != nil {
						if f.Func.GetObjName() == "cast" && expr.Typ.Width == 32 {
							return expr
						}
						for _, arg := range f.Args {
							if found := authored(arg); found != nil {
								return found
							}
						}
					}
					return nil
				}
				oldCast := authored(FindColumn(source.Cols, "g").GeneratedCol.Expr)
				newCast := authored(replay.columns["g"].generated.Expr)
				require.NotNil(t, oldCast)
				require.NotNil(t, newCast)
				require.True(t, proto.Equal(&oldCast.Typ, &newCast.Typ), "authored CAST target is not the destination assignment")
			}
			if tc.name == "authored_collate" {
				var explicit func(*planpb.Expr) *planpb.Expr
				explicit = func(expr *planpb.Expr) *planpb.Expr {
					if expr == nil {
						return nil
					}
					if expr.Typ.CollationCoercibilitySet && expr.Typ.CollationCoercibility == 0 {
						return expr
					}
					if f := expr.GetF(); f != nil {
						for _, arg := range f.Args {
							if found := explicit(arg); found != nil {
								return found
							}
						}
					}
					return nil
				}
				oldCollate, newCollate := explicit(FindColumn(source.Cols, "g").GeneratedCol.Expr), explicit(replay.columns["g"].generated.Expr)
				require.NotNil(t, oldCollate)
				require.NotNil(t, newCollate, "EXPLICIT COLLATE is an authored boundary even with CAST0")
				require.True(t, proto.Equal(&oldCollate.Typ, &newCollate.Typ))
			}
			var refs func(*planpb.Expr)
			refs = func(expr *planpb.Expr) {
				if ref := expr.GetCol(); ref != nil && ref.ColPos == 0 {
					require.Equal(t, FindColumn(target.Cols, "v").Typ.Charset, expr.Typ.Charset)
				}
				if f := expr.GetF(); f != nil {
					for _, arg := range f.Args {
						refs(arg)
					}
				}
			}
			refs(replay.columns["g"].generated.Expr)
			require.True(t, proto.Equal(before, source), "catalog source must remain immutable")
			if tc.name == "concat" {
				current := target
				var nodes func(*planpb.Expr) int
				nodes = func(expr *planpb.Expr) int {
					if expr == nil {
						return 0
					}
					n := 1
					if f := expr.GetF(); f != nil {
						for _, a := range f.Args {
							n += nodes(a)
						}
					}
					return n
				}
				bound := nodes(FindColumn(source.Cols, "g").GeneratedCol.Expr) + 4
				for i := 0; i < 12; i++ {
					current = DeepCopyTableDef(current, true)
					current.Name = "t"
					compiler.tables["t"] = current
					domain := []string{"utf8mb4_unicode_ci", "utf8mb4_bin", "utf8mb4_general_ci"}[i%3]
					current = build(0, "alter table t convert to character set utf8mb4 collate "+domain).GetDdl().GetAlterTable().CopyTableDef
					value := FindColumn(current.Cols, "g").GeneratedCol.Expr
					require.LessOrEqual(t, nodes(value), bound, "repeated conversion must not accumulate implicit character coercions")
				}
			}
		})
	}
}

// Defensive internal-domain control: failed typed adaptation must remain an
// error rather than allowing CONVERT to re-author an unrelated computation.
func TestPersistedDDLReplayCharacterAdaptationError(t *testing.T) {
	typ := planpb.Type{Id: int32(types.T_varchar), Width: 8, Charset: 2}
	ref := &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	expr := &planpb.Expr{Typ: typ, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &ObjectRef{ObjName: "missing_persisted_function"}, Args: []*planpb.Expr{ref}}}}
	source := &planpb.TableDef{Name: "t", Cols: []*planpb.ColDef{{ColId: 1, Name: "v", Typ: typ}, {ColId: 2, Name: "g", Typ: typ, GeneratedCol: &planpb.GeneratedCol{Expr: expr}}}}
	target := DeepCopyTableDef(source, true)
	target.Cols[0].Typ.Charset = 3
	before := proto.Clone(source)
	replay := ddlReplayForTable(WithPersistedDDLReplay(t.Context(), source, target), target.Name)
	require.Error(t, replay.err)
	require.Nil(t, replay.columns["g"].generated)
	require.True(t, proto.Equal(before, source))
}

func TestCharsetConversionCharacterCoercionBoundaries(t *testing.T) {
	ctx := t.Context()
	number := makePlan2Int64ConstExprWithType(12345)
	target := planpb.Type{Id: int32(types.T_varchar), Width: 8, Charset: 2}
	cast := func(input *planpb.Expr, typ planpb.Type) *planpb.Expr {
		value, err := appendCastBeforeExpr(ctx, input, typ)
		require.NoError(t, err)
		return value
	}
	inner := cast(number, target)
	native := target
	native.Charset, native.CollationVersion = 10, 1
	outer := cast(inner, native)
	before := proto.Clone(outer)
	refreshed, err := refreshedPersistedCharacterCoercion(ctx, outer)
	require.NoError(t, err)
	require.True(t, proto.Equal(number, refreshed.GetF().Args[0]), "only equal-width implicit layers may collapse")
	require.Equal(t, target.Width, refreshed.Typ.Width)
	require.EqualValues(t, 5, refreshed.Typ.CollationCoercibility)
	require.True(t, refreshed.Typ.CollationCoercibilitySet)
	require.True(t, proto.Equal(before, outer), "coercion refresh must not mutate its source")

	narrow := target
	narrow.Width = 2
	narrowed := cast(number, narrow)
	refreshed, err = refreshedPersistedCharacterCoercion(ctx, cast(narrowed, native))
	require.NoError(t, err)
	require.True(t, proto.Equal(narrowed, refreshed.GetF().Args[0]), "a narrower inner target must not disappear")
	for _, name := range []string{"authored", "collate", "explicit_overload", "assignment", "numeric"} {
		t.Run(name, func(t *testing.T) {
			var boundary *planpb.Expr
			var err error
			switch name {
			case "authored":
				boundary, err = appendSyntaxExplicitCastBeforeExpr(ctx, number, target)
			case "collate":
				explicit := target
				explicit.CollationCoercibility, explicit.CollationCoercibilitySet = 0, true
				boundary, err = appendCastBeforeExpr(ctx, number, explicit)
			case "explicit_overload":
				boundary, err = appendExplicitCastBeforeExpr(ctx, number, target)
			case "assignment":
				boundary, err = forceAssignmentCastExprWithName(ctx, number, target, "cast_strict")
			case "numeric":
				boundary, err = appendCastBeforeExpr(ctx, number, planpb.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 2})
			}
			require.NoError(t, err)
			before := proto.Clone(boundary)
			retained, err := refreshedPersistedCharacterCoercion(ctx, boundary)
			require.NoError(t, err)
			require.True(t, proto.Equal(before, retained), "authored, numeric, and assignment boundaries are not implicit string coercions")
		})
	}
	list := &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{nil, DeepCopyExpr(outer)}}}}
	refreshed, err = refreshedPersistedCharacterCoercion(ctx, list)
	require.NoError(t, err)
	require.Nil(t, refreshed.GetList().List[0])
	require.True(t, proto.Equal(number, refreshed.GetList().List[1].GetF().Args[0]))
}
