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
	"context"
	stdcrc32 "hash/crc32"
	"strings"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func legacyCRC32Expr() *planpb.Expr {
	return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.CRC32, function.CRC32LegacyOverload), ObjName: "crc32"},
		Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_json)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0, Name: "j"}}}},
	}}}
}

func TestCRC32CopyRetainsBoundGeneratedIdentity(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := planpb.Type{Id: int32(types.T_uint64)}
	source := &planpb.ColDef{Name: "c", Typ: typ, GeneratedCol: &planpb.GeneratedCol{Expr: legacyCRC32Expr(), OriginString: "crc32(j)", IsStored: true}}
	owner := &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "j", Typ: planpb.Type{Id: int32(types.T_json)}}, source}}
	stmt, err := mysql.ParseOne(proc.Ctx, "create table t(j json,c bigint unsigned generated always as (crc32(j)) stored)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	col := stmt.(*tree.CreateTable).Defs[1].(*tree.ColumnTableDef)
	proc.Ctx = context.WithValue(proc.Ctx, defines.CRC32CopyExpressionsKey{}, owner)
	got, err := buildGeneratedExpr(proc.Ctx, col, typ, owner.Cols, proc)
	require.NoError(t, err)
	require.True(t, containsLegacyCRC32(got.Expr))
	got.Expr.GetF().Func.Obj = function.EncodeOverloadID(function.CRC32, function.CRC32JSONTextOverload)
	require.True(t, containsLegacyCRC32(source.GeneratedCol.Expr), "copy must not mutate source catalog")
	for _, attr := range col.Attributes {
		if generated, ok := attr.(*tree.AttributeGeneratedAlways); ok {
			preserved, handled, err := preserveLegacyCRC32Generated(proc.Ctx, source, generated, typ)
			require.NoError(t, err)
			require.True(t, handled)
			require.True(t, containsLegacyCRC32(preserved.Expr))
			changed := typ
			changed.Id = int32(types.T_int64)
			_, handled, err = preserveLegacyCRC32Generated(proc.Ctx, source, generated, changed)
			require.True(t, handled)
			require.ErrorContains(t, err, "explicit table rebuild")
		}
	}

	proc.Ctx = context.Background()
	got, err = buildGeneratedExpr(proc.Ctx, col, typ, owner.Cols, proc)
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(got)
	require.NoError(t, err)
	require.True(t, features.CRC32JSONTextBytes, "new DDL opts into text semantics")
}

func TestCRC32PreparedAndLegacyBinding(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "prepare crc32_stmt from 'select crc32(?)'")
	require.NoError(t, err)
	original := prepared.GetDcl().GetPrepare().Plan
	for _, tc := range []struct {
		value string
		typ   types.Type
	}{
		{"hello", types.T_varchar.ToType()}, {"12", types.T_int64.ToType()},
	} {
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original, []any{ParamValue{Value: tc.value, SourceType: tc.typ, HasSourceType: true}})
		require.NoError(t, err)
		expr := findPlanFunctionExpr(filled, "crc32")
		require.NotNil(t, expr)
		require.Equal(t, function.EncodeOverloadID(function.CRC32, function.CRC32LegacyOverload), expr.GetF().Func.Obj,
			"prepared non-JSON values must select the ordinary byte identity")
		func() {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			exec, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer exec.Free()
			result, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, uint64(stdcrc32.ChecksumIEEE([]byte(tc.value))), vector.MustFixedColNoTypeCheck[uint64](result)[0])
		}()
	}
	legacy := legacyCRC32Expr()
	rule := NewResetParamRefRule(context.Background(), nil)
	rebound, err := rule.ApplyExpr(DeepCopyExpr(legacy))
	require.NoError(t, err)
	require.True(t, containsLegacyCRC32(rebound))
	// Generated DML can substitute a marker below the catalog function. That
	// forces the ordinary parameter binder to revisit this exact function.
	for _, resultType := range []types.T{types.T_uint32, types.T_uint64} {
		withParam := legacyCRC32Expr()
		withParam.Typ.Id = int32(resultType)
		withParam.GetF().Args[0] = &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
		rule = NewResetParamRefRule(context.Background(), []*Expr{legacy.GetF().Args[0]})
		rebound, err = rule.ApplyExpr(withParam)
		require.NoError(t, err)
		require.True(t, containsLegacyCRC32(rebound))
		require.Equal(t, int32(resultType), rebound.Typ.Id)
	}
}

func TestCRC32LegacyGeneratedDMLPlans(t *testing.T) {
	for _, sql := range []string{
		`insert into constraint_test.t_on_update_gen(id,val,updated_at) values(1,'{"t1":"a"}',null)`,
		`insert into constraint_test.t_on_update_gen(id,val,updated_at) select 2,val,updated_at from constraint_test.t_on_update_gen`,
		`update constraint_test.t_on_update_gen set val='{"t1":"b"}' where id=1`,
		`replace into constraint_test.t_on_update_gen(id,val,updated_at) values(1,'{"t1":"a"}',null)`,
		`insert into constraint_test.t_on_update_gen(id,val,updated_at) values(1,'{"t1":"a"}',null) on duplicate key update val=values(val)`,
	} {
		t.Run(sql, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			base := mock.ctxt.tables["t_on_update_gen"]
			sourcePos := mockTableColPos(t, base, "val")
			generatedPos := mockTableColPos(t, base, "g")
			base.Cols[sourcePos].Typ = planpb.Type{Id: int32(types.T_json)}
			base.Cols[generatedPos].Typ = planpb.Type{Id: int32(types.T_uint64)}
			expr := legacyCRC32Expr()
			expr.GetF().Args[0].GetCol().ColPos = sourcePos
			expr.GetF().Args[0].GetCol().Name = "val"
			base.Cols[generatedPos].GeneratedCol = &planpb.GeneratedCol{Expr: expr, OriginString: "crc32(val)", IsStored: true}
			base.Cols[mockTableColPos(t, base, "updated_at")].OnUpdate = nil
			built, err := runOneStmt(mock, t, sql)
			require.NoError(t, err)
			features, err := planpb.RequiredRemoteExpressionFeatures(built)
			require.NoError(t, err)
			require.False(t, features.CRC32JSONTextBytes, "DML must not rebind legacy catalog expressions")
			found := false
			for _, n := range built.GetQuery().Nodes {
				for _, e := range n.ProjectList {
					found = found || containsLegacyCRC32(e)
				}
			}
			require.True(t, found, "production DML must evaluate the generated expression")
			require.True(t, containsLegacyCRC32(base.Cols[generatedPos].GeneratedCol.Expr))
		})
	}
}

func TestCRC32PersistedDDLAdmissionBeforeFold(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	proc := mock.ctxt.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	saved, present := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion106))
	t.Cleanup(func() {
		if present {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, saved)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion106))
		}
	})
	for _, sql := range []string{
		`create table crc32_gate(j json,c bigint unsigned generated always as (crc32(j)) stored)`,
		`create table crc32_gate_const(j json,c bigint unsigned generated always as (crc32(cast('{"a":1}' as json))) stored)`,
		`create view crc32_gate_view as select crc32(cast('{"a":1}' as json)) as c`,
	} {
		_, err := runOneStmt(mock, t, sql)
		require.ErrorContains(t, err, "protocol version 107", sql)
	}
}

func TestCRC32LegacyCatalogLikeAndAlter(t *testing.T) {
	for _, sql := range []string{
		`create table constraint_test.crc32_copy like constraint_test.t_on_update_gen`,
		`alter table constraint_test.t_on_update_gen add column other int`,
		`alter table constraint_test.t_on_update_gen rename column val to payload`,
		`alter table constraint_test.t_on_update_gen rename column g to checksum`,
		`alter table constraint_test.t_on_update_gen modify column g bigint unsigned generated always as (crc32(val)) stored first`,
	} {
		t.Run(sql, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			base := mock.ctxt.tables["t_on_update_gen"]
			base.Indexes = nil
			base.Name2ColIndex = make(map[string]int32, len(base.Cols))
			for i, col := range base.Cols {
				base.Name2ColIndex[col.Name] = int32(i)
			}
			pos := mockTableColPos(t, base, "val")
			base.Cols[pos].Typ = planpb.Type{Id: int32(types.T_json)}
			base.Cols[pos].Default = &planpb.Default{NullAbility: true}
			generated := base.Cols[mockTableColPos(t, base, "g")]
			shape, err := mysql.ParseOne(context.Background(), "create table shape(g bigint unsigned)", 1)
			require.NoError(t, err)
			defer shape.Free()
			generated.Typ, err = getTypeFromAst(context.Background(), shape.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef).Type)
			require.NoError(t, err)
			expr := legacyCRC32Expr()
			expr.GetF().Args[0].GetCol().ColPos = pos
			expr.GetF().Args[0].GetCol().Name = "val"
			generated.GeneratedCol = &planpb.GeneratedCol{Expr: expr, OriginString: "crc32(val)", IsStored: true}
			base.Cols[mockTableColPos(t, base, "updated_at")].OnUpdate = nil
			built, err := runOneStmt(mock, t, sql)
			if strings.Contains(sql, "rename column val") {
				// Existing DDL rejects renaming a dependency; this must remain
				// an explicit rejection, never an implicit algorithm upgrade.
				require.ErrorContains(t, err, "generated column 'g' depends on it")
				require.True(t, containsLegacyCRC32(generated.GeneratedCol.Expr))
				return
			}
			require.NoError(t, err)
			features, err := planpb.RequiredRemoteExpressionFeatures(built)
			require.NoError(t, err)
			require.False(t, features.CRC32JSONTextBytes)
			var target *planpb.TableDef
			if create := built.GetDdl().GetCreateTable(); create != nil {
				target = create.TableDef
			} else {
				target = built.GetDdl().GetAlterTable().CopyTableDef
			}
			require.NotNil(t, target)
			name := "g"
			if strings.Contains(sql, "rename column g") {
				name = "checksum"
			}
			copied := FindColumn(target.Cols, name)
			require.NotNil(t, copied)
			require.True(t, containsLegacyCRC32(copied.GetGeneratedCol().GetExpr()))
			require.Equal(t, mockTableColPos(t, target, "val"), copied.GeneratedCol.Expr.GetF().Args[0].GetCol().ColPos)
			require.True(t, containsLegacyCRC32(generated.GeneratedCol.Expr))
		})
	}
}

func TestCRC32CopyPreservesFoldedDefaultWithoutSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := planpb.Type{Id: int32(types.T_uint64)}
	source := &planpb.ColDef{Name: "c", Typ: typ, Default: &planpb.Default{NullAbility: true, OriginString: `(crc32(cast('{"t1":"a"}' as json)))`, Expr: &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 3719146973}}}}}}
	stmt, err := mysql.ParseOne(proc.Ctx, `create table t(c bigint unsigned default (crc32(cast('{"t1":"a"}' as json))))`, 1)
	require.NoError(t, err)
	defer stmt.Free()
	col := stmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
	got, err := buildDefaultExprWithColumns(proc.Ctx, col, typ, proc, nil, source)
	require.NoError(t, err)
	require.Equal(t, uint64(3719146973), got.Expr.GetLit().GetU64Val())
	require.NotSame(t, source.Default.Expr, got.Expr)
	require.True(t, tableHasLegacyCRC32(&planpb.TableDef{Cols: []*planpb.ColDef{source}}))
}

func TestCRC32UnchangedDefaultClauseRetainsIdentity(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := planpb.Type{Id: int32(types.T_uint64)}
	stmt, err := mysql.ParseOne(proc.Ctx, `create table t(c bigint unsigned not null default (crc32(cast('{"t1":"a"}' as json))))`, 1)
	require.NoError(t, err)
	defer stmt.Free()
	col := stmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
	source := &planpb.ColDef{Typ: typ, Default: &planpb.Default{NullAbility: true, OriginString: `(crc32(cast('{"t1":"a"}' as json)))`, Expr: legacyCRC32Expr()}}
	preserved := unchangedCRC32ColumnClauses(source, col, typ)
	require.NotNil(t, preserved.Default)
	require.False(t, preserved.Default.NullAbility)
	require.True(t, source.Default.NullAbility)
	source.Default.OriginString = "different expression"
	require.Nil(t, unchangedCRC32ColumnClauses(source, col, typ).Default)
}

func TestCRC32FoldedDefaultRetainsCapability(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	stmt, err := mysql.ParseOne(proc.Ctx, `create table t(c bigint unsigned default (crc32(cast('{"t1":"a"}' as json))))`, 1)
	require.NoError(t, err)
	defer stmt.Free()
	col := stmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
	got, err := buildDefaultExprWithColumns(proc.Ctx, col, planpb.Type{Id: int32(types.T_uint64)}, proc, nil)
	require.NoError(t, err)
	require.NotNil(t, got.Expr.GetLit())
	require.Equal(t, uint64(4012824821), got.Expr.GetLit().GetU64Val())
	required, err := planpb.RequiresMORPCVersion107CRC32JSONTextBytes(got)
	require.NoError(t, err)
	require.True(t, required)
}
