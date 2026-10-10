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

	"github.com/gogo/protobuf/proto"
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

func TestCRC32LegacyCheckCopyReplay(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    types.T
		replay bool
	}{
		{"reorder_integer", types.T_int32, true},
		{"reorder_json", types.T_json, true},
		{"no_replay", types.T_json, false},
		{"changed_input_type", types.T_int64, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			expr, err := BindFuncExprImplByPlanExpr(mock.ctxt.GetContext(), ">", []*Expr{legacyCRC32Expr(), MakePlan2Uint64ConstExprWithType(0)})
			require.NoError(t, err)
			original := &planpb.TableDef{Name: "original", Cols: []*planpb.ColDef{
				{ColId: 1, Name: "j", Typ: planpb.Type{Id: int32(types.T_json)}},
				{ColId: 2, Name: "x", Typ: planpb.Type{Id: int32(tc.typ)}},
			}, Checks: []*planpb.CheckDef{{Name: "ck", Check: expr}}}
			target := proto.Clone(original).(*planpb.TableDef)
			target.Name = "copy"
			target.Cols[0], target.Cols[1] = target.Cols[1], target.Cols[0]
			if tc.name == "changed_input_type" {
				target.Cols[1].Typ.Id = int32(types.T_int64)
			}
			ctx := context.WithValue(mock.ctxt.GetContext(), defines.CRC32CopyExpressionsKey{}, target)
			if tc.replay {
				ctx = WithPersistedDDLReplay(ctx, original, target)
			}
			mock.ctxt.SetContext(ctx)
			created := &planpb.TableDef{Name: target.Name, Cols: target.Cols}
			err = appendCheckDef(&mock.ctxt, created, "ck", nil, -1)
			if tc.name == "changed_input_type" || !tc.replay {
				require.ErrorContains(t, err, "explicit table rebuild")
				require.Empty(t, created.Checks)
				return
			}
			require.NoError(t, err)
			require.Len(t, created.Checks, 1)
			got := created.Checks[0].Check.GetF().Args[0]
			require.True(t, containsLegacyCRC32(got))
			require.Equal(t, int32(1), got.GetF().Args[0].GetCol().ColPos)
			got.GetF().Args[0].GetCol().ColPos = 99
			require.Equal(t, int32(0), original.Checks[0].Check.GetF().Args[0].GetF().Args[0].GetCol().ColPos)
			require.Equal(t, int32(0), target.Checks[0].Check.GetF().Args[0].GetF().Args[0].GetCol().ColPos)
		})
	}
}

func TestCRC32LegacyCheckCopyClauseOrder(t *testing.T) {
	for _, tc := range []struct {
		name    string
		clauses string
		reject  bool
	}{
		{"move_then_modify_other", "modify column j json after x, modify column x int", false},
		{"modify_other_then_move", "modify column x int, modify column j json after x", false},
		{"move_then_convert_input", "modify column j json after x, modify column j bigint", true},
		// Reordering x moves j from position 1 to 2 in both clause orders.
		// Each pair has the same final schema and varies only clause order.
		{"move_then_modify_bigint", "modify column x int after id, modify column j bigint", true},
		{"modify_bigint_then_move", "modify column j bigint, modify column x int after id", true},
		{"move_then_modify_varchar", "modify column x int after id, modify column j varchar(32)", true},
		{"modify_varchar_then_move", "modify column j varchar(32), modify column x int after id", true},
		{"move_then_change_bigint", "modify column x int after id, change column j j bigint", true},
		{"change_bigint_then_move", "change column j j bigint, modify column x int after id", true},
		{"move_then_change_varchar", "modify column x int after id, change column j j varchar(32)", true},
		{"change_varchar_then_move", "change column j j varchar(32), modify column x int after id", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			created, err := runOneStmt(mock, t, "create table tpch.crc32_check_order(id int primary key, j json, x int)")
			require.NoError(t, err)
			base := created.GetDdl().GetCreateTable().TableDef
			for i, col := range base.Cols {
				col.ColId = uint64(i + 1)
			}
			// Model a persisted legacy expression, not a new CRC binding or an
			// actual old-release deployment. All SQL planning uses real entry points.
			legacy := legacyCRC32Expr()
			legacy.GetF().Args[0].Typ = base.Cols[1].Typ
			legacy.GetF().Args[0].GetCol().ColPos = 1
			check, err := BindFuncExprImplByPlanExpr(mock.ctxt.GetContext(), ">", []*Expr{legacy, MakePlan2Uint64ConstExprWithType(0)})
			require.NoError(t, err)
			base.Checks = []*planpb.CheckDef{{Name: "ck", Check: check, OriginSql: "crc32(j) > 0"}}
			require.Empty(t, base.Indexes, "no index may mask CHECK admission")
			for _, col := range base.Cols {
				require.Nil(t, col.GeneratedCol, "no generated expression may mask CHECK admission")
			}
			original := proto.Clone(base).(*planpb.TableDef)
			mock.ctxt.tables[base.Name] = base
			mock.ctxt.objects[base.Name] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: base.Name}

			built, err := runOneStmt(mock, t, "alter table tpch.crc32_check_order "+tc.clauses)
			require.True(t, proto.Equal(original, base), "ALTER must not mutate source catalog metadata")
			if tc.reject {
				require.ErrorContains(t, err, "changing a legacy CRC32 JSON check constraint requires an explicit table rebuild")
				require.Nil(t, built, "unsafe input conversion must fail before COPY execution")
				crc := base.Checks[0].Check.GetF().Args[0].GetF()
				require.Equal(t, function.EncodeOverloadID(function.CRC32, function.CRC32LegacyOverload), crc.Func.Obj)
				require.Equal(t, int32(types.T_json), crc.Args[0].Typ.Id)
				require.Equal(t, int32(1), crc.Args[0].GetCol().ColPos)
				// The identical ALTER without CHECK must plan successfully:
				// rejection is caused by CHECK, not another conversion guard.
				control := proto.Clone(original).(*planpb.TableDef)
				control.Checks = nil
				mock.ctxt.tables[base.Name] = control
				allowed, controlErr := runOneStmt(mock, t, "alter table tpch.crc32_check_order "+tc.clauses)
				require.NoError(t, controlErr)
				require.Equal(t, planpb.AlterTable_COPY, allowed.GetDdl().GetAlterTable().AlgorithmType)
				require.True(t, proto.Equal(original, base), "control must not mutate CHECK source")
				return
			}
			require.NoError(t, err)
			alter := built.GetDdl().GetAlterTable()
			require.Equal(t, planpb.AlterTable_COPY, alter.AlgorithmType)
			target := alter.CopyTableDef
			for i, name := range []string{"id", "x", "j"} {
				require.Equal(t, name, target.Cols[i].Name)
			}
			require.Len(t, target.Checks, 1)
			require.True(t, proto.Equal(original.Checks[0], alter.TableDef.Checks[0]))
			require.True(t, proto.Equal(original.Checks[0], target.Checks[0]), "COPY skeleton must keep original CHECK coordinates")

			ctx := context.WithValue(mock.ctxt.GetContext(), defines.CRC32CopyExpressionsKey{}, target)
			mock.ctxt.SetContext(WithPersistedDDLReplay(ctx, alter.TableDef, target))
			rebuilt, err := runOneStmt(mock, t, alter.CreateTmpTableSql)
			require.NoError(t, err)
			checks := rebuilt.GetDdl().GetCreateTable().TableDef.Checks
			require.Len(t, checks, 1)
			want := DeepCopyExpr(original.Checks[0].Check)
			want.GetF().Args[0].GetF().Args[0].GetCol().ColPos = 2
			require.True(t, proto.Equal(want, checks[0].Check), "both clause orders must rebuild the same mapped CRC0(j)")
			crc := checks[0].Check.GetF().Args[0].GetF()
			require.Equal(t, function.EncodeOverloadID(function.CRC32, function.CRC32LegacyOverload), crc.Func.Obj)
			require.Equal(t, int32(2), crc.Args[0].GetCol().ColPos)
			require.Equal(t, "j", crc.Args[0].GetCol().Name)
			crc.Args[0].GetCol().ColPos = 99
			require.True(t, proto.Equal(original, base), "replayed CHECK must not alias source metadata")
			require.True(t, proto.Equal(original.Checks[0], target.Checks[0]))
		})
	}
}

func TestCRC32LegacyCheckAlterRejectsInputConversion(t *testing.T) {
	for _, sql := range []string{
		`alter table constraint_test.t_on_update_gen modify column val bigint`,
		`alter table constraint_test.t_on_update_gen change column val val varchar(32)`,
	} {
		t.Run(sql, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			base := mock.ctxt.tables["t_on_update_gen"]
			base.Indexes = nil
			base.Name2ColIndex = make(map[string]int32, len(base.Cols))
			for i, col := range base.Cols {
				base.Name2ColIndex[col.Name] = int32(i)
				col.GeneratedCol = nil // CHECK, not generated-column protection, must reject this ALTER.
				col.OnUpdate = nil
			}
			pos := mockTableColPos(t, base, "val")
			base.Cols[pos].Typ = planpb.Type{Id: int32(types.T_json)}
			base.Cols[pos].Default = &planpb.Default{NullAbility: true}
			legacy := legacyCRC32Expr()
			legacy.GetF().Args[0].GetCol().ColPos = pos
			legacy.GetF().Args[0].GetCol().Name = "val"
			expr, err := BindFuncExprImplByPlanExpr(mock.ctxt.GetContext(), ">", []*Expr{legacy, MakePlan2Uint64ConstExprWithType(0)})
			require.NoError(t, err)
			base.Checks = []*planpb.CheckDef{{Name: "ck", Check: expr}}
			_, err = runOneStmt(mock, t, sql)
			require.ErrorContains(t, err, "explicit table rebuild")
			require.Equal(t, int32(types.T_json), base.Cols[pos].Typ.Id)
			require.Equal(t, pos, legacy.GetF().Args[0].GetCol().ColPos)
		})
	}
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
	for _, inputType := range []types.T{types.T_int64, types.T_varchar} {
		t.Run(inputType.String(), func(t *testing.T) {
			changed := []*planpb.ColDef{{Name: "j", Typ: planpb.Type{Id: int32(inputType)}}, source}
			_, err := buildGeneratedExpr(proc.Ctx, col, typ, changed, proc)
			require.ErrorContains(t, err, "explicit table rebuild")
			require.Equal(t, int32(types.T_json), source.GeneratedCol.Expr.GetF().Args[0].Typ.Id)
		})
	}
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
		require.ErrorContains(t, err, "protocol version 109", sql)
	}
}

func TestCRC32LegacyCatalogLikeAndAlter(t *testing.T) {
	for _, sql := range []string{
		`create table constraint_test.crc32_copy like constraint_test.t_on_update_gen`,
		`alter table constraint_test.t_on_update_gen add column other int`,
		`alter table constraint_test.t_on_update_gen rename column val to payload`,
		`alter table constraint_test.t_on_update_gen rename column g to checksum`,
		`alter table constraint_test.t_on_update_gen modify column g bigint unsigned generated always as (crc32(val)) stored first`,
		`alter table constraint_test.t_on_update_gen modify column val bigint`,
		`alter table constraint_test.t_on_update_gen change column val val varchar(32)`,
		`alter table constraint_test.t_on_update_gen modify column val json first`,
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
			if strings.Contains(sql, "val bigint") || strings.Contains(sql, "val varchar") {
				require.ErrorContains(t, err, "explicit table rebuild")
				require.Equal(t, int32(types.T_json), base.Cols[pos].Typ.Id, "rejected ALTER must leave source catalog unchanged")
				require.True(t, containsLegacyCRC32(generated.GeneratedCol.Expr))
				return
			}
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
	required, err := planpb.RequiresMORPCVersion109CRC32JSONTextBytes(got)
	require.NoError(t, err)
	require.True(t, required)
}
