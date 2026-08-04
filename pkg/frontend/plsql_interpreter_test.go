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

package frontend

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestInterpreterSetUserVariable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: ctx,
		proc:   testutil.NewProcess(t),
		ses:    ses,
	}
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "begin set @sql = 'select 60 as prep_sum'; end", 1)
	require.NoError(t, err)
	defer stmt.Free()

	back := &backgroundExecTest{}
	back.init()
	varScope := []map[string]interface{}{}
	interpreter := &Interpreter{
		ctx:      ctx,
		ses:      ses,
		bh:       back,
		varScope: &varScope,
		fmtctx:   tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true)),
	}
	interpreter.setAffectedRows(7)

	status, err := interpreter.interpret(stmt)
	require.NoError(t, err)
	require.Equal(t, SpOk, status)
	require.Empty(t, back.executedSQLs)
	userVar, err := ses.GetUserDefinedVar("sql")
	require.NoError(t, err)
	require.Equal(t, "select 60 as prep_sum", userVar.Value)
	require.Equal(t, "set @sql = \"select 60 as prep_sum\"", userVar.Sql)
	require.Zero(t, interpreter.lastAffectedRows)
}

func TestBackgroundPreparedStatementUsesClientRegistry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	owner := newTestSession(t, ctrl)
	defer owner.Close()
	prepared := &PrepareStmt{Name: "inside_proc", Sql: "select 1"}
	require.NoError(t, owner.SetPrepareStmt(ctx, prepared.Name, prepared))

	backSes := &backSession{feSessionImpl: feSessionImpl{upstream: owner}}
	require.NoError(t, backSes.SetUserDefinedVar("sql", "select 1", "set @sql = 'select 1'"))
	userVar, err := owner.GetUserDefinedVar("sql")
	require.NoError(t, err)
	require.Equal(t, "select 1", userVar.Value)
	prepareVar := tree.NewPrepareVar("inside_proc", tree.NewVarExpr("sql", false, false, nil))
	defer prepareVar.Free()
	canExecute, err := statementCanBeExecutedInUncommittedTransaction(ctx, backSes, prepareVar)
	require.NoError(t, err)
	require.True(t, canExecute)
	resolvedOwner, err := preparedStatementOwner(ctx, backSes)
	require.NoError(t, err)
	require.Same(t, owner, resolvedOwner)
	got, err := backSes.GetPrepareStmt(ctx, prepared.Name)
	require.NoError(t, err)
	require.Same(t, prepared, got)
	require.True(t, backSes.RemovePrepareStmt(prepared.Name))
	_, err = owner.GetPrepareStmt(ctx, prepared.Name)
	require.Error(t, err)
}

func TestInterpreterOutputDecimalPreservesDeclaredRuntimeType(t *testing.T) {
	require.Equal(t, types.T_decimal256, procedureOutputRuntimeType(&tree.T{
		InternalType: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_NEWDECIMAL)},
	}))
	require.Equal(t, types.T_decimal256, procedureOutputRuntimeType(&tree.T{
		InternalType: tree.InternalType{Family: tree.FloatFamily, FamilyString: "DECIMAL"},
	}))
	require.Equal(t, types.T_bool, procedureOutputRuntimeType(&tree.T{
		InternalType: tree.InternalType{FamilyString: "BOOL"},
	}))
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	interpreter := &Interpreter{
		ses:             ses,
		argsRuntimeType: map[string]types.T{"amount": types.T_decimal256},
	}
	require.NoError(t, interpreter.setOutputUserVariable("out_amount", "amount", "9007199254740993.25"))
	userVar, err := ses.GetUserDefinedVar("out_amount")
	require.NoError(t, err)
	require.Equal(t, types.T_decimal256, userVar.RuntimeType)
	require.Equal(t, "9007199254740993.25", userVar.Value)

	interpreter.argsRuntimeType["flag"] = types.T_bool
	require.NoError(t, interpreter.setOutputUserVariable("out_flag", "flag", true))
	flag, err := ses.GetUserDefinedVar("out_flag")
	require.NoError(t, err)
	require.Equal(t, types.T_int64, flag.RuntimeType)
	require.Equal(t, int64(1), flag.Value)
}

func TestProcedureArgumentDeclaredTypeSurvivesCatalogJSON(t *testing.T) {
	for _, test := range []struct {
		name string
		typ  *tree.T
		want types.T
	}{
		{name: "decimal", typ: &tree.T{InternalType: tree.InternalType{
			Family: tree.FloatFamily, FamilyString: "DECIMAL", Width: 30, Scale: 2,
		}}, want: types.T_decimal256},
		{name: "bool", typ: &tree.T{InternalType: tree.InternalType{
			Family: tree.BoolFamily, FamilyString: "BOOL",
		}}, want: types.T_bool},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal([]tree.ProcedureArgForMarshal{{
				ArgName: "out_value", Type: test.typ, InOutType: tree.TYPE_OUT,
			}})
			require.NoError(t, err)
			var decoded []tree.ProcedureArgForMarshal
			require.NoError(t, json.Unmarshal(encoded, &decoded))
			require.Len(t, decoded, 1)
			require.IsType(t, &tree.T{}, decoded[0].Type)
			require.Equal(t, test.want, procedureOutputRuntimeType(decoded[0].Type))
		})
	}
}
