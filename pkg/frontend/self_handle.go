// Copyright 2021 Matrix Origin
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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func handleInFrontend(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx,
) (err error) {
	//check transaction states
	switch st := execCtx.stmt.(type) {
	case *tree.BeginTransaction:
		err = ses.GetTxnHandler().TxnBegin()
		if err != nil {
			return
		}
		RecordStatementTxnID(requestCtx, ses)
	case *tree.CommitTransaction:
		err = ses.GetTxnHandler().TxnCommit()
		if err != nil {
			return
		}
	case *tree.RollbackTransaction:
		err = ses.GetTxnHandler().TxnRollback()
		if err != nil {
			return
		}
	case *tree.SetRole:

		ses.InvalidatePrivilegeCache()
		//switch role
		err = handleSwitchRole(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.Use:

		var v interface{}
		v, err = ses.GetGlobalVar("lower_case_table_names")
		if err != nil {
			return
		}
		st.Name.SetConfig(v.(int64))
		//use database
		err = handleChangeDB(requestCtx, ses, st.Name.Compare())
		if err != nil {
			return
		}
		err = changeVersion(requestCtx, ses, st.Name.Compare())
		if err != nil {
			return
		}
	case *tree.MoDump:

		//dump
		err = handleDump(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.PrepareStmt:

		execCtx.prepareStmt, err = handlePrepareStmt(requestCtx, ses, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			ses.RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.PrepareString:

		execCtx.prepareStmt, err = handlePrepareString(requestCtx, ses, st)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			ses.RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.CreateConnector:

		err = handleCreateConnector(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.PauseDaemonTask:

		err = handlePauseDaemonTask(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.CancelDaemonTask:

		err = handleCancelDaemonTask(requestCtx, ses, st.TaskID)
		if err != nil {
			return
		}
	case *tree.ResumeDaemonTask:

		err = handleResumeDaemonTask(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.DropConnector:

		err = handleDropConnector(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.ShowConnectors:

		if err = handleShowConnectors(requestCtx, ses, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.Deallocate:

		err = handleDeallocate(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.Reset:

		err = handleReset(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.SetVar:

		err = handleSetVar(requestCtx, ses, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
	case *tree.ShowVariables:

		err = handleShowVariables(ses, st, execCtx.proc, execCtx.isLastStmt)
		if err != nil {
			return
		}
	case *tree.ShowErrors, *tree.ShowWarnings:

		err = handleShowErrors(ses, execCtx.isLastStmt)
		if err != nil {
			return
		}
	case *tree.AnalyzeStmt:

		if err = handleAnalyzeStmt(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ExplainStmt:

		if err = handleExplainStmt(requestCtx, ses, st); err != nil {
			return
		}
	case *InternalCmdFieldList:

		if err = handleCmdFieldList(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreatePublication:

		if err = handleCreatePublication(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterPublication:

		if err = handleAlterPublication(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropPublication:

		if err = handleDropPublication(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ShowSubscriptions:

		if err = handleShowSubscriptions(requestCtx, ses, st, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.CreateStage:

		if err = handleCreateStage(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropStage:

		if err = handleDropStage(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterStage:

		if err = handleAlterStage(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateAccount:

		ses.InvalidatePrivilegeCache()
		if err = handleCreateAccount(requestCtx, ses, st, execCtx.proc); err != nil {
			return
		}
	case *tree.DropAccount:

		ses.InvalidatePrivilegeCache()
		if err = handleDropAccount(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterAccount:
		ses.InvalidatePrivilegeCache()

		if err = handleAlterAccount(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterDataBaseConfig:
		ses.InvalidatePrivilegeCache()

		if st.IsAccountLevel {
			if err = handleAlterAccountConfig(requestCtx, ses, st); err != nil {
				return
			}
		} else {
			if err = handleAlterDataBaseConfig(requestCtx, ses, st); err != nil {
				return
			}
		}
	case *tree.CreateUser:

		ses.InvalidatePrivilegeCache()
		if err = handleCreateUser(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropUser:

		ses.InvalidatePrivilegeCache()
		if err = handleDropUser(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterUser: //TODO

		ses.InvalidatePrivilegeCache()
		if err = handleAlterUser(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateRole:

		ses.InvalidatePrivilegeCache()
		if err = handleCreateRole(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropRole:

		ses.InvalidatePrivilegeCache()
		if err = handleDropRole(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateFunction:

		if err = st.Valid(); err != nil {
			return err
		}
		if err = handleCreateFunction(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropFunction:

		if err = handleDropFunction(requestCtx, ses, st, execCtx.proc); err != nil {
			return
		}
	case *tree.CreateProcedure:

		if err = handleCreateProcedure(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropProcedure:

		if err = handleDropProcedure(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CallStmt:

		if err = handleCallProcedure(requestCtx, ses, st, execCtx.proc); err != nil {
			return
		}
	case *tree.Grant:

		ses.InvalidatePrivilegeCache()
		switch st.Typ {
		case tree.GrantTypeRole:
			if err = handleGrantRole(requestCtx, ses, &st.GrantRole); err != nil {
				return
			}
		case tree.GrantTypePrivilege:
			if err = handleGrantPrivilege(requestCtx, ses, &st.GrantPrivilege); err != nil {
				return
			}
		}
	case *tree.Revoke:

		ses.InvalidatePrivilegeCache()
		switch st.Typ {
		case tree.RevokeTypeRole:
			if err = handleRevokeRole(requestCtx, ses, &st.RevokeRole); err != nil {
				return
			}
		case tree.RevokeTypePrivilege:
			if err = handleRevokePrivilege(requestCtx, ses, &st.RevokePrivilege); err != nil {
				return
			}
		}
	case *tree.Kill:

		ses.InvalidatePrivilegeCache()
		if err = handleKill(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ShowAccounts:

		if err = handleShowAccounts(requestCtx, ses, st, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.ShowCollation:

		if err = handleShowCollation(ses, st, execCtx.proc, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.ShowBackendServers:

		if err = handleShowBackendServers(requestCtx, ses, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.SetTransaction:

		//TODO: handle set transaction
	case *tree.LockTableStmt:

	case *tree.UnLockTableStmt:

	case *tree.BackupStart:

		if err = handleStartBackup(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.EmptyStmt:

		if err = handleEmptyStmt(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateSnapShot:
		//TODO: invalidate privilege cache
		if err = handleCreateSnapshot(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropSnapShot:
		//TODO: invalidate privilege cache
		if err = handleDropSnapshot(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.UpgradeStatement:
		//TODO: invalidate privilege cache
		if err = handleExecUpgrade(requestCtx, ses, st); err != nil {
			return
		}
	}
	return
}

/*
handle "SELECT @@xxx.yyyy"
*/
func handleSelectVariables(ses FeSession, ve *tree.VarExpr, isLastStmt bool) error {
	var err error = nil
	mrs := ses.GetMysqlResultSet()

	col := new(MysqlColumn)
	col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col.SetName("@@" + ve.Name)
	mrs.AddColumn(col)

	row := make([]interface{}, 1)
	if ve.System {
		if ve.Global {
			val, err := ses.GetGlobalVar(ve.Name)
			if err != nil {
				return err
			}
			row[0] = val
		} else {
			val, err := ses.GetSessionVar(ve.Name)
			if err != nil {
				return err
			}
			row[0] = val
		}
	} else {
		//user defined variable
		_, val, err := ses.GetUserDefinedVar(ve.Name)
		if err != nil {
			return err
		}
		if val != nil {
			row[0] = val.Value
		} else {
			row[0] = nil
		}
	}

	mrs.AddRow(row)
	return err
}

/*
handle cmd CMD_FIELD_LIST
*/
func handleCmdFieldList(requestCtx context.Context, ses FeSession, icfl *InternalCmdFieldList) error {
	var err error
	proto := ses.GetMysqlProtocol()

	ses.SetMysqlResultSet(nil)
	err = doCmdFieldList(requestCtx, ses.(*Session), icfl)
	if err != nil {
		return err
	}

	/*
		mysql CMD_FIELD_LIST response: End after the column has been sent.
		send EOF packet
	*/
	err = proto.sendEOFOrOkPacket(0, ses.GetTxnHandler().GetServerStatus())
	if err != nil {
		return err
	}

	return err
}

func doCmdFieldList(requestCtx context.Context, ses *Session, _ *InternalCmdFieldList) error {
	dbName := ses.GetDatabaseName()
	if dbName == "" {
		return moerr.NewNoDB(requestCtx)
	}

	//Get table infos for the database from the cube
	//case 1: there are no table infos for the db
	//case 2: db changed
	//NOTE: it costs too much time.
	//It just reduces the information in the auto-completion (auto-rehash) of the mysql client.
	//var attrs []ColumnInfo
	//
	//if tableInfos == nil || db != dbName {
	//	txnHandler := ses.GetTxnHandler()
	//	eng := ses.GetStorage()
	//	db, err := eng.Database(requestCtx, dbName, txnHandler.GetTxn())
	//	if err != nil {
	//		return err
	//	}
	//
	//	names, err := db.Relations(requestCtx)
	//	if err != nil {
	//		return err
	//	}
	//	for _, name := range names {
	//		table, err := db.Relation(requestCtx, name)
	//		if err != nil {
	//			return err
	//		}
	//
	//		defs, err := table.TableDefs(requestCtx)
	//		if err != nil {
	//			return err
	//		}
	//		for _, def := range defs {
	//			if attr, ok := def.(*engine.AttributeDef); ok {
	//				attrs = append(attrs, &engineColumnInfo{
	//					name: attr.Attr.Name,
	//					typ:  attr.Attr.Type,
	//				})
	//			}
	//		}
	//	}
	//
	//	if tableInfos == nil {
	//		tableInfos = make(map[string][]ColumnInfo)
	//	}
	//	tableInfos[tableName] = attrs
	//}
	//
	//cols, ok := tableInfos[tableName]
	//if !ok {
	//	//just give the empty info when there is no such table.
	//	attrs = make([]ColumnInfo, 0)
	//} else {
	//	attrs = cols
	//}
	//
	//for _, c := range attrs {
	//	col := new(MysqlColumn)
	//	col.SetName(c.GetName())
	//	err = convertEngineTypeToMysqlType(c.GetType(), col)
	//	if err != nil {
	//		return err
	//	}
	//
	//	/*
	//		mysql CMD_FIELD_LIST response: send the column definition per column
	//	*/
	//	err = proto.SendColumnDefinitionPacket(col, int(COM_FIELD_LIST))
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

// handleDeallocate
func handleDeallocate(ctx context.Context, ses FeSession, st *tree.Deallocate) error {
	return doDeallocate(ctx, ses.(*Session), st)
}

func doDeallocate(ctx context.Context, ses *Session, st *tree.Deallocate) error {
	deallocatePlan, err := buildPlan(ctx, ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return err
	}
	ses.RemovePrepareStmt(deallocatePlan.GetDcl().GetDeallocate().GetName())
	return nil
}

// handleReset
func handleReset(ctx context.Context, ses FeSession, st *tree.Reset) error {
	return doReset(ctx, ses.(*Session), st)
}

func doReset(_ context.Context, _ *Session, _ *tree.Reset) error {
	return nil
}

func handleEmptyStmt(ctx context.Context, ses FeSession, stmt *tree.EmptyStmt) error {
	var err error
	return err
}

func constructVarBatch(ses *Session, rows [][]interface{}) (*batch.Batch, error) {
	bat := batch.New(true, []string{"Variable_name", "Value"})
	typ := types.New(types.T_varchar, types.MaxVarcharLen, 0)
	cnt := len(rows)
	bat.SetRowCount(cnt)
	v0 := make([]string, cnt)
	v1 := make([]string, cnt)
	for i, row := range rows {
		v0[i] = row[0].(string)
		v1[i] = fmt.Sprintf("%v", row[1])
	}
	bat.Vecs[0] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[0], v0, nil, ses.GetMemPool())
	bat.Vecs[1] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[1], v1, nil, ses.GetMemPool())
	return bat, nil
}

func constructCollationBatch(ses *Session, rows [][]interface{}) (*batch.Batch, error) {
	bat := batch.New(true, []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"})
	typ := types.New(types.T_varchar, types.MaxVarcharLen, 0)
	longlongTyp := types.New(types.T_int64, 0, 0)
	longTyp := types.New(types.T_int32, 0, 0)
	cnt := len(rows)
	bat.SetRowCount(cnt)
	v0 := make([]string, cnt)
	v1 := make([]string, cnt)
	v2 := make([]int64, cnt)
	v3 := make([]string, cnt)
	v4 := make([]string, cnt)
	v5 := make([]int32, cnt)
	v6 := make([]string, cnt)
	for i, row := range rows {
		v0[i] = row[0].(string)
		v1[i] = row[1].(string)
		v2[i] = row[2].(int64)
		v3[i] = row[3].(string)
		v4[i] = row[4].(string)
		v5[i] = row[5].(int32)
		v6[i] = row[6].(string)
	}
	bat.Vecs[0] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[0], v0, nil, ses.GetMemPool())
	bat.Vecs[1] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[1], v1, nil, ses.GetMemPool())
	bat.Vecs[2] = vector.NewVec(longlongTyp)
	vector.AppendFixedList[int64](bat.Vecs[2], v2, nil, ses.GetMemPool())
	bat.Vecs[3] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[3], v3, nil, ses.GetMemPool())
	bat.Vecs[4] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[4], v4, nil, ses.GetMemPool())
	bat.Vecs[5] = vector.NewVec(longTyp)
	vector.AppendFixedList[int32](bat.Vecs[5], v5, nil, ses.GetMemPool())
	bat.Vecs[6] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[6], v6, nil, ses.GetMemPool())
	return bat, nil
}
