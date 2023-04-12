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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// resultSetStmtExecutor represents the execution outputting result set to the client
type resultSetStmtExecutor struct {
	*baseStmtExecutor
}

func (rsse *resultSetStmtExecutor) ResponseBeforeExec(ctx context.Context, ses *Session) error {
	var err error
	var columns []interface{}
	proto := ses.GetMysqlProtocol()
	err = rsse.baseStmtExecutor.ResponseBeforeExec(ctx, ses)
	if err != nil {
		return err
	}

	columns, err = rsse.GetColumns()
	if err != nil {
		logutil.Errorf("GetColumns from Computation handler failed. error: %v", err)
		return err
	}
	/*
		Step 1 : send column count and column definition.
	*/
	//send column count
	colCnt := uint64(len(columns))
	err = proto.SendColumnCountPacket(colCnt)
	if err != nil {
		return err
	}
	//send columns
	//column_count * Protocol::ColumnDefinition packets
	cmd := ses.GetCmd()
	mrs := ses.GetMysqlResultSet()
	for _, c := range columns {
		mysqlc := c.(Column)
		mrs.AddColumn(mysqlc)

		/*
			mysql COM_QUERY response: send the column definition per column
		*/
		err = proto.SendColumnDefinitionPacket(ctx, mysqlc, int(cmd))
		if err != nil {
			return err
		}
	}

	/*
		mysql COM_QUERY response: End after the column has been sent.
		send EOF packet
	*/
	err = proto.SendEOFPacketIf(0, 0)
	if err != nil {
		return err
	}
	return nil
}

func (rsse *resultSetStmtExecutor) ResponseAfterExec(ctx context.Context, ses *Session) error {
	/*
		Step 3: Say goodbye
		mysql COM_QUERY response: End after the data row has been sent.
		After all row data has been sent, it sends the EOF or OK packet.
	*/
	proto := ses.GetMysqlProtocol()
	return proto.sendEOFOrOkPacket(0, 0)
}

// TODO: special handle for export
type SelectExecutor struct {
	*resultSetStmtExecutor
	sel *tree.Select
}

type ValuesStmtExecutor struct {
	*resultSetStmtExecutor
	sel *tree.ValuesStatement
}

type ShowCreateTableExecutor struct {
	*resultSetStmtExecutor
	sct *tree.ShowCreateTable
}

type ShowCreateDatabaseExecutor struct {
	*resultSetStmtExecutor
	scd *tree.ShowCreateDatabase
}

type ShowTablesExecutor struct {
	*resultSetStmtExecutor
	st *tree.ShowTables
}

type ShowSequencesExecutor struct {
	*resultSetStmtExecutor
	ss *tree.ShowSequences
}

type ShowDatabasesExecutor struct {
	*resultSetStmtExecutor
	sd *tree.ShowDatabases
}

type ShowColumnsExecutor struct {
	*resultSetStmtExecutor
	sc *tree.ShowColumns
}

func (sec *ShowColumnsExecutor) Setup(ctx context.Context, ses *Session) error {
	err := sec.baseStmtExecutor.Setup(ctx, ses)
	if err != nil {
		return err
	}
	return err
}

type ShowProcessListExecutor struct {
	*resultSetStmtExecutor
	spl *tree.ShowProcessList
}

type ShowStatusExecutor struct {
	*resultSetStmtExecutor
	ss *tree.ShowStatus
}

type ShowTableStatusExecutor struct {
	*resultSetStmtExecutor
	sts *tree.ShowTableStatus
}

func (sec *ShowTableStatusExecutor) Setup(ctx context.Context, ses *Session) error {
	err := sec.baseStmtExecutor.Setup(ctx, ses)
	if err != nil {
		return err
	}
	ses.showStmtType = ShowTableStatus
	ses.SetData(nil)
	return nil
}

type ShowGrantsExecutor struct {
	*resultSetStmtExecutor
	sg *tree.ShowGrants
}

type ShowIndexExecutor struct {
	*resultSetStmtExecutor
	si *tree.ShowIndex
}

type ShowCreateViewExecutor struct {
	*resultSetStmtExecutor
	scv *tree.ShowCreateView
}

type ShowTargetExecutor struct {
	*resultSetStmtExecutor
	st *tree.ShowTarget
}

type ExplainForExecutor struct {
	*resultSetStmtExecutor
	ef *tree.ExplainFor
}

type ExplainStmtExecutor struct {
	*resultSetStmtExecutor
	es *tree.ExplainStmt
}

func (ese *ExplainStmtExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	//TODO
	return nil
}

type ShowVariablesExecutor struct {
	*resultSetStmtExecutor
	sv *tree.ShowVariables
}

func (sve *ShowVariablesExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doShowVariables(ses, sve.GetProcess(), sve.sv)
}

type ShowErrorsExecutor struct {
	*resultSetStmtExecutor
	se *tree.ShowErrors
}

func (see *ShowErrorsExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doShowErrors(ses)
}

type ShowWarningsExecutor struct {
	*resultSetStmtExecutor
	sw *tree.ShowWarnings
}

func (swe *ShowWarningsExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doShowErrors(ses)
}

type AnalyzeStmtExecutor struct {
	*resultSetStmtExecutor
	as *tree.AnalyzeStmt
}

func (ase *AnalyzeStmtExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	//TODO:
	return nil
}

type ExplainAnalyzeExecutor struct {
	*resultSetStmtExecutor
	ea *tree.ExplainAnalyze
}

func (eae *ExplainAnalyzeExecutor) Setup(ctx context.Context, ses *Session) error {
	err := eae.baseStmtExecutor.Setup(ctx, ses)
	if err != nil {
		return err
	}
	ses.SetData(nil)
	return err
}

func (eae *ExplainAnalyzeExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	//TODO:
	return nil
}

type InternalCmdFieldListExecutor struct {
	*resultSetStmtExecutor
	icfl *InternalCmdFieldList
}

func (icfle *InternalCmdFieldListExecutor) ResponseBeforeExec(ctx context.Context, ses *Session) error {
	return nil
}

func (icfle *InternalCmdFieldListExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doCmdFieldList(ctx, ses, icfle.icfl)
}

func (icfle *InternalCmdFieldListExecutor) ResponseAfterExec(ctx context.Context, ses *Session) error {
	var err error
	if icfle.GetStatus() == stmtExecSuccess {
		proto := ses.GetMysqlProtocol()
		/*
			mysql CMD_FIELD_LIST response: End after the column has been sent.
			send EOF packet
		*/
		err = proto.sendEOFOrOkPacket(0, 0)
		if err != nil {
			return err
		}
	}
	return nil
}
