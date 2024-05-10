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

package tree

import (
	"fmt"
)

type Statement interface {
	fmt.Stringer
	NodeFormatter
	StatementType
	StmtKind() StmtKind
	Free()
}

type StatementType interface {
	// GetStatementType return like insert, update, delete, begin, rename database, rename table, ...
	GetStatementType() string
	// GetQueryType return val like DQL, DML, DDL, ...
	GetQueryType() string
}

type statementImpl struct {
	Statement
}

func (s *statementImpl) Free() {
}

const (
	// QueryTypeDQL (Data Query Language) Select, MoDump, ValuesStatement, With
	QueryTypeDQL = "DQL"
	// QueryTypeDDL (Data Definition Language): CreateDatabase, DropDatabase, DropTable,
	// Create/Drop/Alter/Rename Database/Table/View/Index/Function, TruncateTable,
	QueryTypeDDL = "DDL"
	// QueryTypeDML (Data Manipulation Language): Insert, Update, Delete, Load
	QueryTypeDML = "DML"
	// QueryTypeDCL (Data Control Language)
	// statement: Grant, Revoke
	// CreateAccount, CreateUser, CreateRole, AlterAccount, AlterUser, DropAccount, DropUser, DropRole
	QueryTypeDCL = "DCL"
	// QueryTypeTCL (Transaction Control Language): BeginTransaction, RollbackTransaction, CommitTransaction, Savepoint(Not Support)
	QueryTypeTCL = "TCL"
	// QueryTypeOth (Other.)
	// statement: AnalyzeStmt(Not Support), ExplainStmt, ExplainAnalyze, ExplainFor,
	// SetVar, SetDefaultRole, SetRole, SetPassword, Declare, Do, TableFunction, Use, PrepareStmt, Execute, Deallocate, Kill
	// Show ..., ShowCreateTable, ShowColumns(Desc)
	QueryTypeOth = "Other"
)

//result type bit format
//all bits zero : undefined
//bit 0~1 :
//		00 undefined. invalid result type or the statement based on the real statement like EXECUTE,CALL
//		01 result row
//		10 status
//bit 2~4 :
//		000 stream result row
//		001 prebuild all result rows before sending
//		010 no resp. do no response to the client. like COM_QUIT command, Deallocate from COM_STMT_CLOSE.
//		011 resp in handle function. result row or status. like EXECUTE,CALL.
//		100 resp_status
//		101 mixed result row. show table status. stream column def. composite result row.
//bit 5
//		0 in computation engine
//		1 in the frontend

type StmtKind int

type OutputType int
type RespType int
type ExecLocation int

const (
	OUTPUT_UNDEFINED  OutputType = 0x0
	OUTPUT_RESULT_ROW OutputType = 0x1
	OUTPUT_STATUS     OutputType = 0x2

	//
	RESP_STREAM_RESULT_ROW   RespType = 0x0
	RESP_PREBUILD_RESULT_ROW RespType = 0x1
	RESP_NOTHING             RespType = 0x2
	RESP_BY_SITUATION        RespType = 0x3
	RESP_STATUS              RespType = 0x4
	RESP_MIXED_RESULT_ROW    RespType = 0x5

	//
	EXEC_IN_ENGINE   ExecLocation = 0x0
	EXEC_IN_FRONTEND ExecLocation = 0x1
)

func MakeStmtKind(resTyp OutputType, respTyp RespType, handleTyp ExecLocation) StmtKind {
	return StmtKind(int(resTyp) | (int(respTyp) << 2) | (int(handleTyp) << 5))
}

func (t StmtKind) OutputType() OutputType {
	return OutputType((0x3 & int(t)))
}

func (t StmtKind) RespType() RespType {
	return RespType((int(t) >> 2) & 0x7)
}

func (t StmtKind) ExecLocation() ExecLocation {
	return ExecLocation((int(t) >> 5) & 0x1)
}

var (
	//response result rows to the client.
	//like SELECT...,SHOW...,
	defaultResRowTyp = MakeStmtKind(OUTPUT_RESULT_ROW, RESP_STREAM_RESULT_ROW, EXEC_IN_ENGINE)

	//response status(success or fail) to the client.
	//like CREATE...,DROP...,
	defaultStatusTyp = MakeStmtKind(OUTPUT_STATUS, RESP_STATUS, EXEC_IN_ENGINE)

	frontendStatusTyp = MakeStmtKind(OUTPUT_STATUS, RESP_STATUS, EXEC_IN_FRONTEND)

	//like statements: they composite the result set themselves
	//    ShowConnectors
	//    ExplainStmt
	//    ShowTableStatus
	//    ShowErrors
	//    ShowVariables
	//    ShowAccounts
	//    ShowCollation
	//    ShowSubscriptions
	//    ShowBackendServers
	compositeResRowType = MakeStmtKind(OUTPUT_RESULT_ROW, RESP_PREBUILD_RESULT_ROW, EXEC_IN_FRONTEND)
)

func (node *Select) StmtKind() StmtKind {
	if node.Ep != nil {
		return defaultStatusTyp
	}
	return defaultResRowTyp
}

func (node *Use) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *BeginTransaction) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *CommitTransaction) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *RollbackTransaction) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *CreatePublication) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *AlterPublication) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *DropPublication) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *ShowSubscriptions) StmtKind() StmtKind {
	return compositeResRowType
}

func (node *CreateStage) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *DropStage) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *AlterStage) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *CreateAccount) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *DropAccount) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *AlterAccount) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *AlterDataBaseConfig) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *CreateUser) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *DropUser) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *AlterUser) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *CreateRole) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *DropRole) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *CreateFunction) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *DropFunction) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *CreateProcedure) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *DropProcedure) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *CallStmt) StmtKind() StmtKind {
	return MakeStmtKind(OUTPUT_UNDEFINED, RESP_BY_SITUATION, EXEC_IN_FRONTEND)
}

func (node *Grant) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *Revoke) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (k *Kill) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *ShowAccounts) StmtKind() StmtKind {
	return compositeResRowType
}

func (node *ShowBackendServers) StmtKind() StmtKind {
	return compositeResRowType
}

func (node *SetTransaction) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *LockTableStmt) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *UnLockTableStmt) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *BackupStart) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (e *EmptyStmt) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *prepareImpl) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *Execute) StmtKind() StmtKind {
	return MakeStmtKind(OUTPUT_UNDEFINED, RESP_BY_SITUATION, EXEC_IN_ENGINE)
}

func (node *Deallocate) StmtKind() StmtKind {
	//if it triggered by COM_STMT_CLOSE, it should return NoResp
	return frontendStatusTyp
}

func (node *Update) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *CreateDatabase) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *CreateTable) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *CreateSource) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *CreateView) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *ShowDatabases) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowTables) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowCreateTable) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *Insert) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *ShowVariables) StmtKind() StmtKind {
	return compositeResRowType
}

func (node *ShowIndex) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowTarget) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowCollation) StmtKind() StmtKind {
	return compositeResRowType
}

func (node *ShowFunctionOrProcedureStatus) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowGrants) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowTableStatus) StmtKind() StmtKind {
	//FIXME: result row,prebuild result row, in backend
	return MakeStmtKind(OUTPUT_RESULT_ROW, RESP_MIXED_RESULT_ROW, EXEC_IN_ENGINE)
}

func (node *ExplainStmt) StmtKind() StmtKind {
	return compositeResRowType
}

func (node *ExplainAnalyze) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ExplainFor) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowColumns) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowStatus) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowWarnings) StmtKind() StmtKind {
	return compositeResRowType
}

func (node *ShowErrors) StmtKind() StmtKind {
	return compositeResRowType
}

func (node *ShowProcessList) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowCreateDatabase) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowStages) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowCreatePublications) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowPublications) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowTableSize) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowRolesStmt) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowConnectors) StmtKind() StmtKind {
	return compositeResRowType
}

func (node *AlterTable) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *CreateConnector) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *DropTable) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *Load) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *AlterView) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *TruncateTable) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *Delete) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *SetVar) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *Replace) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *CreateIndex) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *DropDatabase) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *SetDefaultRole) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *SetPassword) StmtKind() StmtKind {
	return MakeStmtKind(OUTPUT_STATUS, RESP_STATUS, EXEC_IN_FRONTEND)
}

func (node *DropIndex) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *AnalyzeStmt) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *SetRole) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *Do) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *Declare) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *CreateExtension) StmtKind() StmtKind {
	return MakeStmtKind(OUTPUT_UNDEFINED, RESP_STATUS, EXEC_IN_FRONTEND)
}

func (node *LoadExtension) StmtKind() StmtKind {
	return MakeStmtKind(OUTPUT_UNDEFINED, RESP_STATUS, EXEC_IN_FRONTEND)
}

func (node *ValuesStatement) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *MoDump) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *CreateSequence) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *AlterSequence) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *Reset) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *DropConnector) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *ResumeDaemonTask) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *CancelDaemonTask) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *PauseDaemonTask) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *PrepareString) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *PrepareStmt) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *DropView) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *ShowCreateView) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *DropSequence) StmtKind() StmtKind {
	return defaultStatusTyp
}

func (node *ShowTableNumber) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowColumnNumber) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowTableValues) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowNodeList) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowLocks) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *ShowSequences) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *CreateSnapShot) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *DropSnapShot) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *ShowSnapShots) StmtKind() StmtKind {
	return defaultResRowTyp
}

func (node *UpgradeStatement) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *RestoreSnapShot) StmtKind() StmtKind {
	return frontendStatusTyp
}
