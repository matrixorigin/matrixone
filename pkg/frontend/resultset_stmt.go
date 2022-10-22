package frontend

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// TODO: special handle for export
type SelectExecutor struct {
	*resultSetStmtExecutor
	sel *tree.Select
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

type ShowDatabasesExecutor struct {
	*resultSetStmtExecutor
	sd *tree.ShowDatabases
}

type ShowColumnsExecutor struct {
	*resultSetStmtExecutor
	sc *tree.ShowColumns
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

type ShowVariablesExecutor struct {
	*resultSetStmtExecutor
	sv *tree.ShowVariables
}

func (sve *ShowVariablesExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	//TODO
	return nil
}

type ShowErrorsExecutor struct {
	*resultSetStmtExecutor
	se *tree.ShowErrors
}

type ShowWarningsExecutor struct {
	*resultSetStmtExecutor
	sw *tree.ShowWarnings
}

type AnalyzeStmtExecutor struct {
	*resultSetStmtExecutor
	as *tree.AnalyzeStmt
}

type ExplainAnalyzeExecutor struct {
	*resultSetStmtExecutor
	ea *tree.ExplainAnalyze
}

type InternalCmdFieldListExecutor struct {
	*resultSetStmtExecutor
	icfl *InternalCmdFieldList
}
