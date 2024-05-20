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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// IsAdministrativeStatement checks the statement is the administrative statement.
func IsAdministrativeStatement(stmt tree.Statement) bool {
	switch st := stmt.(type) {
	case *tree.CreateAccount, *tree.DropAccount, *tree.AlterAccount,
		*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
		*tree.CreateRole, *tree.DropRole,
		*tree.Revoke, *tree.Grant,
		*tree.SetDefaultRole, *tree.SetRole, *tree.SetPassword:
		return true
	case *tree.Use:
		return st.IsUseRole()
	}
	return false
}

// IsParameterModificationStatement checks the statement is the statement of parameter modification statement.
func IsParameterModificationStatement(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.SetVar:
		return true
	}
	return false
}

// IsPrepareStatement checks the statement is the Prepare statement.
func IsPrepareStatement(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.PrepareStmt, *tree.PrepareString:
		return true
	}
	return false
}

// IsDDL checks the statement is the DDL statement.
func IsDDL(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.CreateTable, *tree.DropTable,
		*tree.CreateView, *tree.DropView, *tree.AlterView, *tree.AlterTable,
		*tree.CreateDatabase, *tree.DropDatabase, *tree.CreateSequence, *tree.DropSequence,
		*tree.CreateIndex, *tree.DropIndex, *tree.TruncateTable:
		return true
	}
	return false
}

// IsDropStatement checks the statement is the drop statement.
func IsDropStatement(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.DropDatabase, *tree.DropTable, *tree.DropView, *tree.DropIndex, *tree.DropSequence:
		return true
	}
	return false
}

func IsCreateDropDatabase(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.CreateDatabase, *tree.DropDatabase:
		return true
	}
	return false
}

func IsCreateDropSequence(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.CreateSequence, *tree.DropSequence:
		return true
	}
	return false
}

/*
NeedToBeCommittedInActiveTransaction checks the statement that need to be committed
in an active transaction.

Currently, it includes the drop statement, the administration statement ,

	the parameter modification statement.
*/
func NeedToBeCommittedInActiveTransaction(stmt tree.Statement) bool {
	if stmt == nil {
		return false
	}
	return IsCreateDropSequence(stmt) || IsAdministrativeStatement(stmt) || IsParameterModificationStatement(stmt)
}

/*
StatementCanBeExecutedInUncommittedTransaction checks the statement can be executed in an active transaction.

Cases    | set Autocommit = 1/0 | BEGIN statement |
---------------------------------------------------
Case1      1                       Yes
Case2      1                       No
Case3      0                       Yes
Case4      0                       No
---------------------------------------------------

If it is Case1,Case3, Then

	Create/Drop database reports error

If it is Case2, Then

	Create/Drop database as other statement.

If it is Case4, Then

	Create/Drop database commits current txn. a new txn for the next statement if needed.
*/
func statementCanBeExecutedInUncommittedTransaction(ctx context.Context, ses FeSession, stmt tree.Statement) (bool, error) {
	switch st := stmt.(type) {
	//ddl statement
	case *tree.CreateTable, *tree.CreateIndex, *tree.CreateView, *tree.AlterView, *tree.AlterTable:
		if createTblStmt, ok := stmt.(*tree.CreateTable); ok && createTblStmt.IsAsSelect {
			return false, nil
		}
		return true, nil
	case *tree.CreateDatabase, *tree.DropDatabase:
		return true, nil
	case *tree.CreateSequence: //Case1, Case3 above
		return ses.IsBackgroundSession() || !ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), nil
		//dml statement
	case *tree.Insert, *tree.Update, *tree.Delete, *tree.Select, *tree.Load, *tree.MoDump, *tree.ValuesStatement, *tree.Replace:
		return true, nil
		//transaction
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
		return true, nil
		//show
	case *tree.ShowCreateTable,
		*tree.ShowCreateView,
		*tree.ShowCreateDatabase,
		*tree.ShowColumns,
		*tree.ShowDatabases,
		*tree.ShowTarget,
		*tree.ShowTableStatus,
		*tree.ShowGrants,
		*tree.ShowTables,
		*tree.ShowSequences,
		*tree.ShowProcessList,
		*tree.ShowErrors,
		*tree.ShowWarnings,
		*tree.ShowCollation,
		*tree.ShowVariables,
		*tree.ShowStatus,
		*tree.ShowIndex,
		*tree.ShowFunctionOrProcedureStatus,
		*tree.ShowNodeList,
		*tree.ShowLocks,
		*tree.ShowTableNumber,
		*tree.ShowColumnNumber,
		*tree.ShowTableValues,
		*tree.ShowAccounts,
		*tree.ShowPublications,
		*tree.ShowSubscriptions,
		*tree.ShowCreatePublications,
		*tree.ShowBackendServers,
		*tree.ShowAccountUpgrade,
		*tree.ShowConnectors:
		return true, nil
		//others
	case *tree.ExplainStmt, *tree.ExplainAnalyze, *tree.ExplainFor, *InternalCmdFieldList:
		return true, nil
	case *tree.PrepareStmt:
		return statementCanBeExecutedInUncommittedTransaction(ctx, ses, st.Stmt)
	case *tree.PrepareString:
		v, err := ses.GetGlobalVar(ctx, "lower_case_table_names")
		if err != nil {
			return false, err
		}
		preStmt, err := mysql.ParseOne(ctx, st.Sql, v.(int64), 0)
		defer func() {
			preStmt.Free()
		}()
		if err != nil {
			return false, err
		}
		return statementCanBeExecutedInUncommittedTransaction(ctx, ses, preStmt)
	case *tree.Execute:
		preName := string(st.Name)
		preStmt, err := ses.GetPrepareStmt(ctx, preName)
		if err != nil {
			return false, err
		}
		return statementCanBeExecutedInUncommittedTransaction(ctx, ses, preStmt.PrepareStmt)
	case *tree.Deallocate, *tree.Reset:
		return true, nil
	case *tree.Use:
		/*
			These statements can not be executed in an uncommitted transaction:
				USE SECONDARY ROLE { ALL | NONE }
				USE ROLE role;
		*/
		return !st.IsUseRole(), nil
	case *tree.DropTable, *tree.DropIndex, *tree.DropView, *tree.TruncateTable:
		return true, nil
	case *tree.DropSequence: //Case1, Case3 above
		//background transaction can execute the DROPxxx in one transaction
		return ses.IsBackgroundSession() || !ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), nil
	case *tree.SetVar:
		return true, nil
	}

	return false, nil
}
