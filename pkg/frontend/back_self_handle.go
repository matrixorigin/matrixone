// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func execInFrontendInBack(backSes *backSession,
	execCtx *ExecCtx) (err error) {
	execCtx.ses.EnterFPrint(FPExecInFrontEndInBack)
	defer execCtx.ses.ExitFPrint(FPExecInFrontEndInBack)
	//check transaction states
	switch st := execCtx.stmt.(type) {
	case *tree.BeginTransaction:
	case *tree.CommitTransaction:
	case *tree.RollbackTransaction:
	case *tree.Use:
		execCtx.ses.EnterFPrint(FPInBackUse)
		defer execCtx.ses.ExitFPrint(FPInBackUse)
		err = handleChangeDB(backSes, execCtx, st.Name.Compare())
		if err != nil {
			return
		}
	case *tree.CreateDatabase:
		execCtx.ses.EnterFPrint(FPInBackCreateDatabase)
		defer execCtx.ses.ExitFPrint(FPInBackCreateDatabase)
		err = inputNameIsInvalid(execCtx.reqCtx, string(st.Name))
		if err != nil {
			return
		}
		if st.SubscriptionOption != nil && backSes.tenant != nil && !backSes.tenant.IsAdminRole() {
			err = moerr.NewInternalError(execCtx.reqCtx, "only admin can create subscription")
			return
		}
		st.Sql = execCtx.sqlOfStmt
	case *tree.DropDatabase:
		execCtx.ses.EnterFPrint(FPInBackDropDatabase)
		defer execCtx.ses.ExitFPrint(FPInBackDropDatabase)
		err = inputNameIsInvalid(execCtx.reqCtx, string(st.Name))
		if err != nil {
			return
		}
		// if the droped database is the same as the one in use, database must be reseted to empty.
		if string(st.Name) == backSes.GetDatabaseName() {
			backSes.SetDatabaseName("")
		}
	case *tree.Grant:
		execCtx.ses.EnterFPrint(FPInBackGrant)
		defer execCtx.ses.ExitFPrint(FPInBackGrant)
		switch st.Typ {
		case tree.GrantTypeRole:
			if err = handleGrantRole(backSes, execCtx, &st.GrantRole); err != nil {
				return
			}
		case tree.GrantTypePrivilege:
			if err = handleGrantPrivilege(backSes, execCtx, &st.GrantPrivilege); err != nil {
				return
			}
		}
	case *tree.Revoke:
		execCtx.ses.EnterFPrint(FPInBackRevoke)
		defer execCtx.ses.ExitFPrint(FPInBackRevoke)
		switch st.Typ {
		case tree.RevokeTypeRole:
			if err = handleRevokeRole(backSes, execCtx, &st.RevokeRole); err != nil {
				return
			}
		case tree.RevokeTypePrivilege:
			if err = handleRevokePrivilege(backSes, execCtx, &st.RevokePrivilege); err != nil {
				return
			}
		}
	case *tree.EmptyStmt:
		if err = handleEmptyStmt(backSes, execCtx, st); err != nil {
			return
		}
	default:
		return moerr.NewInternalErrorf(execCtx.reqCtx, "backExec does not support %s", execCtx.sqlOfStmt)
	}
	return
}
