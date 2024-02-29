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
