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

import "fmt"

type Statement interface {
	fmt.Stringer
	NodeFormatter
	StatementType
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

type QueryType string

const (
	QueryTypeDQL = "DQL"   // (Data Query Language): Select, modump, AnalyzeStmt, Explain, ExplainAnalyze, ShowCreateTable, ...
	QueryTypeDDL = "DDL"   // (Data Definition Language): Create/Drop/Alter/Rename Database/Table/View/Index, TruncateTable,
	QueryTypeDML = "DML"   // (Data Manipulation Language): Insert, Update, Delete, Load, Import, ValuesStatement?, With?
	QueryTypeDCL = "DCL"   // (Data Control Language): Grant, Revoke, Drop account, Set, Declare, Do, TableFunction?, Use?, Prepare, Execute, Deallocate, Kill
	QueryTypeTCL = "TCL"   // (Transaction Control Language): Begin, Savepoint, Rollback, Commit
	QueryTypeOth = "other" // syntax error
)
