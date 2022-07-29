// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"strings"
)

type ComputationRunner interface {
	Run(ts uint64) (err error)
}

// ComputationWrapper is the wrapper of the computation
type ComputationWrapper interface {
	ComputationRunner
	GetAst() tree.Statement

	SetDatabaseName(db string) error

	GetColumns() ([]interface{}, error)

	GetAffectedRows() uint64

	Compile(u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error)
}

type ColumnInfo interface {
	GetName() string

	GetType() types.T
}

var _ ColumnInfo = &engineColumnInfo{}

type TableInfo interface {
	GetColumns()
}

type engineColumnInfo struct {
	name string
	typ  types.Type
}

func (ec *engineColumnInfo) GetName() string {
	return ec.name
}

func (ec *engineColumnInfo) GetType() types.T {
	return ec.typ.Oid
}

type PrepareStmt struct {
	Name        string
	PreparePlan *plan.Plan
	PrepareStmt tree.Statement
}

/*
Disguise the COMMAND CMD_FIELD_LIST as sql query.
*/
const (
	cmdFieldListSql = "__++__internal_cmd_field_list"
)

//isCmdFieldListSql checks the sql is the cmdFieldListSql or not.
func isCmdFieldListSql(sql string) bool {
	return strings.HasPrefix(strings.ToLower(sql), cmdFieldListSql)
}

//makeCmdFieldListSql makes the internal CMD_FIELD_LIST sql
func makeCmdFieldListSql(query string) string {
	return cmdFieldListSql + " " + query
}

// parseCmdFieldList parses the internal cmd field list
func parseCmdFieldList(sql string) (*InternalCmdFieldList, error) {
	if !isCmdFieldListSql(sql) {
		return nil, fmt.Errorf("it is not the CMD_FIELD_LIST")
	}
	rest := strings.TrimSpace(sql[len(cmdFieldListSql):])
	//find null
	nullIdx := strings.IndexRune(rest, rune(0))
	var tableName string
	if nullIdx < len(rest) {
		tableName = rest[:nullIdx]
		//neglect wildcard
		//wildcard := payload[nullIdx+1:]
		return &InternalCmdFieldList{tableName: tableName}, nil
	} else {
		return nil, fmt.Errorf("wrong format for COM_FIELD_LIST")
	}
}

var _ tree.Statement = &InternalCmdFieldList{}

// InternalCmdFieldList the CMD_FIELD_LIST statement
type InternalCmdFieldList struct {
	tableName string
}

func (icfl *InternalCmdFieldList) String() string {
	return makeCmdFieldListSql(icfl.tableName)
}

func (icfl *InternalCmdFieldList) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(makeCmdFieldListSql(icfl.tableName))
}
