// Copyright 2022 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var MoLocksColNames = []string{
	//"cn_id",
	//"session_id",
	"txn_id",
	"table_id",
	//"table_name",
	"lock_key",
	"lock_content",
	"lock_mode",
	"lock_status",
	"lock_wait",
}

var MoLocksColTypes = []types.Type{
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	//types.New(types.T_varchar, types.MaxVarcharLen, 0),
	//types.New(types.T_varchar, types.MaxVarcharLen, 0),
	//types.New(types.T_varchar, types.MaxVarcharLen, 0),
}

var MoLocksColName2Index = map[string]int32{
	"txn_id":       0,
	"table_id":     1,
	"lock_key":     2,
	"lock_content": 3,
	"lock_mode":    4,
	"lock_status":  5,
	"lock_wait":    6,
}

type MoLocksColType int32

const (
	//MoLocksColTypeCnId = iota
	//MoLocksColTypeSessionId
	MoLocksColTypeTxnId = iota
	MoLocksColTypeTableId
	//MoLocksColTypeTableName
	MoLocksColTypeLockKey
	MoLocksColTypeLockContent
	MoLocksColTypeLockMode
	MoLocksColTypeLockStatus
	MoLocksColTypeLockWait
)

func (builder *QueryBuilder) buildMoLocks(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {
	var err error

	colDefs := make([]*plan.ColDef, 0, len(MoLocksColNames))

	for i, name := range MoLocksColNames {
		colDefs = append(colDefs, &plan.ColDef{
			Name: name,
			Typ: &plan.Type{
				Id:    int32(MoLocksColTypes[i].Oid),
				Width: MoLocksColTypes[i].Width,
			},
		})
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "mo_locks",
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), err
}
