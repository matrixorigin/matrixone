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
	"cn_id",
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
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	//types.New(types.T_varchar, types.MaxVarcharLen, 0),
	//types.New(types.T_varchar, types.MaxVarcharLen, 0),
}

var MoLocksColName2Index = map[string]int32{
	"cn_id":        0,
	"txn_id":       1,
	"table_id":     2,
	"lock_key":     3,
	"lock_content": 4,
	"lock_mode":    5,
	"lock_status":  6,
	"lock_wait":    7,
}

type MoLocksColType int32

const (
	MoLocksColTypeCnId = iota
	//MoLocksColTypeSessionId
	MoLocksColTypeTxnId
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
			Typ: plan.Type{
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

var MoConfigColNames = []string{
	"node_type",
	"node_id",
	"name",
	"current_value",
	"default_value",
	"internal",
}

var MoConfigColTypes = []types.Type{
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
}

var MoConfigColName2Index = map[string]int32{
	"node_type":     0,
	"node_id":       1,
	"name":          2,
	"current_value": 3,
	"default_value": 4,
	"internal":      5,
}

type MoConfigColType int32

const (
	MoConfigColTypeNodeType = iota
	MoConfigColTypeNodeId
	MoConfigColTypeName
	MoConfigColTypeCurrentValue
	MoConfigColTypeDefaultValue
	MoConfigColTypeInternal
)

func (builder *QueryBuilder) buildMoConfigurations(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {
	var err error

	colDefs := make([]*plan.ColDef, 0, len(MoConfigColNames))

	for i, name := range MoConfigColNames {
		colDefs = append(colDefs, &plan.ColDef{
			Name: name,
			Typ: plan.Type{
				Id:    int32(MoConfigColTypes[i].Oid),
				Width: MoConfigColTypes[i].Width,
			},
		})
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "mo_configurations",
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), err
}

var MoTransactionsColNames = []string{
	"cn_id",
	"txn_id",
	"create_ts",
	"snapshot_ts",
	"prepared_ts",
	"commit_ts",
	"txn_mode",
	"isolation",
	"user_txn",
	"txn_status",
	"table_id",
	"lock_key",
	"lock_content",
	"lock_mode",
}

var MoTransactionsColTypes = []types.Type{
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
}

var MoTransactionsColName2Index = map[string]int32{
	"cn_id":        0,
	"txn_id":       1,
	"create_ts":    2,
	"snapshot_ts":  3,
	"prepared_ts":  4,
	"commit_ts":    5,
	"txn_mode":     6,
	"isolation":    7,
	"user_txn":     8,
	"txn_status":   9, //based on wait locks list
	"table_id":     10,
	"lock_key":     11,
	"lock_content": 12,
	"lock_mode":    13,
}

type MoTransactionsColType int32

const (
	MoTransactionsColTypeCnId = iota
	MoTransactionsColTypeTxnId
	MoTransactionsColTypeCreateTs
	MoTransactionsColTypeSnapshotTs
	MoTransactionsColTypePreparedTs
	MoTransactionsColTypeCommitTs
	MoTransactionsColTypeTxnMode
	MoTransactionsColTypeIsolation
	MoTransactionsColTypeUserTxn
	MoTransactionsColTypeTxnStatus
	MoTransactionsColTypeTableId
	MoTransactionsColTypeLockKey
	MoTransactionsColTypeLockContent
	MoTransactionsColTypeLockMode
)

func (builder *QueryBuilder) buildMoTransactions(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {
	var err error

	colDefs := make([]*plan.ColDef, 0, len(MoTransactionsColNames))

	for i, name := range MoTransactionsColNames {
		colDefs = append(colDefs, &plan.ColDef{
			Name: name,
			Typ: plan.Type{
				Id:    int32(MoTransactionsColTypes[i].Oid),
				Width: MoTransactionsColTypes[i].Width,
			},
		})
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "mo_transactions",
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), err
}

var MoCacheColNames = []string{
	"node_type",
	"node_id",
	"type",
	"used",
	"free",
	"hit_ratio",
}

var MoCacheColTypes = []types.Type{
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_uint64, 0, 0),
	types.New(types.T_uint64, 0, 0),
	types.New(types.T_float32, 0, 0),
}

var MoCacheColName2Index = map[string]int32{
	"node_type": 0,
	"node_id":   1,
	"type":      2,
	"used":      3,
	"free":      4,
	"hit_ratio": 5,
}

type MoCacheColType int32

const (
	MoCacheColTypeNodeType = iota
	MoCacheColTypeNodeId
	MoCacheColTypeType
	MoCacheColTypeUsed
	MoCacheColTypeFree
	MoCacheColTypeHitRatio
)

func (builder *QueryBuilder) buildMoCache(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {
	var err error

	colDefs := make([]*plan.ColDef, 0, len(MoCacheColNames))

	for i, name := range MoCacheColNames {
		colDefs = append(colDefs, &plan.ColDef{
			Name: name,
			Typ: plan.Type{
				Id:    int32(MoCacheColTypes[i].Oid),
				Width: MoCacheColTypes[i].Width,
			},
		})
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "mo_cache",
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), err
}
