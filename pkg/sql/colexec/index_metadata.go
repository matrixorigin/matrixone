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

package colexec

import (
	"context"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	ALLOCID_INDEX_KEY = "index_key"
)
const (
	// 'mo_indexes' table
	MO_INDEX_ID          = "id"
	MO_INDEX_TABLE_ID    = "table_id"
	MO_INDEX_DATABASE_ID = "database_id"
	MO_INDEX_NAME        = "name"
	// MO_INDEX_TYPE can have values : unique, primary or multiple.
	// It is better called as MO_INDEX_CATEGORY. But for now, we will keep it as MO_INDEX_TYPE.
	// The INDEX_TYPE in MYSQL has values BTREE, FULLTEXT, HASH, RTREE
	// Ref: https://dev.mysql.com/doc/mysql-infoschema-excerpt/5.7/en/information-schema-statistics-table.html
	MO_INDEX_TYPE                 = "type"
	MO_INDEX_ALGORITHM            = "algo"
	MO_INDEX_ALGORITHM_TABLE_TYPE = "algo_table_type"
	MO_INDEX_ALGORITHM_PARAMS     = "algo_params"
	MO_INDEX_IS_VISIBLE           = "is_visible"
	MO_INDEX_HIDDEN               = "hidden"
	MO_INDEX_COMMENT              = "comment"
	MO_INDEX_OPTIONS              = "options"
	MO_INDEX_COLUMN_NAME          = "column_name"
	MO_INDEX_ORDINAL_POSITION     = "ordinal_position"
	MO_INDEX_TABLE_NAME           = "index_table_name"
	MO_INDEX_PRIKEY               = catalog.CPrimaryKeyColName
)

// Column type mapping of table 'mo_indexes'
var MO_INDEX_COLTYPE = map[string]types.T{
	MO_INDEX_ID:                   types.T_uint64,
	MO_INDEX_TABLE_ID:             types.T_uint64,
	MO_INDEX_DATABASE_ID:          types.T_uint64,
	MO_INDEX_NAME:                 types.T_varchar,
	MO_INDEX_TYPE:                 types.T_varchar,
	MO_INDEX_ALGORITHM:            types.T_varchar,
	MO_INDEX_ALGORITHM_TABLE_TYPE: types.T_varchar,
	MO_INDEX_ALGORITHM_PARAMS:     types.T_varchar,
	MO_INDEX_IS_VISIBLE:           types.T_int8,
	MO_INDEX_HIDDEN:               types.T_int8,
	MO_INDEX_COMMENT:              types.T_varchar,
	MO_INDEX_COLUMN_NAME:          types.T_varchar,
	MO_INDEX_ORDINAL_POSITION:     types.T_uint32,
	MO_INDEX_OPTIONS:              types.T_text,
	MO_INDEX_TABLE_NAME:           types.T_varchar,
	MO_INDEX_PRIKEY:               types.T_varchar,
}

const (
	INDEX_TYPE_PRIMARY  = "PRIMARY"
	INDEX_TYPE_UNIQUE   = "UNIQUE"
	INDEX_TYPE_MULTIPLE = "MULTIPLE"
)

// InsertIndexMetadata :Synchronize the index metadata information of the table to the index metadata table
func InsertIndexMetadata(eg engine.Engine, ctx context.Context, db engine.Database, proc *process.Process, tblName string) error {
	databaseId, err := strconv.ParseUint(db.GetDatabaseId(ctx), 10, 64)
	if err != nil {
		return moerr.NewInternalError(ctx, "The databaseid of '%v' is not a valid number", databaseId)
	}

	relation, err := db.Relation(ctx, tblName, nil)
	if err != nil {
		return err
	}
	tableId := relation.GetTableID(ctx)

	tableDefs, err := relation.TableDefs(ctx)
	if err != nil {
		return err
	}
	var ct *engine.ConstraintDef
	for _, def := range tableDefs {
		if constraintDef, ok := def.(*engine.ConstraintDef); ok {
			ct = constraintDef
			break
		}
	}
	if ct == nil {
		return nil
	}

	hasIndex := false
	for _, constraint := range ct.Cts {
		if _, ok := constraint.(*engine.IndexDef); ok {
			hasIndex = true
			break
		}
		if pkdef, ok := constraint.(*engine.PrimaryKeyDef); ok {
			if pkdef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
				hasIndex = true
				break
			}
		}
	}

	if !hasIndex {
		return nil
	}

	relIndex, err := GetNewRelation(eg, catalog.MO_CATALOG, catalog.MO_INDEXES, proc.TxnOperator, ctx)
	if err != nil {
		return err
	}

	indexMetaBatch, err := buildInsertIndexMetaBatch(tableId, databaseId, ct, eg, proc)
	if err != nil {
		return err
	}
	err = relIndex.Write(ctx, indexMetaBatch)
	if err != nil {
		return err
	}
	return nil
}

// InsertOneIndexMetadata :Synchronize the single index metadata information into the index metadata table
func InsertOneIndexMetadata(eg engine.Engine, ctx context.Context, db engine.Database, proc *process.Process, tblName string, idxdef *plan.IndexDef) error {
	databaseId, err := strconv.ParseUint(db.GetDatabaseId(ctx), 10, 64)
	if err != nil {
		return moerr.NewInternalError(ctx, "The databaseid of '%v' is not a valid number", databaseId)
	}
	relation, err := db.Relation(ctx, tblName, nil)
	if err != nil {
		return err
	}
	tableId := relation.GetTableID(ctx)
	relIndex, err := GetNewRelation(eg, catalog.MO_CATALOG, catalog.MO_INDEXES, proc.TxnOperator, ctx)
	if err != nil {
		return err
	}

	ct := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.IndexDef{
				Indexes: []*plan.IndexDef{idxdef},
			},
		},
	}

	indexMetaBatch, err := buildInsertIndexMetaBatch(tableId, databaseId, ct, eg, proc)
	if err != nil {
		return err
	}
	err = relIndex.Write(ctx, indexMetaBatch)
	if err != nil {
		return err
	}
	return nil
}

func buildInsertIndexMetaBatch(tableId uint64, databaseId uint64, ct *engine.ConstraintDef, eg engine.Engine, proc *process.Process) (*batch.Batch, error) {
	bat := &batch.Batch{
		Attrs: make([]string, 16),
		Vecs:  make([]*vector.Vector, 16),
		Cnt:   1,
	}
	bat.Attrs[0] = MO_INDEX_ID
	bat.Attrs[1] = MO_INDEX_TABLE_ID
	bat.Attrs[2] = MO_INDEX_DATABASE_ID
	bat.Attrs[3] = MO_INDEX_NAME
	bat.Attrs[4] = MO_INDEX_TYPE
	bat.Attrs[5] = MO_INDEX_ALGORITHM
	bat.Attrs[6] = MO_INDEX_ALGORITHM_TABLE_TYPE
	bat.Attrs[7] = MO_INDEX_ALGORITHM_PARAMS
	bat.Attrs[8] = MO_INDEX_IS_VISIBLE
	bat.Attrs[9] = MO_INDEX_HIDDEN
	bat.Attrs[10] = MO_INDEX_COMMENT
	bat.Attrs[11] = MO_INDEX_COLUMN_NAME
	bat.Attrs[12] = MO_INDEX_ORDINAL_POSITION
	bat.Attrs[13] = MO_INDEX_OPTIONS
	bat.Attrs[14] = MO_INDEX_TABLE_NAME
	bat.Attrs[15] = MO_INDEX_PRIKEY

	vec_id := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_ID].ToType())
	bat.Vecs[0] = vec_id

	vec_table_id := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_TABLE_ID].ToType())
	bat.Vecs[1] = vec_table_id

	vec_database_id := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_DATABASE_ID].ToType())
	bat.Vecs[2] = vec_database_id

	vec_name := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_NAME].ToType())
	bat.Vecs[3] = vec_name

	vec_type := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_TYPE].ToType())
	bat.Vecs[4] = vec_type

	vec_algo := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_ALGORITHM].ToType())
	bat.Vecs[5] = vec_algo

	vec_algo_table_type := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_ALGORITHM_TABLE_TYPE].ToType())
	bat.Vecs[6] = vec_algo_table_type

	vec_algo_params := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_ALGORITHM_PARAMS].ToType())
	bat.Vecs[7] = vec_algo_params

	vec_visible := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_IS_VISIBLE].ToType())
	bat.Vecs[8] = vec_visible

	vec_hidden := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_HIDDEN].ToType())
	bat.Vecs[9] = vec_hidden

	vec_comment := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_COMMENT].ToType())
	bat.Vecs[10] = vec_comment

	vec_column_name := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_COLUMN_NAME].ToType())
	bat.Vecs[11] = vec_column_name

	vec_ordinal_position := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_ORDINAL_POSITION].ToType())
	bat.Vecs[12] = vec_ordinal_position

	vec_options := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_OPTIONS].ToType())
	bat.Vecs[13] = vec_options

	vec_index_table := proc.GetVector(MO_INDEX_COLTYPE[MO_INDEX_TABLE_NAME].ToType())
	bat.Vecs[14] = vec_index_table

	var indexId uint64
	var err error
	defer func() {
		if err != nil {
			bat.Clean(proc.Mp())
		}
	}()

	for _, constraint := range ct.Cts {
		switch def := constraint.(type) {
		case *engine.IndexDef:
			for _, index := range def.Indexes {
				ctx, cancelFunc := context.WithTimeout(proc.Ctx, time.Second*30)

				indexId, err = eg.AllocateIDByKey(ctx, ALLOCID_INDEX_KEY)
				cancelFunc()
				if err != nil {
					return nil, err
				}

				for i, part := range index.Parts {
					//NOTE: buildInsertIndexMetaBatch function is used in UT only.
					part = catalog.ResolveAlias(part)
					err = vector.AppendFixed(vec_id, indexId, false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendFixed(vec_table_id, tableId, false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendFixed(vec_database_id, databaseId, false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendBytes(vec_name, []byte(index.IndexName), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					if index.Unique {
						err = vector.AppendBytes(vec_type, []byte(INDEX_TYPE_UNIQUE), false, proc.Mp())
					} else {
						err = vector.AppendBytes(vec_type, []byte(INDEX_TYPE_MULTIPLE), false, proc.Mp())
					}
					if err != nil {
						return nil, err
					}

					if len(index.IndexAlgo) == 0 {
						err = vector.AppendBytes(vec_algo, []byte(""), true, proc.Mp())
					} else {
						err = vector.AppendBytes(vec_algo, []byte(index.IndexAlgo), false, proc.Mp())
					}
					if err != nil {
						return nil, err
					}

					if len(index.IndexAlgoTableType) == 0 {
						err = vector.AppendBytes(vec_algo_table_type, []byte(""), true, proc.Mp())
					} else {
						err = vector.AppendBytes(vec_algo_table_type, []byte(index.IndexAlgoTableType), false, proc.Mp())
					}
					if err != nil {
						return nil, err
					}

					if len(index.IndexAlgoParams) == 0 {
						err = vector.AppendBytes(vec_algo_params, []byte(""), true, proc.Mp())
					} else {
						err = vector.AppendBytes(vec_algo_params, []byte(index.IndexAlgoParams), false, proc.Mp())
					}
					if err != nil {
						return nil, err
					}

					err = vector.AppendFixed(vec_visible, int8(1), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendFixed(vec_hidden, int8(0), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendBytes(vec_comment, []byte(index.Comment), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendBytes(vec_column_name, []byte(part), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendFixed(vec_ordinal_position, uint32(i+1), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendBytes(vec_options, []byte(""), true, proc.Mp())
					if err != nil {
						return nil, err
					}
					if index.TableExist {
						err = vector.AppendBytes(vec_index_table, []byte(index.IndexTableName), false, proc.Mp())
					} else {
						err = vector.AppendBytes(vec_index_table, []byte(""), true, proc.Mp())
					}
					if err != nil {
						return nil, err
					}
				}
			}
		case *engine.PrimaryKeyDef:
			ctx, cancelFunc := context.WithTimeout(proc.Ctx, time.Second*30)
			defer cancelFunc()
			indexId, err = eg.AllocateIDByKey(ctx, ALLOCID_INDEX_KEY)
			if err != nil {
				return nil, err
			}
			if def.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
				for i, colName := range def.Pkey.Names {
					err = vector.AppendFixed(vec_id, indexId, false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendFixed(vec_table_id, tableId, false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendFixed(vec_database_id, databaseId, false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendBytes(vec_name, []byte("PRIMARY"), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendBytes(vec_type, []byte(INDEX_TYPE_PRIMARY), false, proc.Mp())
					if err != nil {
						return nil, err
					}

					err = vector.AppendBytes(vec_algo, []byte(""), true, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendBytes(vec_algo_table_type, []byte(""), true, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendBytes(vec_algo_params, []byte(""), true, proc.Mp())
					if err != nil {
						return nil, err
					}

					err = vector.AppendFixed(vec_visible, int8(1), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendFixed(vec_hidden, int8(0), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendBytes(vec_comment, []byte(""), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendBytes(vec_column_name, []byte(colName), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendFixed(vec_ordinal_position, uint32(i+1), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendBytes(vec_options, []byte(""), true, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vector.AppendBytes(vec_index_table, []byte(""), true, proc.Mp())
					if err != nil {
						return nil, err
					}
				}
			}

		}
	}

	// processing composite primary key
	var vecPrikey *vector.Vector
	vecPrikey, err = function.RunFunctionDirectly(proc, function.SerialFunctionEncodeID, []*vector.Vector{vec_id, vec_column_name}, vec_id.Length())
	if err != nil {
		return nil, err
	}
	bat.Vecs[12] = vecPrikey

	bat.SetRowCount(bat.GetVector(0).Length())
	return bat, nil
}

func GetNewRelation(eg engine.Engine, dbName, tbleName string, txn client.TxnOperator, ctx context.Context) (engine.Relation, error) {
	dbHandler, err := eg.Database(ctx, dbName, txn)
	if err != nil {
		return nil, err
	}
	tableHandler, err := dbHandler.Relation(ctx, tbleName, nil)
	if err != nil {
		return nil, err
	}
	return tableHandler, nil
}
