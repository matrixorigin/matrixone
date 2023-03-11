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
	// 'mo_indexes' table
	MO_INDEX_ID               = "id"
	MO_INDEX_TABLE_ID         = "table_id"
	MO_INDEX_NAME             = "name"
	MO_INDEX_TYPE             = "type"
	MO_INDEX_IS_VISIBLE       = "is_visible"
	MO_INDEX_HIDDEN           = "hidden"
	MO_INDEX_COMMENT          = "comment"
	MO_INDEX_OPTIONS          = "options"
	MO_INDEX_COLUMN_NAME      = "column_name"
	MO_INDEX_ORDINAL_POSITION = "ordinal_position"
	MO_INDEX_TABLE_NAME       = "index_table_name"
)

// Column type mapping of table 'mo_indexes'
var MO_INDEX_COLTYPE = map[string]types.T{
	MO_INDEX_ID:               types.T_uint64,
	MO_INDEX_TABLE_ID:         types.T_uint64,
	MO_INDEX_NAME:             types.T_varchar,
	MO_INDEX_TYPE:             types.T_varchar,
	MO_INDEX_IS_VISIBLE:       types.T_int8,
	MO_INDEX_HIDDEN:           types.T_int8,
	MO_INDEX_COMMENT:          types.T_varchar,
	MO_INDEX_COLUMN_NAME:      types.T_varchar,
	MO_INDEX_ORDINAL_POSITION: types.T_uint32,
	MO_INDEX_OPTIONS:          types.T_text,
	MO_INDEX_TABLE_NAME:       types.T_varchar,
}

const (
	INDEX_TYPE_PRIMARY  = "PRIMARY"
	INDEX_TYPE_UNIQUE   = "UNIQUE"
	INDEX_TYPE_MULTIPLE = "MULTIPLE"
)

// InsertIndexMetadata :Synchronize the index metadata information of the table to the index metadata table
func InsertIndexMetadata(eg engine.Engine, ctx context.Context, db engine.Database, proc *process.Process, tblName string) error {
	relation, err := db.Relation(ctx, tblName)
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
		if _, ok := constraint.(*engine.PrimaryKeyDef); ok {
			hasIndex = true
			break
		}
	}

	if !hasIndex {
		return nil
	}

	txn, err := NewTxn(eg, proc, ctx)
	if err != nil {
		return err
	}
	relIndex, err := GetNewRelation(eg, catalog.MO_CATALOG, catalog.MO_INDEXES, txn, ctx)
	if err != nil {
		return err
	}

	indexMetaBatch, err := buildInsertIndexMetaBatch(tableId, ct, eg, proc)
	if err != nil {
		return err
	}
	err = relIndex.Write(ctx, indexMetaBatch)
	if err != nil {
		if err2 := RolllbackTxn(eg, txn, ctx); err2 != nil {
			return err2
		}
		return err
	}
	if err = CommitTxn(eg, txn, ctx); err != nil {
		return err
	}
	return nil
}

// InsertOneIndexMetadata :Synchronize the single index metadata information into the index metadata table
func InsertOneIndexMetadata(eg engine.Engine, ctx context.Context, db engine.Database, proc *process.Process, tblName string, idxdef *plan.IndexDef) error {
	relation, err := db.Relation(ctx, tblName)
	if err != nil {
		return err
	}
	tableId := relation.GetTableID(ctx)
	txn, err := NewTxn(eg, proc, ctx)
	if err != nil {
		return err
	}
	relIndex, err := GetNewRelation(eg, catalog.MO_CATALOG, catalog.MO_INDEXES, txn, ctx)
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

	indexMetaBatch, err := buildInsertIndexMetaBatch(tableId, ct, eg, proc)
	if err != nil {
		return err
	}
	err = relIndex.Write(ctx, indexMetaBatch)
	if err != nil {
		if err2 := RolllbackTxn(eg, txn, ctx); err2 != nil {
			return err2
		}
		return err
	}
	if err = CommitTxn(eg, txn, ctx); err != nil {
		return err
	}
	return nil
}

// DeleteIndexMetadata :When dropping a table, delete all index metadata information of the table
func DeleteIndexMetadata(eg engine.Engine, ctx context.Context, relation engine.Relation, proc *process.Process) error {
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
		if indexdef, ok := constraint.(*engine.IndexDef); ok && len(indexdef.Indexes) != 0 {
			hasIndex = true
			break
		}
		if _, ok := constraint.(*engine.PrimaryKeyDef); ok {
			hasIndex = true
			break
		}
	}

	if !hasIndex {
		return nil
	}

	txn, err := NewTxn(eg, proc, ctx)
	if err != nil {
		return err
	}
	relIndex, err := GetNewRelation(eg, catalog.MO_CATALOG, catalog.MO_INDEXES, txn, ctx)
	if err != nil {
		return err
	}
	indexMetaBatch, err := buildDeleteIndexBatch(tableId, relIndex, ctx, proc)
	if err != nil {
		return err
	}
	if indexMetaBatch.GetVector(0).Length() > 0 {
		err = relIndex.Delete(ctx, indexMetaBatch, catalog.Row_ID)
		if err != nil {
			if err2 := RolllbackTxn(eg, txn, ctx); err2 != nil {
				return err2
			}
			return err
		}
	}
	if err = CommitTxn(eg, txn, ctx); err != nil {
		return err
	}
	return nil
}

// When the drop index is executed, delete the metadata information of the corresponding index of the table
func DeleteOneIndexMetadata(eg engine.Engine, ctx context.Context, relation engine.Relation, indexName string, proc *process.Process) error {
	tableId := relation.GetTableID(ctx)

	txn, err := NewTxn(eg, proc, ctx)
	if err != nil {
		return err
	}

	relIndex, err := GetNewRelation(eg, catalog.MO_CATALOG, catalog.MO_INDEXES, txn, ctx)
	if err != nil {
		return err
	}

	indexMetaBatch, err := buildDeleteSingleIndexBatch(tableId, indexName, relIndex, ctx, proc)
	if err != nil {
		return err
	}

	if indexMetaBatch.GetVector(0).Length() > 0 {
		err = relIndex.Delete(ctx, indexMetaBatch, catalog.Row_ID)
		if err != nil {
			if err2 := RolllbackTxn(eg, txn, ctx); err2 != nil {
				return err2
			}
			return err
		}
	}
	if err = CommitTxn(eg, txn, ctx); err != nil {
		return err
	}
	return nil
}

func buildInsertIndexMetaBatch(tableId uint64, ct *engine.ConstraintDef, eg engine.Engine, proc *process.Process) (*batch.Batch, error) {
	bat := &batch.Batch{
		Attrs: make([]string, 11),
		Vecs:  make([]*vector.Vector, 11),
		Cnt:   1,
	}
	bat.Attrs[0] = MO_INDEX_ID
	bat.Attrs[1] = MO_INDEX_TABLE_ID
	bat.Attrs[2] = MO_INDEX_NAME
	bat.Attrs[3] = MO_INDEX_TYPE
	bat.Attrs[4] = MO_INDEX_IS_VISIBLE
	bat.Attrs[5] = MO_INDEX_HIDDEN
	bat.Attrs[6] = MO_INDEX_COMMENT
	bat.Attrs[7] = MO_INDEX_COLUMN_NAME
	bat.Attrs[8] = MO_INDEX_ORDINAL_POSITION
	bat.Attrs[9] = MO_INDEX_OPTIONS
	bat.Attrs[10] = MO_INDEX_TABLE_NAME

	vec_id := vector.New(MO_INDEX_COLTYPE[MO_INDEX_ID].ToType())
	bat.Vecs[0] = vec_id

	vec_table_id := vector.New(MO_INDEX_COLTYPE[MO_INDEX_TABLE_ID].ToType())
	bat.Vecs[1] = vec_table_id

	vec_name := vector.New(MO_INDEX_COLTYPE[MO_INDEX_NAME].ToType())
	bat.Vecs[2] = vec_name

	vec_type := vector.New(MO_INDEX_COLTYPE[MO_INDEX_TYPE].ToType())
	bat.Vecs[3] = vec_type

	vec_visible := vector.New(MO_INDEX_COLTYPE[MO_INDEX_IS_VISIBLE].ToType())
	bat.Vecs[4] = vec_visible

	vec_hidden := vector.New(MO_INDEX_COLTYPE[MO_INDEX_HIDDEN].ToType())
	bat.Vecs[5] = vec_hidden

	vec_comment := vector.New(MO_INDEX_COLTYPE[MO_INDEX_COMMENT].ToType())
	bat.Vecs[6] = vec_comment

	vec_column_name := vector.New(MO_INDEX_COLTYPE[MO_INDEX_COLUMN_NAME].ToType())
	bat.Vecs[7] = vec_column_name

	vec_ordinal_position := vector.New(MO_INDEX_COLTYPE[MO_INDEX_ORDINAL_POSITION].ToType())
	bat.Vecs[8] = vec_ordinal_position

	vec_options := vector.New(MO_INDEX_COLTYPE[MO_INDEX_OPTIONS].ToType())
	bat.Vecs[9] = vec_options

	vec_index_table := vector.New(MO_INDEX_COLTYPE[MO_INDEX_TABLE_NAME].ToType())
	bat.Vecs[10] = vec_index_table

	for _, constraint := range ct.Cts {
		switch def := constraint.(type) {
		case *engine.IndexDef:
			for _, index := range def.Indexes {
				indexId, err := eg.AllocateID(proc.Ctx)
				if err != nil {
					return nil, err
				}

				for i, part := range index.Parts {
					err = vec_id.Append(indexId, false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vec_table_id.Append(tableId, false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vec_name.Append([]byte(index.IndexName), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					if index.Unique {
						err = vec_type.Append([]byte(INDEX_TYPE_UNIQUE), false, proc.Mp())
					} else {
						err = vec_type.Append([]byte(INDEX_TYPE_MULTIPLE), false, proc.Mp())
					}
					if err != nil {
						return nil, err
					}
					err = vec_visible.Append(int8(1), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vec_hidden.Append(int8(0), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vec_comment.Append([]byte(index.Comment), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vec_column_name.Append([]byte(part), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					vec_ordinal_position.Append(uint32(i+1), false, proc.Mp())
					if err != nil {
						return nil, err
					}
					err = vec_options.Append([]byte(""), true, proc.Mp())
					if err != nil {
						return nil, err
					}
					if index.TableExist {
						err = vec_index_table.Append([]byte(index.IndexTableName), false, proc.Mp())
					} else {
						err = vec_index_table.Append([]byte(""), true, proc.Mp())
					}
					if err != nil {
						return nil, err
					}
				}
			}
		case *engine.PrimaryKeyDef:
			indexId, err := eg.AllocateID(proc.Ctx)
			if err != nil {
				return nil, err
			}
			for i, colName := range def.Pkey.Names {
				err = vec_id.Append(indexId, false, proc.Mp())
				if err != nil {
					return nil, err
				}
				err = vec_table_id.Append(tableId, false, proc.Mp())
				if err != nil {
					return nil, err
				}
				err = vec_name.Append([]byte("PRIMARY"), false, proc.Mp())
				if err != nil {
					return nil, err
				}
				err = vec_type.Append([]byte(INDEX_TYPE_PRIMARY), false, proc.Mp())
				if err != nil {
					return nil, err
				}
				err = vec_visible.Append(int8(1), false, proc.Mp())
				if err != nil {
					return nil, err
				}
				err = vec_hidden.Append(int8(0), false, proc.Mp())
				if err != nil {
					return nil, err
				}
				err = vec_comment.Append([]byte(""), false, proc.Mp())
				if err != nil {
					return nil, err
				}
				err = vec_column_name.Append([]byte(colName), false, proc.Mp())
				if err != nil {
					return nil, err
				}
				vec_ordinal_position.Append(uint32(i+1), false, proc.Mp())
				if err != nil {
					return nil, err
				}
				err = vec_options.Append([]byte(""), true, proc.Mp())
				if err != nil {
					return nil, err
				}
				err = vec_index_table.Append([]byte(""), true, proc.Mp())
				if err != nil {
					return nil, err
				}
			}
		}
	}
	bat.SetZs(vector.Length(bat.Vecs[0]), proc.Mp())
	return bat, nil
}

func buildDeleteIndexBatch(tableId uint64, rel engine.Relation, ctx context.Context, proc *process.Process) (*batch.Batch, error) {
	var rds []engine.Reader
	ret, err := rel.Ranges(ctx, nil)
	if err != nil {
		panic(err)
	}

	switch {
	case len(ret) == 0:
		rds, _ = rel.NewReader(ctx, 1, nil, nil)
	case len(ret) == 1 && len(ret[0]) == 0:
		rds, _ = rel.NewReader(ctx, 1, nil, nil)
	case len(ret[0]) == 0:
		rds0, _ := rel.NewReader(ctx, 1, nil, nil)
		rds1, _ := rel.NewReader(ctx, 1, nil, ret[1:])
		rds = append(rds, rds0...)
		rds = append(rds, rds1...)
	default:
		rds, _ = rel.NewReader(ctx, 1, nil, ret)
	}

	retbat := batch.NewWithSize(1)
	vec_rowid := vector.New(types.T_Rowid.ToType())
	retbat.Vecs[0] = vec_rowid
	for len(rds) > 0 {
		bat, err := rds[0].Read(ctx, []string{catalog.Row_ID, MO_INDEX_TABLE_ID}, nil, proc.Mp())
		if err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		if bat == nil {
			rds[0].Close()
			rds = rds[1:]
			continue
		}
		if len(bat.Vecs) != 2 {
			panic(moerr.NewInternalError(ctx, "drop all indexes of a table must return two columns batch of 'mo_indexes'"))
		}
		var rowIdx int64
		tableIdCols := vector.MustTCols[uint64](bat.Vecs[1])
		for rowIdx = 0; rowIdx < int64(bat.Length()); rowIdx++ {
			if tableIdCols[rowIdx] == tableId {
				rowid := vector.MustTCols[types.Rowid](bat.GetVector(0))[rowIdx]
				if err = retbat.Vecs[0].Append(rowid, false, proc.Mp()); err != nil {
					panic(moerr.NewInternalError(ctx, err.Error()))
				}
			}
		}
		bat.Clean(proc.Mp())
		rds[0].Close()
		rds = rds[1:]
	}
	retbat.SetZs(vector.Length(retbat.Vecs[0]), proc.Mp())
	return retbat, nil
}

func buildDeleteSingleIndexBatch(tableId uint64, indexName string, rel engine.Relation, ctx context.Context, proc *process.Process) (*batch.Batch, error) {
	var rds []engine.Reader
	ret, err := rel.Ranges(ctx, nil)
	if err != nil {
		panic(err)
	}
	switch {
	case len(ret) == 0:
		rds, _ = rel.NewReader(ctx, 1, nil, nil)
	case len(ret) == 1 && len(ret[0]) == 0:
		rds, _ = rel.NewReader(ctx, 1, nil, nil)
	case len(ret[0]) == 0:
		rds0, _ := rel.NewReader(ctx, 1, nil, nil)
		rds1, _ := rel.NewReader(ctx, 1, nil, ret[1:])
		rds = append(rds, rds0...)
		rds = append(rds, rds1...)
	default:
		rds, _ = rel.NewReader(ctx, 1, nil, ret)
	}

	retbat := batch.NewWithSize(1)
	vec_rowid := vector.New(types.T_Rowid.ToType())
	retbat.Vecs[0] = vec_rowid
	for len(rds) > 0 {
		bat, err := rds[0].Read(ctx, []string{catalog.Row_ID, MO_INDEX_TABLE_ID, MO_INDEX_NAME}, nil, proc.Mp())
		if err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		if bat == nil {
			rds[0].Close()
			rds = rds[1:]
			continue
		}
		if len(bat.Vecs) != 3 {
			panic(moerr.NewInternalError(ctx, "drop a table index must return three columns batch of 'mo_indexes'"))
		}
		var rowIdx int64
		tableIdCols := vector.MustTCols[uint64](bat.Vecs[1])
		indexNameCols := vector.MustStrCols(bat.Vecs[2])
		for rowIdx = 0; rowIdx < int64(bat.Length()); rowIdx++ {
			if tableIdCols[rowIdx] == tableId && indexNameCols[rowIdx] == indexName {
				rowid := vector.MustTCols[types.Rowid](bat.GetVector(0))[rowIdx]
				if err = retbat.Vecs[0].Append(rowid, false, proc.Mp()); err != nil {
					panic(moerr.NewInternalError(ctx, err.Error()))
				}
			}
		}
		bat.Clean(proc.Mp())
		rds[0].Close()
		rds = rds[1:]
	}
	retbat.SetZs(vector.Length(retbat.Vecs[0]), proc.Mp())
	return retbat, nil
}
