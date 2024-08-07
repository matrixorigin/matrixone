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

package cdc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

const (
	ROWS_REAL_DATA_OFFSET int = 2
)

var _ Decoder = new(decoder)

type decoder struct {
}

func NewDecoder() Decoder {
	return &decoder{}
}

func (m *decoder) Decode(ctx context.Context, cdcCtx *disttae.TableCtx, input *disttae.DecoderInput) (out *DecoderOutput) {
	//parallel step1:decode rows
	wg := sync.WaitGroup{}
	wg.Add(1)
	err := ants.Submit(func() {
		defer wg.Done()
		it := input.State().NewRowsIterInCdc()
		rows, err := decodeRows(ctx, cdcCtx, input.TS(), it)
		if err != nil {
			return
		}
		out.sqlOfRows.Store(rows)
		defer it.Close()
	})
	if err != nil {
		panic(err)
	}
	//TODO: objects
	//parallel step2:decode objects
	//wg.Add(1)
	//err = ants.Submit(func() {
	//	defer wg.Done()
	//
	//})
	//if err != nil {
	//	panic(err)
	//}
	////parallel step3:decode deltas
	//wg.Add(1)
	//err = ants.Submit(func() {
	//	defer wg.Done()
	//
	//})
	//if err != nil {
	//	panic(err)
	//}
	wg.Wait()
	return nil
}

func decodeRows(
	ctx context.Context,
	cdcCtx *disttae.TableCtx,
	ts timestamp.Timestamp,
	rowsIter logtailreplay.RowsIter) (res [][]byte, err error) {
	//TODO: schema info
	var row []any
	//TODO:refine && limit sql size
	timePrefix := fmt.Sprintf("/* %v, %v */ ", ts.String(), time.Now())
	//---------------------------------------------------
	insertPrefix := fmt.Sprintf("INSERT INTO `%s`.`%s` values ", cdcCtx.Db(), cdcCtx.Table())
	/*
		FORMAT:
		insert into db.t values
		(...),
		...
		(...)
	*/

	//---------------------------------------------------
	/*
		DELETE FORMAT:
			mysql:
				delete from db.t where
				(pk1,..,pkn) in
				(
					(col1,..,coln),
					...
					(col1,...,coln)
				)
			matrixone:
				TODO:
	*/
	//FIXME: assume the sink is mysql
	sbuf := strings.Builder{}
	sbuf.WriteByte('(')
	tableDef := cdcCtx.TableDef()
	for i, pkColIdx := range tableDef.Pkey.Cols {
		if i > 0 {
			sbuf.WriteByte(',')
		}
		pkCol := tableDef.Cols[pkColIdx]
		sbuf.WriteString(pkCol.Name)
	}
	sbuf.WriteByte(')')
	deletePrefix := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s IN ( ", cdcCtx.Db(), cdcCtx.Table(), sbuf.String())

	//TODO: complement the
	firstInsertRow, firstDeleteRow := true, true
	insertBuff := make([]byte, 1024)
	deleteBuff := make([]byte, 1024)
	for rowsIter.Next() {
		ent := rowsIter.Entry()
		//step1 : get row from the batch
		//TODO: refine
		if row == nil {
			colCnt := len(ent.Batch.Attrs) - ROWS_REAL_DATA_OFFSET
			if colCnt <= 0 {
				return nil, moerr.NewInternalError(ctx, "invalid row entry")
			}
			row = make([]any, len(ent.Batch.Attrs))
		}
		err = extractRowFromEveryVector(ctx, ent.Batch, ROWS_REAL_DATA_OFFSET, int(ent.Offset), row)
		if err != nil {
			return nil, err
		}
		//step2 : transform rows into sql parts
		if ent.Deleted {
			//to delete
			//need primary key only
			//if the schema does not have the primary key,
			//it also has the fake primary key
			//end insert sql first
			if len(insertBuff) != 0 {
				res = append(res, copyBytes(insertBuff))
				firstInsertRow = true
				insertBuff = insertBuff[:0]
			}
			if len(deleteBuff) == 0 {
				//fill delete prefix
				deleteBuff = appendString(deleteBuff, timePrefix)
				deleteBuff = appendString(deleteBuff, deletePrefix)
			}

			if !firstDeleteRow {
				deleteBuff = appendByte(deleteBuff, ',')
			} else {
				firstDeleteRow = false
			}

			//decode primary key col from composited pk col
			comPkCol := row[2]
			pkTuple, pkTypes, err := types.UnpackWithSchema(comPkCol.([]byte))
			if err != nil {
				return nil, err
			}
			deleteBuff = appendByte(deleteBuff, '(')
			for pkIdx, pkEle := range pkTuple {
				//
				if pkIdx > 0 {
					deleteBuff = appendByte(deleteBuff, ',')
				}
				pkColIdx := tableDef.Pkey.Cols[pkIdx]
				pkCol := tableDef.Cols[pkColIdx]
				if pkTypes[pkIdx] != types.T(pkCol.Typ.Id) {
					return nil, moerr.NewInternalError(ctx, "different pk col Type %v %v", pkTypes[pkIdx], pkCol.Typ.Id)
				}
				ttype := types.Type{
					Oid:   types.T(pkCol.Typ.Id),
					Width: pkCol.Typ.Width,
					Scale: pkCol.Typ.Scale,
				}
				deleteBuff, err = convertColIntoSql(ctx, pkEle, &ttype, deleteBuff)
			}
			deleteBuff = appendByte(deleteBuff, ')')
		} else {
			//to insert
			//just fetch all columns.
			//do not distinguish primary keys first.
			//end delete sql first
			if len(deleteBuff) != 0 {
				deleteBuff = appendString(deleteBuff, ")")
				res = append(res, copyBytes(deleteBuff))
				firstDeleteRow = true
				deleteBuff = deleteBuff[:0]
			}
			if len(insertBuff) == 0 {
				//fill insert prefix
				insertBuff = appendString(insertBuff, timePrefix)
				insertBuff = appendString(insertBuff, insertPrefix)
			}

			if !firstInsertRow {
				insertBuff = appendString(insertBuff, ",")
			} else {
				firstInsertRow = false
			}
			insertBuff = appendString(insertBuff, "(")
			for colIdx, col := range row {
				if colIdx < ROWS_REAL_DATA_OFFSET {
					continue
				}
				if colIdx > ROWS_REAL_DATA_OFFSET {
					insertBuff = appendString(insertBuff, ",")
				}
				//transform column into text values
				insertBuff, err = convertColIntoSql(ctx, col, ent.Batch.Vecs[colIdx].GetType(), insertBuff)
				if err != nil {
					return nil, err
				}
			}
			insertBuff = appendString(insertBuff, ") ")
		}
	}
	if len(insertBuff) != 0 {
		res = append(res, copyBytes(insertBuff))
	}

	if len(deleteBuff) != 0 {
		deleteBuff = appendString(deleteBuff, ")")
		res = append(res, copyBytes(deleteBuff))
	}
	return res, nil
}

func decodeObjects(
	ts timestamp.Timestamp,
	objects *btree.BTreeG[logtailreplay.ObjectEntry],
) error {
	return nil
}

func decodeDeltas(
	ts timestamp.Timestamp,
	deltas *btree.BTreeG[logtailreplay.BlockDeltaEntry],
) error {
	return nil
}
