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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

const (
	ROWS_REAL_DATA_OFFSET int = 2
)

var _ Decoder = new(decoder)

type decoder struct {
	mp *mpool.MPool
	fs fileservice.FileService
}

func NewDecoder(mp *mpool.MPool, fs fileservice.FileService) Decoder {
	return &decoder{
		mp: mp,
		fs: fs,
	}
}

func (dec *decoder) Decode(ctx context.Context, cdcCtx *disttae.TableCtx, input *disttae.DecoderInput) (out *DecoderOutput) {
	//parallel step1:decode rows
	out = &DecoderOutput{
		ts: input.TS(),
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	err := ants.Submit(func() {
		defer wg.Done()
		it := input.State().NewRowsIterInCdc()
		defer it.Close()
		rows, err2 := decodeRows(ctx, cdcCtx, input.TS(), it)
		if err2 != nil {
			return
		}
		out.sqlOfRows.Store(rows)

	})
	if err != nil {
		panic(err)
	}
	//parallel step2:decode objects
	wg.Add(1)
	err = ants.Submit(func() {
		defer wg.Done()
		it := input.State().NewObjectsIterInCdc()
		defer it.Close()
		rows, err2 := decodeObjects(
			ctx,
			cdcCtx,
			input.TS(),
			it,
			dec.fs,
			dec.mp,
		)
		if err2 != nil {
			return
		}
		out.sqlOfObjects.Store(rows)
	})
	if err != nil {
		panic(err)
	}
	//parallel step3:decode deltas
	wg.Add(1)
	err = ants.Submit(func() {
		defer wg.Done()
		it := input.State().NewBlockDeltaIter()
		defer it.Close()
		rows, err2 := decodeDeltas(
			ctx,
			input.TS(),
			it.Entry(),
			dec.fs,
		)
		if err2 != nil {
			return
		}
		out.sqlOfDeletes.Store(rows)
	})
	if err != nil {
		panic(err)
	}
	wg.Wait()
	return
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
	if len(tableDef.Pkey.Names) == 0 {
		return nil, moerr.NewInternalError(ctx, "cdc table need primary key")
	}
	singlePkCol := false
	if len(tableDef.Pkey.Names) == 1 {
		singlePkCol = true
	}
	colName2Index := make(map[string]int)
	for i, col := range tableDef.Cols {
		colName2Index[col.Name] = i
	}
	for i, pkName := range tableDef.Pkey.Names {
		if i > 0 {
			sbuf.WriteByte(',')
		}
		sbuf.WriteString(pkName)
	}
	sbuf.WriteByte(')')
	deletePrefix := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s IN ( ", cdcCtx.Db(), cdcCtx.Table(), sbuf.String())

	//TODO: complement the
	firstInsertRow, firstDeleteRow := true, true
	insertBuff := make([]byte, 0, 1024)
	deleteBuff := make([]byte, 0, 1024)
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

			//decode primary key col from pk col data
			if !singlePkCol {
				//case 1: composed pk col
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
					pkName := tableDef.Pkey.Names[pkIdx]
					pkColIdx := colName2Index[pkName]
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
				//case 2: sinle pk col
				pkColData := row[2]
				pkName := tableDef.Pkey.Names[0]
				pkColIdx := colName2Index[pkName]
				pkCol := tableDef.Cols[pkColIdx]
				ttype := types.Type{
					Oid:   types.T(pkCol.Typ.Id),
					Width: pkCol.Typ.Width,
					Scale: pkCol.Typ.Scale,
				}
				deleteBuff, err = convertColIntoSql(ctx, pkColData, &ttype, deleteBuff)
			}

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
	ctx context.Context,
	cdcCtx *disttae.TableCtx,
	ts timestamp.Timestamp,
	objIter logtailreplay.ObjectsIter,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (res [][]byte, err error) {
	var objMeta objectio.ObjectMeta
	var bat *batch.Batch
	var release func()
	var row []any
	tableDef := cdcCtx.TableDef()
	cols := make([]uint16, 0)
	typs := make([]types.Type, 0)
	for i, colDef := range tableDef.Cols {
		if colDef.Name == catalog.Row_ID {
			continue
		}
		cols = append(cols, uint16(i))
		typs = append(typs, types.Type{
			Oid:   types.T(colDef.Typ.Id),
			Width: colDef.Typ.Width,
			Scale: colDef.Typ.Scale,
		})
	}
	for objIter.Next() {
		ent := objIter.Entry()
		loc := ent.ObjectLocation()
		objMeta, err = objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
		if err != nil {
			return nil, err
		}
		disttae.ForeachBlkInObjStatsList(
			true,
			objMeta.MustDataMeta(),
			func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
				bat, release, err = blockio.LoadColumns(
					ctx,
					cols,
					typs,
					fs,
					blk.MetaLocation(),
					mp,
					fileservice.Policy(0))
				if err != nil {
					return false
				}
				defer release()

				if row == nil {
					colCnt := len(bat.Vecs)
					if colCnt <= 0 {
						return false
					}
					row = make([]any, len(bat.Vecs))
				}
				for i := 0; i < bat.Vecs[0].Length(); i++ {
					err = extractRowFromEveryVector(ctx, bat, 0, i, row)
					if err != nil {
						return false
					}
					fmt.Fprintln(os.Stderr, "-----objects row----", row)
				}
				//TODO:decode
				return true
			},
			ent.ObjectStats,
		)
	}
	return
}

func decodeDeltas(
	ctx context.Context,
	ts timestamp.Timestamp,
	delta logtailreplay.BlockDeltaEntry,
	fs fileservice.FileService,
) (res [][]byte, err error) {
	var dels *nulls.Nulls
	bat, byCn, release, err := blockio.ReadBlockDelete(ctx, delta.DeltaLocation(), fs)
	if err != nil {
		return nil, err
	}
	defer release()
	if byCn {
		dels = blockio.EvalDeleteRowsByTimestampForDeletesPersistedByCN(bat, types.MaxTs(), delta.CommitTs)
	}
	if dels == nil {
		dels = nulls.NewWithSize(128)
	}
	//TODO:how to process dels
	return
}
