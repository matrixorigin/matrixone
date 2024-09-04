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

const (
	RowsRealDataOffset    int = 2
	ObjectsRealDataOffset int = 0
	CnDeltaRealDataOffset int = 1
	DnDeltaRealDataOffset int = 2

	MaxSqlSize int = 32 * 1024 * 1024
)

//
//func decodeRows(
//	ctx context.Context,
//	cdcCtx *disttae.TableCtx,
//	ts timestamp.Timestamp,
//	rowsIter logtailreplay.RowsIter,
//	wmarkPair *WatermarkPair,
//) (res [][]byte, err error) {
//	//TODO: schema info
//	var row []any
//	var typs []types.Type
//	//TODO:refine && limit sql size
//	timePrefix := fmt.Sprintf("/* decodeRows: %v, %v */ ", ts.String(), time.Now().Format(time.RFC3339Nano))
//	//---------------------------------------------------
//	insertPrefix := timePrefix + fmt.Sprintf("REPLACE INTO `%s`.`%s` VALUES ", cdcCtx.Db(), cdcCtx.Table())
//	/*
//		FORMAT:
//		insert into db.t values
//		(...),
//		...
//		(...)
//	*/
//
//	//---------------------------------------------------
//	/*
//		DELETE FORMAT:
//			mysql:
//				delete from db.t where
//				(pk1,..,pkn) in
//				(
//					(col1,..,coln),
//					...
//					(col1,...,coln)
//				)
//			matrixone:
//				TODO:
//	*/
//	//FIXME: assume the sink is mysql
//	tableDef := cdcCtx.TableDef()
//	colName2Index := make(map[string]int)
//	for i, col := range tableDef.Cols {
//		colName2Index[col.Name] = i
//	}
//
//	userDefinedColCnt := 0
//	for _, col := range tableDef.Cols {
//		// skip internal columns
//		if _, ok := catalog.InternalColumns[col.Name]; !ok {
//			userDefinedColCnt++
//		}
//	}
//
//	primaryKeyStr, err := getPrimaryKeyStr(ctx, tableDef)
//	if err != nil {
//		return nil, err
//	}
//	deletePrefix := timePrefix + fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s IN (", cdcCtx.Db(), cdcCtx.Table(), primaryKeyStr)
//
//	// init sql buffer
//	insertBuff := make([]byte, 0, MaxSqlSize)
//	insertBuff = append(insertBuff, []byte(insertPrefix)...)
//	deleteBuff := make([]byte, 0, MaxSqlSize)
//	deleteBuff = append(deleteBuff, []byte(deletePrefix)...)
//
//	valuesBuff := make([]byte, 0, 1024)
//	deleteInBuff := make([]byte, 0, 1024)
//	tCount := 0
//	skippedCount := 0
//	for rowsIter.Next() {
//		tCount++
//		ent := rowsIter.Entry()
//
//		toTs := ent.Time.ToTimestamp()
//		if wmarkPair.NeedSkip(toTs) {
//			skippedCount++
//			continue
//		}
//		wmarkPair.Update(toTs)
//
//		if row == nil {
//			colCnt := len(ent.Batch.Vecs) - RowsRealDataOffset
//			if colCnt <= 0 {
//				return nil, moerr.NewInternalError(ctx, "invalid row entry")
//			}
//			row = make([]any, len(ent.Batch.Vecs))
//		}
//		if typs == nil {
//			for _, vec := range ent.Batch.Vecs {
//				typs = append(typs, *vec.GetType())
//			}
//		}
//
//		//step1 : get row from the batch
//		if err = extractRowFromEveryVector(ctx, ent.Batch, RowsRealDataOffset, int(ent.Offset), row); err != nil {
//			return nil, err
//		}
//		//step2 : transform rows into sql parts
//		if ent.Deleted {
//			if deleteInBuff, err = getDeleteInBuff(ctx, tableDef, colName2Index, row, RowsRealDataOffset, deleteInBuff); err != nil {
//				return
//			}
//			deleteBuff = appendDeleteBuff(deleteBuff, []byte(deletePrefix), deleteInBuff, &res)
//		} else {
//			if valuesBuff, err = getValuesBuff(ctx, typs, row, RowsRealDataOffset, userDefinedColCnt, valuesBuff); err != nil {
//				return
//			}
//			insertBuff = appendInsertBuff(insertBuff, []byte(insertPrefix), valuesBuff, &res)
//		}
//	}
//
//	if len(insertBuff) != len(insertPrefix) {
//		res = append(res, copyBytes(insertBuff))
//	}
//	if len(deleteBuff) != len(deletePrefix) {
//		deleteBuff = appendString(deleteBuff, ")")
//		res = append(res, copyBytes(deleteBuff))
//	}
//	fmt.Fprintln(os.Stderr, "-----decodeRows-----", "total rows Count", tCount, "skipped rows count", skippedCount, wmarkPair.String())
//	return res, nil
//}
//
//func decodeObjects(
//	ctx context.Context,
//	cdcCtx *disttae.TableCtx,
//	ts timestamp.Timestamp,
//	objIter logtailreplay.ObjectsIter,
//	fs fileservice.FileService,
//	mp *mpool.MPool,
//	wmarkPair *WatermarkPair,
//) (res [][]byte, err error) {
//	var objMeta objectio.ObjectMeta
//	var bat *batch.Batch
//	var release func()
//	var row []any
//	timePrefix := fmt.Sprintf("/* decodeObjects: %v, %v */ ", ts.String(), time.Now().Format(time.RFC3339Nano))
//	//---------------------------------------------------
//	insertPrefix := timePrefix + fmt.Sprintf("REPLACE INTO `%s`.`%s` VALUES ", cdcCtx.Db(), cdcCtx.Table())
//	/*
//		FORMAT:
//		insert into db.t values
//		(...),
//		...
//		(...)
//	*/
//
//	tableDef := cdcCtx.TableDef()
//	if len(tableDef.Pkey.Names) == 0 {
//		return nil, moerr.NewInternalError(ctx, "cdc table need primary key")
//	}
//
//	userDefinedColCnt := 0
//	for _, col := range tableDef.Cols {
//		// skip internal columns
//		if _, ok := catalog.InternalColumns[col.Name]; !ok {
//			userDefinedColCnt++
//		}
//	}
//
//	cols := make([]uint16, 0, len(tableDef.Cols))
//	typs := make([]types.Type, 0, len(tableDef.Cols))
//	for i, col := range tableDef.Cols {
//		cols = append(cols, uint16(i))
//		typs = append(typs, types.Type{
//			Oid:   types.T(col.Typ.Id),
//			Width: col.Typ.Width,
//			Scale: col.Typ.Scale,
//		})
//	}
//
//	// init sql buffer
//	insertBuff := make([]byte, 0, MaxSqlSize)
//	insertBuff = append(insertBuff, []byte(insertPrefix)...)
//
//	valuesBuff := make([]byte, 0, 1024)
//	rowCnt := uint64(0)
//	tCount := 0
//	skippedCount := 0
//	for objIter.Next() {
//		ent := objIter.Entry()
//		loc := ent.ObjectLocation()
//		if loc.IsEmpty() {
//			continue
//		}
//		tCount++
//		toTs := ent.CommitTS.ToTimestamp()
//		if wmarkPair.NeedSkip(toTs) {
//			skippedCount++
//			continue
//		}
//		wmarkPair.Update(toTs)
//
//		objMeta, err = objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
//		if err != nil {
//			return nil, err
//		}
//		disttae.ForeachBlkInObjStatsList(
//			true,
//			objMeta.MustDataMeta(),
//			func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
//				bat, release, err = blockio.LoadColumns(
//					ctx,
//					cols,
//					typs,
//					fs,
//					blk.MetaLocation(),
//					mp,
//					fileservice.Policy(0))
//				if err != nil {
//					return false
//				}
//				defer release()
//
//				if row == nil {
//					colCnt := len(bat.Vecs)
//					if colCnt <= 0 {
//						return false
//					}
//					row = make([]any, len(bat.Vecs))
//				}
//				rowCnt += uint64(bat.Vecs[0].Length())
//				for i := 0; i < bat.Vecs[0].Length(); i++ {
//					if err = extractRowFromEveryVector(ctx, bat, ObjectsRealDataOffset, i, row); err != nil {
//						return false
//					}
//
//					if valuesBuff, err = getValuesBuff(ctx, typs, row, ObjectsRealDataOffset, userDefinedColCnt, valuesBuff); err != nil {
//						return false
//					}
//
//					insertBuff = appendInsertBuff(insertBuff, []byte(insertPrefix), valuesBuff, &res)
//
//				}
//				return true
//			},
//			ent.ObjectStats,
//		)
//	}
//
//	if len(insertBuff) != len(insertPrefix) {
//		res = append(res, copyBytes(insertBuff))
//	}
//
//	fmt.Fprintln(os.Stderr, "-----objects row count----", "total objects count", tCount, "skipped objects count", skippedCount, "rows count", rowCnt)
//	return
//}
//
//func decodeDeltas(
//	ctx context.Context,
//	cdcCtx *disttae.TableCtx,
//	ts timestamp.Timestamp,
//	deltaIter logtailreplay.BlockDeltaIter,
//	fs fileservice.FileService,
//	wmarkPair *WatermarkPair,
//	fromSubResp bool,
//) (res [][]byte, err error) {
//	tableDef := cdcCtx.TableDef()
//	colName2Index := make(map[string]int)
//	for i, col := range tableDef.Cols {
//		colName2Index[col.Name] = i
//	}
//
//	primaryKeyStr, err := getPrimaryKeyStr(ctx, tableDef)
//	if err != nil {
//		return nil, err
//	}
//
//	timePrefix := fmt.Sprintf("/* decodeDeltas: %v, %v */ ", ts.String(), time.Now().Format(time.RFC3339Nano))
//	deletePrefix := timePrefix + fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s IN (", cdcCtx.Db(), cdcCtx.Table(), primaryKeyStr)
//
//	deleteBuff := make([]byte, 0, MaxSqlSize)
//	deleteBuff = append(deleteBuff, []byte(deletePrefix)...)
//	deleteInBuff := make([]byte, 0, 1024)
//
//	//obj location -> obj commit ts
//	dedup := make(map[[objectio.LocationLen]byte]timestamp.Timestamp)
//	for deltaIter.Next() {
//		ent := deltaIter.Entry()
//		if ent.DeltaLocation().IsEmpty() {
//			continue
//		}
//		if _, ok := dedup[ent.DeltaLoc]; !ok {
//			dedup[ent.DeltaLoc] = ent.CommitTs.ToTimestamp()
//		}
//	}
//	fmt.Fprintln(os.Stderr, "-----delta count----", len(dedup))
//	for loc, commitTs := range dedup {
//		if err = decodeDeltaEntry(
//			ctx,
//			loc[:],
//			fs,
//			tableDef,
//			colName2Index,
//			deletePrefix,
//			deleteInBuff,
//			deleteBuff,
//			&res,
//			ts,
//			commitTs,
//			wmarkPair,
//			fromSubResp,
//		); err != nil {
//			return nil, err
//		}
//	}
//	return
//}
//
//func decodeDeltaEntry(
//	ctx context.Context,
//	loc []byte,
//	fs fileservice.FileService,
//	tableDef *plan.TableDef,
//	colName2Index map[string]int,
//	deletePrefix string,
//	deleteInBuff []byte,
//	deleteBuff []byte,
//	res *[][]byte,
//	ts timestamp.Timestamp,
//	cnObjCommitTs timestamp.Timestamp,
//	wmarkPair *WatermarkPair,
//	fromSubResp bool,
//) (err error) {
//	bat, byCn, release, err := blockio.ReadBlockDelete(ctx, loc, fs)
//	if err != nil {
//		return err
//	}
//	defer release()
//	rowCnt := bat.Vecs[0].Length()
//
//	colCnt := len(bat.Vecs)
//	if colCnt <= 0 {
//		return moerr.NewInternalError(ctx, "invalid row entry")
//	}
//	skippedCount := 0
//	if byCn {
//		if !wmarkPair.NeedSkip(cnObjCommitTs) {
//			wmarkPair.Update(cnObjCommitTs)
//			//Two columns : rowid, pk col
//			row := make([]any, colCnt)
//
//			for rowIdx := 0; rowIdx < bat.Vecs[0].Length(); rowIdx++ {
//				//skip rowid
//				if err = extractRowFromEveryVector(ctx, bat, CnDeltaRealDataOffset, rowIdx, row); err != nil {
//					return err
//				}
//
//				if deleteInBuff, err = getDeleteInBuff(ctx, tableDef, colName2Index, row, CnDeltaRealDataOffset, deleteInBuff); err != nil {
//					return
//				}
//
//				deleteBuff = appendDeleteBuff(deleteBuff, []byte(deletePrefix), deleteInBuff, res)
//			}
//
//			if len(deleteBuff) != len(deletePrefix) {
//				deleteBuff = appendString(deleteBuff, ")")
//				*res = append(*res, copyBytes(deleteBuff))
//			}
//		} else {
//			skippedCount += rowCnt
//		}
//	} else if fromSubResp { //only process the deltas from subscribe response
//		//Four columns : rowid, ts, pk col, abort
//		row := make([]any, colCnt)
//		for rowIdx := 0; rowIdx < bat.Vecs[0].Length(); rowIdx++ {
//			//skip rowid
//			if err = extractRowFromEveryVector(ctx, bat, DnDeltaRealDataOffset-1, rowIdx, row); err != nil {
//				return err
//			}
//
//			// filter by ts
//			rowTs := row[1].(types.TS)
//			abort := row[3].(bool)
//			toTs := rowTs.ToTimestamp()
//			if abort || wmarkPair.NeedSkip(toTs) {
//				wmarkPair.Update(toTs)
//				if deleteInBuff, err = getDeleteInBuff(ctx, tableDef, colName2Index, row, DnDeltaRealDataOffset, deleteInBuff); err != nil {
//					return
//				}
//
//				deleteBuff = appendDeleteBuff(deleteBuff, []byte(deletePrefix), deleteInBuff, res)
//			} else {
//				skippedCount++
//			}
//		}
//
//		if len(deleteBuff) != len(deletePrefix) {
//			deleteBuff = appendString(deleteBuff, ")")
//			*res = append(*res, copyBytes(deleteBuff))
//		}
//	}
//	fmt.Fprintln(os.Stderr, "-----delta batch----",
//		"byCn", byCn,
//		"column cnt", len(bat.Vecs),
//		"commitTs", TimestampToStr(cnObjCommitTs),
//		"needSkip Cn Object", wmarkPair.NeedSkip(cnObjCommitTs),
//		"row count", rowCnt,
//		"fromSubResp", fromSubResp,
//		"skippedCount", skippedCount,
//		"logtail ts", ts.String(),
//		wmarkPair,
//	)
//	return
//}

//// appendInsertBuff appends bytes to insertBuff if not exceed its cap
//// otherwise, save insertBuff to res and reset insertBuff
//func appendInsertBuff(insertBuff, insertPrefix, bytes []byte, res *[][]byte) []byte {
//	// insert comma if not the first item
//	commaLen := 0
//	if len(insertBuff) != len(insertPrefix) {
//		commaLen = 1
//	}
//
//	if len(insertBuff)+commaLen+len(bytes) > cap(insertBuff) {
//		*res = append(*res, copyBytes(insertBuff))
//		insertBuff = insertBuff[:0]
//		// TODO need to update timePrefix?
//		insertBuff = append(insertBuff, insertPrefix...)
//	}
//
//	if len(insertBuff) != len(insertPrefix) {
//		insertBuff = appendByte(insertBuff, ',')
//	}
//	return append(insertBuff, bytes...)
//}

//func appendDeleteBuff(deleteBuff, deletePrefix, bytes []byte, res *[][]byte) []byte {
//	// insert comma if not the first item
//	commaLen := 0
//	if len(deleteBuff) != len(deletePrefix) {
//		commaLen = 1
//	}
//
//	// +1 is for the right parenthesis
//	if len(deleteBuff)+commaLen+len(bytes)+1 > cap(deleteBuff) {
//		deleteBuff = appendByte(deleteBuff, ')')
//		*res = append(*res, copyBytes(deleteBuff))
//		deleteBuff = deleteBuff[:0]
//		deleteBuff = append(deleteBuff, deletePrefix...)
//	}
//
//	if len(deleteBuff) != len(deletePrefix) {
//		deleteBuff = appendByte(deleteBuff, ',')
//	}
//	return append(deleteBuff, bytes...)
//}

//func getPrimaryKeyStr(ctx context.Context, tableDef *plan.TableDef) (string, error) {
//	if len(tableDef.Pkey.Names) == 0 {
//		return "", moerr.NewInternalError(ctx, "cdc table need primary key")
//	}
//
//	buf := strings.Builder{}
//	buf.WriteByte('(')
//	for i, pkName := range tableDef.Pkey.Names {
//		if i > 0 {
//			buf.WriteByte(',')
//		}
//		buf.WriteString(pkName)
//	}
//	buf.WriteByte(')')
//	return buf.String(), nil
//}

//func getValuesBuff(
//	ctx context.Context,
//	typs []types.Type,
//	row []any,
//	startColIdx int,
//	colsCnt int,
//	valuesBuff []byte,
//) ([]byte, error) {
//	var err error
//	valuesBuff = valuesBuff[:0]
//
//	valuesBuff = appendByte(valuesBuff, '(')
//	for i := startColIdx; i < startColIdx+colsCnt; i++ {
//		if i > startColIdx {
//			valuesBuff = appendByte(valuesBuff, ',')
//		}
//
//		//transform column into text values
//		if valuesBuff, err = convertColIntoSql(ctx, row[i], &typs[i], valuesBuff); err != nil {
//			return valuesBuff, err
//		}
//	}
//	valuesBuff = appendByte(valuesBuff, ')')
//
//	return valuesBuff, nil
//}

//func getDeleteInBuff(
//	ctx context.Context,
//	tableDef *plan.TableDef,
//	colName2Index map[string]int,
//	row []any,
//	pkIdxInRow int,
//	deleteInBuff []byte,
//) ([]byte, error) {
//	var err error
//	deleteInBuff = deleteInBuff[:0]
//
//	//decode primary key col from pk col data
//	if len(tableDef.Pkey.Names) != 1 {
//		//case 1: composed pk col
//		comPkCol := row[pkIdxInRow]
//		pkTuple, pkTypes, err := types.UnpackWithSchema(comPkCol.([]byte))
//		if err != nil {
//			return deleteInBuff, err
//		}
//
//		deleteInBuff = appendByte(deleteInBuff, '(')
//		for pkIdx, pkEle := range pkTuple {
//			if pkIdx > 0 {
//				deleteInBuff = appendByte(deleteInBuff, ',')
//			}
//			pkName := tableDef.Pkey.Names[pkIdx]
//			pkColIdx := colName2Index[pkName]
//			pkCol := tableDef.Cols[pkColIdx]
//			if pkTypes[pkIdx] != types.T(pkCol.Typ.Id) {
//				return deleteInBuff, moerr.NewInternalErrorf(ctx, "different pk col Type %v %v", pkTypes[pkIdx], pkCol.Typ.Id)
//			}
//			ttype := types.Type{
//				Oid:   types.T(pkCol.Typ.Id),
//				Width: pkCol.Typ.Width,
//				Scale: pkCol.Typ.Scale,
//			}
//			if deleteInBuff, err = convertColIntoSql(ctx, pkEle, &ttype, deleteInBuff); err != nil {
//				return deleteInBuff, err
//			}
//		}
//		deleteInBuff = appendByte(deleteInBuff, ')')
//	} else {
//		//case 2: single pk col
//		pkColData := row[pkIdxInRow]
//		pkName := tableDef.Pkey.Names[0]
//		pkColIdx := colName2Index[pkName]
//		pkCol := tableDef.Cols[pkColIdx]
//		ttype := types.Type{
//			Oid:   types.T(pkCol.Typ.Id),
//			Width: pkCol.Typ.Width,
//			Scale: pkCol.Typ.Scale,
//		}
//		if deleteInBuff, err = convertColIntoSql(ctx, pkColData, &ttype, deleteInBuff); err != nil {
//			return deleteInBuff, err
//		}
//	}
//
//	return deleteInBuff, nil
//}
