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

package disttae

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (txn *Transaction) getTableList(ctx context.Context, databaseId uint64) ([]string, error) {
	columns := []string{catalog.MoTablesSchema[catalog.MO_TABLES_REL_NAME_IDX]}
	expr := genTableListExpr(getAccountId(ctx), databaseId)
	rows, err := txn.getRows(ctx, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID, txn.dnStores[:1],
		columns, expr, getMoTableTableDef(columns))
	if err != nil {
		return nil, err
	}
	tableList := make([]string, len(rows))
	for i := range rows {
		tableList[i] = rows[i][0].(string)
	}
	return tableList, nil
}

func (txn *Transaction) getTableInfo(ctx context.Context, databaseId uint64,
	name string) (uint64, []engine.TableDef, error) {
	accountId := getAccountId(ctx)
	columns := catalog.MoTablesSchema
	row, err := txn.getRow(ctx, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		txn.dnStores[:1], columns, genTableIdExpr(accountId, databaseId, name), getMoTableTableDef(columns))
	if err != nil {
		return 0, nil, err
	}
	id := row[0].(uint64)
	rows, err := txn.getRows(ctx, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID,
		txn.dnStores[:1], catalog.MoColumnsSchema,
		genColumnInfoExpr(accountId, databaseId, id), getMoColumnTableDef(catalog.MoColumnsSchema))
	if err != nil {
		return 0, nil, err
	}
	cols := getColumnsFromRows(rows)
	defs := make([]engine.TableDef, 0, len(cols))
	defs = append(defs, genTableDefOfComment(string(row[6].([]byte))))
	for _, col := range cols {
		defs = append(defs, genTableDefOfColumn(col))
	}
	return id, defs, nil
}

func (txn *Transaction) getTableId(ctx context.Context, databaseId uint64,
	name string) (uint64, error) {
	accountId := getAccountId(ctx)
	columns := []string{catalog.MoTablesSchema[catalog.MO_TABLES_REL_ID_IDX]}

	row, err := txn.getRow(ctx, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		txn.dnStores[:1], columns,
		genTableIdExpr(accountId, databaseId, name), getMoTableTableDef(columns))
	if err != nil {
		return 0, err
	}
	return row[0].(uint64), nil
}

func (txn *Transaction) getDatabaseList(ctx context.Context) ([]string, error) {
	columns := []string{catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_NAME_IDX]}

	rows, err := txn.getRows(ctx, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		txn.dnStores[:1], columns, genDatabaseListExpr(getAccountId(ctx)), getMoDatabaseTableDef(columns))
	if err != nil {
		return nil, err
	}
	databaseList := make([]string, len(rows))
	for i := range rows {
		databaseList[i] = rows[i][0].(string)
	}
	return databaseList, nil
}

func (txn *Transaction) getDatabaseId(ctx context.Context, name string) (uint64, error) {
	accountId := getAccountId(ctx)
	columns := []string{catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_ID_IDX]}
	row, err := txn.getRow(ctx, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID, txn.dnStores[:1],
		columns, genDatabaseIdExpr(accountId, name), getMoDatabaseTableDef(columns))
	if err != nil {
		return 0, err
	}
	return row[0].(uint64), nil
}

// detecting whether a transaction is a read-only transaction
func (txn *Transaction) ReadOnly() bool {
	return txn.readOnly
}

// use for solving halloween problem
func (txn *Transaction) IncStatementId() {
	txn.statementId++
	txn.writes = append(txn.writes, make([]Entry, 0, 1))
}

// Write used to write data to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteBatch(typ int, databaseId, tableId uint64,
	databaseName, tableName string, bat *batch.Batch) error {
	txn.readOnly = true
	txn.writes[txn.statementId] = append(txn.writes[txn.statementId], Entry{
		typ:          typ,
		bat:          bat,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
	})
	return nil
}

func (txn *Transaction) RegisterFile(fileName string) {
	txn.fileMap[fileName] = txn.blockId
	txn.blockId++
}

// WriteFile used to add a s3 file information to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteFile(typ int, databaseId, tableId uint64,
	databaseName, tableName string, fileName string) error {
	txn.readOnly = true
	txn.writes[txn.statementId] = append(txn.writes[txn.statementId], Entry{
		typ:          typ,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		fileName:     fileName,
		blockId:      txn.fileMap[fileName],
	})
	return nil
}

// getRow used to get a row of table based on a condition
func (txn *Transaction) getRow(ctx context.Context, databaseId uint64, tableId uint64,
	dnList []DNStore, columns []string, expr *plan.Expr, tableDef *plan.TableDef) ([]any, error) {
	bats, err := txn.readTable(ctx, databaseId, tableId, dnList, columns, expr, tableDef)
	if err != nil {
		return nil, err
	}
	if len(bats) == 0 {
		return nil, moerr.NewInvalidInput("empty table: %v.%v", databaseId, tableId)
	}
	if len(bats) != 1 {
		return nil, moerr.NewInvalidInput("table is not unique")
	}
	rows := genRows(bats[0])
	if len(rows) != 1 {
		return nil, moerr.NewInvalidInput("table is not unique")
	}
	return rows[0], nil
}

// getRows used to get rows of table
func (txn *Transaction) getRows(ctx context.Context, databaseId uint64, tableId uint64,
	dnList []DNStore, columns []string, expr *plan.Expr, tableDef *plan.TableDef) ([][]any, error) {
	bats, err := txn.readTable(ctx, databaseId, tableId, dnList, columns, expr, tableDef)
	if err != nil {
		return nil, err
	}
	if len(bats) == 0 {
		return nil, moerr.NewInternalError("empty table: %v.%v", databaseId, tableId)
	}
	rows := make([][]any, 0, len(bats))
	for _, bat := range bats {
		rows = append(rows, genRows(bat)...)
	}
	return rows, nil
}

// readTable used to get tuples of table based on a condition
// only used to read data from catalog, for which the execution is currently single-core
func (txn *Transaction) readTable(ctx context.Context, databaseId uint64, tableId uint64,
	dnList []DNStore, columns []string, expr *plan.Expr, tableDef *plan.TableDef) ([]*batch.Batch, error) {
	var writes [][]Entry

	// consider halloween problem
	if int64(txn.statementId)-1 > 0 {
		writes = txn.writes[:txn.statementId-1]
	}
	blkInfos := txn.db.BlockList(ctx, dnList, databaseId, tableId, txn.meta.SnapshotTS, writes)
	bats := make([]*batch.Batch, 0, len(blkInfos))

	isMonotonically := checkExprIsMonotonical(expr)
	if isMonotonically {
		for _, blkInfo := range blkInfos {
			if !needRead(expr, blkInfo, tableDef, txn.proc) {
				continue
			}
			bat, err := blockRead(ctx, columns, blkInfo, txn.fs, tableDef)
			if err != nil {
				return nil, err
			}
			bats = append(bats, bat)
		}
	} else {
		for _, blkInfo := range blkInfos {
			bat, err := blockRead(ctx, columns, blkInfo, txn.fs, tableDef)
			if err != nil {
				return nil, err
			}
			bats = append(bats, bat)
		}
	}

	rds, err := txn.db.NewReader(ctx, 1, expr, dnList, databaseId,
		tableId, txn.meta.SnapshotTS, writes)
	if err != nil {
		return nil, err
	}
	for _, rd := range rds {
		bat, err := rd.Read(columns, expr, nil)
		if err != nil {
			return nil, err
		}
		bats = append(bats, bat)
	}
	return bats, nil
}

// needRead determine if a block needs to be read
func needRead(expr *plan.Expr, blkInfo BlockMeta, tableDef *plan.TableDef, proc *process.Process) bool {
	var err error
	columns := getColumnsByExpr(expr)

	// if expr match no columns, just eval expr
	if len(columns) == 0 {
		bat := batch.NewWithSize(0)
		ifNeed, err := evalFilterExpr(expr, bat, proc)
		if err != nil {
			return true
		}
		return ifNeed
	}

	// get min max data from Meta
	datas, dataTypes, err := getZonemapDataFromMeta(columns, blkInfo, tableDef)
	if err != nil {
		return true
	}

	// use all min/max data to build []vectors.
	buildVectors := buildVectorsByData(datas, dataTypes, proc.GetMheap())
	bat := batch.NewWithSize(len(columns))
	bat.Zs = make([]int64, buildVectors[0].Length())
	bat.Vecs = buildVectors

	ifNeed, err := evalFilterExpr(expr, bat, proc)
	if err != nil {
		return true
	}

	return ifNeed

}

// write a block to s3
func blockWrite(ctx context.Context, blkInfo BlockMeta, bat *batch.Batch, fs fileservice.FileService) ([]objectio.BlockObject, error) {
	// 1. check columns length, check types
	if len(blkInfo.columns) != len(bat.Vecs) {
		return nil, moerr.NewInternalError(fmt.Sprintf("write block error: need %v columns, get %v columns", len(blkInfo.columns), len(bat.Vecs)))
	}
	for i, vec := range bat.Vecs {
		if blkInfo.columns[i].typ != uint8(vec.Typ.Oid) {
			return nil, moerr.NewInternalError(fmt.Sprintf("write block error: column[%v]'s type is not match", i))
		}
	}

	// 2. write bat
	s3FileName := getNameFromMeta(blkInfo)
	writer, err := objectio.NewObjectWriter(s3FileName, fs)
	if err != nil {
		return nil, err
	}
	fd, err := writer.Write(bat)
	if err != nil {
		return nil, err
	}

	// 3. write index (index and zonemap)
	for i, vec := range bat.Vecs {
		bloomFilter, zoneMap, err := getIndexDataFromVec(uint16(i), vec)
		if err != nil {
			return nil, err
		}
		if bloomFilter != nil {
			err = writer.WriteIndex(fd, bloomFilter)
			if err != nil {
				return nil, err
			}
		}
		if zoneMap != nil {
			err = writer.WriteIndex(fd, zoneMap)
			if err != nil {
				return nil, err
			}
		}
	}

	// 4. get return
	return writer.WriteEnd()
}

// read a block from s3
func blockRead(ctx context.Context, columns []string, blkInfo BlockMeta, fs fileservice.FileService, tableDef *plan.TableDef) (*batch.Batch, error) {
	// 1. get extent from meta
	extent := getExtentFromMeta(blkInfo)

	// 2. get idxs
	columnLength := len(columns)
	idxs := make([]uint16, columnLength)
	columnTypes := make([]types.Type, columnLength)
	for i, column := range columns {
		idxs[i] = uint16(tableDef.Name2ColIndex[column])
		columnTypes[i] = types.T(blkInfo.columns[idxs[i]].typ).ToType()
	}

	// 2. read data
	s3FileName := getNameFromMeta(blkInfo)
	reader, err := objectio.NewObjectReader(s3FileName, fs)
	if err != nil {
		return nil, err
	}
	ioVec, err := reader.Read(extent, idxs)
	if err != nil {
		return nil, err
	}

	// 3. fill Batch
	bat := batch.NewWithSize(columnLength)
	bat.Attrs = columns
	for i, entry := range ioVec.Entries {
		vec := vector.New(columnTypes[i])
		err := vec.Read(entry.Data)
		if err != nil {
			return nil, err
		}
		bat.Vecs[i] = vec
	}
	bat.Zs = make([]int64, int64(bat.Vecs[0].Length()))

	return bat, nil
}
