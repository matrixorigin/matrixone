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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (txn *Transaction) getTableList(ctx context.Context, databaseId uint64) ([]string, error) {
	rows, err := txn.getRows(ctx, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID, txn.dnStores[:1],
		catalog.MoTablesTableDefs, []string{
			catalog.MoTablesSchema[catalog.MO_TABLES_REL_NAME_IDX],
			catalog.MoTablesSchema[catalog.MO_TABLES_RELDATABASE_ID_IDX],
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX],
		},
		genTableListExpr(getAccountId(ctx), databaseId))
	if err != nil {
		return nil, err
	}
	tableList := make([]string, len(rows))
	for i := range rows {
		tableList[i] = string(rows[i][0].([]byte))
	}
	return tableList, nil
}

func (txn *Transaction) getTableInfo(ctx context.Context, databaseId uint64,
	name string) (uint64, []engine.TableDef, error) {
	accountId := getAccountId(ctx)
	row, err := txn.getRow(ctx, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		txn.dnStores[:1], catalog.MoTablesTableDefs, catalog.MoTablesSchema,
		genTableInfoExpr(accountId, databaseId, name))
	if err != nil {
		return 0, nil, err
	}
	id := row[0].(uint64)
	rows, err := txn.getRows(ctx, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID,
		txn.dnStores[:1], catalog.MoColumnsTableDefs, catalog.MoColumnsSchema,
		genColumnInfoExpr(accountId, databaseId, id))
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
	row, err := txn.getRow(ctx, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		txn.dnStores[:1],
		catalog.MoDatabaseTableDefs, []string{
			catalog.MoTablesSchema[catalog.MO_TABLES_REL_ID_IDX],
			catalog.MoTablesSchema[catalog.MO_TABLES_REL_NAME_IDX],
			catalog.MoTablesSchema[catalog.MO_TABLES_RELDATABASE_ID_IDX],
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX],
		},
		genTableIdExpr(accountId, databaseId, name))
	if err != nil {
		return 0, err
	}
	return row[0].(uint64), nil
}

func (txn *Transaction) getDatabaseList(ctx context.Context) ([]string, error) {
	rows, err := txn.getRows(ctx, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		txn.dnStores[:1],
		catalog.MoDatabaseTableDefs, []string{
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_NAME_IDX],
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_ACCOUNT_ID_IDX],
		},
		genDatabaseListExpr(getAccountId(ctx)))
	if err != nil {
		return nil, err
	}
	databaseList := make([]string, len(rows))
	for i := range rows {
		databaseList[i] = string(rows[i][0].([]byte))
	}
	return databaseList, nil
}

func (txn *Transaction) getDatabaseId(ctx context.Context, name string) (uint64, error) {
	accountId := getAccountId(ctx)
	row, err := txn.getRow(ctx, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID, txn.dnStores[:1],
		catalog.MoDatabaseTableDefs, []string{
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_ID_IDX],
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_NAME_IDX],
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_ACCOUNT_ID_IDX],
		},
		genDatabaseIdExpr(accountId, name))
	if err != nil {
		return 0, err
	}
	return row[0].(uint64), nil
}

func (txn *Transaction) getTableMeta(ctx context.Context, databaseId uint64,
	name string) (*tableMeta, error) {
	id, defs, err := txn.getTableInfo(ctx, databaseId, name)
	if err.Error() == "info: empty table" {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := txn.db.Update(ctx, txn.dnStores, databaseId,
		id, txn.meta.SnapshotTS); err != nil {
		return nil, err
	}
	cols := make([]string, 0, len(defs))
	{
		for _, def := range defs {
			if attr, ok := def.(*engine.AttributeDef); ok {
				cols = append(cols, attr.Attr.Name)
			}
		}
	}
	blocks := make([][]BlockMeta, len(txn.dnStores))
	for i, dnStore := range txn.dnStores {
		rows, err := txn.getRows(ctx, databaseId, id,
			[]DNStore{dnStore}, defs, cols, nil)
		if err != nil {
			return nil, err
		}
		blocks[i] = genBlockMetas(rows)
	}
	return &tableMeta{
		tableId:   id,
		defs:      defs,
		tableName: name,
		blocks:    blocks,
	}, nil
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
	databaseName, tableName string, bat *batch.Batch, dnStore DNStore) error {
	txn.readOnly = false
	if typ == INSERT {
		len := bat.Length()
		vec := vector.New(types.New(types.T_Rowid, 0, 0, 0))
		for i := 0; i < len; i++ {
			if err := vec.Append(txn.genRowId(), false,
				txn.proc.Mp()); err != nil {
				return err
			}
		}
		bat.Vecs = append([]*vector.Vector{vec}, bat.Vecs...)
		bat.Attrs = append([]string{catalog.Row_ID}, bat.Attrs...)
	}
	txn.writes[txn.statementId] = append(txn.writes[txn.statementId], Entry{
		typ:          typ,
		bat:          bat,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		dnStore:      dnStore,
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
	txn.readOnly = false
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
	dnList []DNStore, defs []engine.TableDef, columns []string, expr *plan.Expr) ([]any, error) {
	bats, err := txn.readTable(ctx, databaseId, tableId, defs, dnList, columns, expr)
	if err != nil {
		return nil, err
	}
	if len(bats) == 0 {
		return nil, moerr.NewInfo("empty table")
	}
	rows := make([][]any, 0, len(bats))
	for _, bat := range bats {
		if bat.Length() > 0 {
			rows = append(rows, catalog.GenRows(bat)...)
		}
		bat.Clean(txn.proc.Mp())
	}
	if len(rows) == 0 {
		return nil, moerr.NewInfo("empty table")
	}
	if len(rows) != 1 {
		return nil, moerr.NewInvalidInput("table is not unique")
	}
	return rows[0], nil
}

// getRows used to get rows of table
func (txn *Transaction) getRows(ctx context.Context, databaseId uint64, tableId uint64,
	dnList []DNStore, defs []engine.TableDef, columns []string, expr *plan.Expr) ([][]any, error) {
	bats, err := txn.readTable(ctx, databaseId, tableId, defs, dnList, columns, expr)
	if err != nil {
		return nil, err
	}
	if len(bats) == 0 {
		return nil, moerr.NewInternalError("empty table: %v.%v", databaseId, tableId)
	}
	rows := make([][]any, 0, len(bats))
	for _, bat := range bats {
		if bat.Length() > 0 {
			rows = append(rows, catalog.GenRows(bat)...)
		}
		bat.Clean(txn.proc.Mp())
	}
	return rows, nil
}

// readTable used to get tuples of table based on a condition
// only used to read data from catalog, for which the execution is currently single-core
func (txn *Transaction) readTable(ctx context.Context, databaseId uint64, tableId uint64,
	defs []engine.TableDef, dnList []DNStore, columns []string, expr *plan.Expr) ([]*batch.Batch, error) {
	/*
		var writes [][]Entry
		// consider halloween problem
		if int64(txn.statementId)-1 > 0 {
			writes = txn.writes[:txn.statementId-1]
		}
	*/
	writes := make([]Entry, 0, len(txn.writes))
	for i := range txn.writes {
		for _, entry := range txn.writes[i] {
			if entry.databaseId == databaseId &&
				entry.tableId == tableId {
				writes = append(writes, entry)
			}
		}
	}
	bats := make([]*batch.Batch, 0, 1)
	accessed := make(map[string]uint8)
	for _, dn := range dnList {
		accessed[dn.GetUUID()] = 0
	}
	parts := txn.db.getPartitions(databaseId, tableId)
	for i, dn := range txn.dnStores {
		if _, ok := accessed[dn.GetUUID()]; !ok {
			continue
		}
		rds, err := parts[i].NewReader(ctx, 1, expr, defs, nil, txn.meta.SnapshotTS, writes)
		if err != nil {
			return nil, err
		}
		for _, rd := range rds {
			for {
				bat, err := rd.Read(columns, expr, txn.proc.Mp())
				if err != nil {
					return nil, err
				}
				if bat != nil {
					bats = append(bats, bat)
				} else {
					break
				}
			}
		}
	}

	for i, bat := range bats {
		vec, err := colexec.EvalExpr(bat, txn.proc, expr)
		if err != nil {
			return nil, err
		}
		bs := vector.GetColumn[bool](vec)
		if vec.IsScalar() {
			if !bs[0] {
				bat.Shrink(nil)
			}
		} else {
			sels := txn.proc.Mp().GetSels()
			for i, b := range bs {
				if b {
					sels = append(sels, int64(i))
				}
			}
			bat.Shrink(sels)
			txn.proc.Mp().PutSels(sels)
		}
		vec.Free(txn.proc.Mp())
		bats[i] = bat
	}
	return bats, nil
}

func (txn *Transaction) deleteBatch(bat *batch.Batch,
	databaseId, tableId uint64) *batch.Batch {
	mp := make(map[types.Rowid]uint8)
	rowids := vector.MustTCols[types.Rowid](bat.GetVector(0))
	for _, rowid := range rowids {
		mp[rowid] = 0
	}
	sels := txn.proc.Mp().GetSels()
	for i := range txn.writes {
		for j, e := range txn.writes[i] {
			sels = sels[:0]
			if e.tableId == tableId && e.databaseId == databaseId {
				vs := vector.MustTCols[types.Rowid](e.bat.GetVector(0))
				for k, v := range vs {
					if _, ok := mp[v]; !ok {
						sels = append(sels, int64(k))
					} else {
						mp[v]++
					}
				}
				if len(sels) != len(vs) {
					txn.writes[i][j].bat.Shrink(sels)
				}
			}
		}
	}
	sels = sels[:0]
	for k, rowid := range rowids {
		if mp[rowid] == 0 {
			sels = append(sels, int64(k))
		}
	}
	bat.Shrink(sels)
	txn.proc.Mp().PutSels(sels)
	return bat
}

func (txn *Transaction) genRowId() types.Rowid {
	txn.rowId[1]++
	return types.DecodeFixed[types.Rowid](types.EncodeSlice(txn.rowId[:]))
}

func (h transactionHeap) Len() int {
	return len(h)
}

func (h transactionHeap) Less(i, j int) bool {
	return h[i].meta.SnapshotTS.Less(h[j].meta.SnapshotTS)
}

func (h transactionHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *transactionHeap) Push(x any) {
	*h = append(*h, x.(*Transaction))
}

func (h *transactionHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
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
	buildVectors := buildVectorsByData(datas, dataTypes, proc.Mp())
	bat := batch.NewWithSize(len(columns))
	bat.Zs = make([]int64, buildVectors[0].Length())
	bat.Vecs = buildVectors

	ifNeed, err := evalFilterExpr(expr, bat, proc)
	if err != nil {
		return true
	}

	return ifNeed

}

// needSyncDnStores determine the dn store need to sync
func needSyncDnStores(expr *plan.Expr, defs []engine.TableDef, dnStores []DNStore) []int {
	//TODO
	dnList := make([]int, len(dnStores))
	for i := range dnStores {
		dnList[i] = i
	}
	return dnList
}

// get row count of block
func blockRows(blkInfo BlockMeta) int64 {
	// TODO
	return 0
}

func blockMarshal(blkInfo BlockMeta) []byte {
	// TODO
	return nil
}

func blockUnmarshal(data []byte) BlockMeta {
	return BlockMeta{}
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
