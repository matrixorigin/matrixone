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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func (txn *Transaction) getTableList(ctx context.Context, databaseId uint64) ([]string, error) {
	rows, err := txn.getRows(ctx, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID, txn.dnStores[:1],
		[]string{catalog.MoTablesSchema[catalog.MO_TABLES_REL_NAME_IDX]})
	if err != nil {
		return nil, err
	}
	tableList := make([]string, len(rows))
	for i := range rows {
		tableList[i] = rows[i][0].(string)
	}
	return tableList, nil
}

func (txn *Transaction) getTableId(ctx context.Context, databaseId uint64,
	name string) (uint64, error) {
	row, err := txn.getRow(ctx, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		txn.dnStores[:1], []string{catalog.MoTablesSchema[catalog.MO_TABLES_REL_ID_IDX]},
		genTableIdExpr(databaseId, name))
	if err != nil {
		return 0, err
	}
	return row[0].(uint64), nil
}

func (txn *Transaction) getDatabaseList(ctx context.Context) ([]string, error) {
	rows, err := txn.getRows(ctx, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		txn.dnStores[:1], []string{catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_NAME_IDX]})
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
	row, err := txn.getRow(ctx, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID, txn.dnStores[:1],
		[]string{catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_ID_IDX]}, genDatabaseIdExpr(name))
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
	dnList []DNStore, columns []string, expr *plan.Expr) ([]any, error) {
	bats, err := txn.readTable(ctx, databaseId, tableId, dnList, columns, expr)
	if err != nil {
		return nil, nil
	}
	if len(bats) != 1 {
		return nil, nil
	}
	if bats[0].Length() != 1 {
		return nil, nil
	}
	return nil, nil
}

// getRows used to get rows of table
func (txn *Transaction) getRows(ctx context.Context, databaseId uint64, tableId uint64,
	dnList []DNStore, columns []string) ([][]any, error) {
	bats, err := txn.readTable(ctx, databaseId, tableId, dnList, columns, nil)
	if err != nil {
		return nil, nil
	}
	if len(bats) == 0 {
		return nil, nil
	}
	if bats[0].Length() != 1 {
		return nil, nil
	}
	return nil, nil
}

// readTable used to get tuples of table based on a condition
// only used to read data from catalog, for which the execution is currently single-core
func (txn *Transaction) readTable(ctx context.Context, databaseId uint64, tableId uint64,
	dnList []DNStore, columns []string, expr *plan.Expr) ([]*batch.Batch, error) {
	var writes [][]Entry

	// consider halloween problem
	if int64(txn.statementId)-1 > 0 {
		writes = txn.writes[:txn.statementId-1]
	}
	blkInfos := txn.db.BlockList(ctx, dnList, databaseId, tableId, txn.meta.SnapshotTS, writes)
	bats := make([]*batch.Batch, 0, len(blkInfos))
	for _, blkInfo := range blkInfos {
		if !needRead(expr, blkInfo) {
			continue
		}
		bat, err := blockRead(ctx, columns, blkInfo)
		if err != nil {
			return nil, err
		}
		bats = append(bats, bat)
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
func needRead(expr *plan.Expr, blkInfo BlockMeta) bool {
	//TODO
	return false
}

// write a block to s3
func blockWrite(ctx context.Context, blkInfo BlockMeta, bat *batch.Batch) error {
	//TODO
	return nil
}

// read a block from s3
func blockRead(ctx context.Context, columns []string, blkInfo BlockMeta) (*batch.Batch, error) {
	//TODO
	return nil, nil
}
