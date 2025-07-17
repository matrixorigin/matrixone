// Copyright 2024 Matrix Origin
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

package idxcdc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	sqlPrintLen            = 200
	fakeSql                = "fakeSql"
	createTable            = "create table"
	createTableIfNotExists = "create table if not exists"

	targetDbName = "test_async_index_cdc"
)

func NewConsumer(
	cnUUID string,
	tableDef *plan.TableDef,
	info *ConsumerInfo,
) (Consumer, error) {

	if info.ConsumerType == int8(ConsumerType_CNConsumer) {
		return NewInteralSqlConsumer(cnUUID, tableDef, info)
	}
	panic("todo")

}

var _ Consumer = &interalSqlConsumer{}

type interalSqlConsumer struct {
	internalSqlExecutor executor.SQLExecutor
	indexName           string

	dataRetriever DataRetriever
	tableInfo     *plan.TableDef

	targetTableName string

	// prefix of sql statement, e.g. `insert into xx values ...`
	insertPrefix []byte
	upsertPrefix []byte
	deletePrefix []byte
	// suffix of sql statement, e.g. `;` or `);`
	insertSuffix []byte
	deleteSuffix []byte

	// buf of row data from batch, e.g. values part of insert statement `insert into xx values (a),(b),(c)`
	// or `where ... in ... ` part of delete statement `delete from xx where pk in ((a),(b),(c))`
	rowBuf []byte
	// prefix of row buffer, e.g. `(`
	insertRowPrefix []byte
	deleteRowPrefix []byte
	// separator of col buffer, e.g. `,` or `and`
	insertColSeparator []byte
	deleteColSeparator []byte
	// suffix of row buffer, e.g. `)`
	insertRowSuffix []byte
	deleteRowSuffix []byte
	// separator of row buffer, e.g. `,` or `or`
	insertRowSeparator []byte
	deleteRowSeparator []byte

	// only contains user defined column types, no mo meta cols
	insertTypes []*types.Type
	// only contains pk columns
	deleteTypes []*types.Type
	// used for delete multi-col pk
	pkColNames []string

	// for collect row data, allocate only once
	insertRow []any
	deleteRow []any

	// insert or delete of last record, used for combine inserts and deletes
	preRowType RowType

	maxAllowedPacket uint64
}

func NewInteralSqlConsumer(
	cnUUID string,
	tableDef *plan.TableDef,
	info *ConsumerInfo,
) (Consumer, error) {
	s := &interalSqlConsumer{
		tableInfo: tableDef,
		indexName: info.IndexName,
	}
	s.maxAllowedPacket = uint64(1024 * 1024)
	logutil.Infof("cdc mysqlSinker(%v) maxAllowedPacket = %d", tableDef.Name, s.maxAllowedPacket)

	v, ok := moruntime.ServiceRuntime(cnUUID).
		GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	s.internalSqlExecutor = exec
	s.targetTableName = fmt.Sprintf("test_table_%d_%v", tableDef.TblId, info.IndexName)
	logutil.Infof("cdc %v->%vs", tableDef.Name, s.targetTableName)
	err := s.createTargetTable(context.Background())
	if err != nil {
		return nil, err
	}

	s.rowBuf = make([]byte, 0, 1024)

	// prefix and suffix
	s.insertPrefix = []byte(fmt.Sprintf("Insert INTO `%s`.`%s` VALUES ", targetDbName, s.targetTableName))
	s.upsertPrefix = []byte(fmt.Sprintf("Replace INTO `%s`.`%s` VALUES ", targetDbName, s.targetTableName))
	s.insertSuffix = []byte(";")
	s.insertRowPrefix = []byte("(")
	s.insertColSeparator = []byte(",")
	s.insertRowSuffix = []byte(")")
	s.insertRowSeparator = []byte(",")
	//                                     deleteRowSeparator
	//      |<- deletePrefix  ->| 			       v
	// e.g. delete from t1 where pk1=a1 and pk2=a2 or pk1=b1 and pk2=b2 or pk1=c1 and pk2=c2 ...;
	//                                   ^
	//                            deleteColSeparator
	s.deletePrefix = []byte(fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE ", targetDbName, s.targetTableName))
	s.deleteSuffix = []byte(";")
	s.deleteRowPrefix = []byte("")
	s.deleteColSeparator = []byte(" and ")
	s.deleteRowSuffix = []byte("")
	s.deleteRowSeparator = []byte(" or ")

	// types
	for _, col := range tableDef.Cols {
		// skip internal columns
		if _, ok := catalog.InternalColumns[col.Name]; ok {
			continue
		}

		s.insertTypes = append(s.insertTypes, &types.Type{
			Oid:   types.T(col.Typ.Id),
			Width: col.Typ.Width,
			Scale: col.Typ.Scale,
		})
	}
	for _, name := range tableDef.Pkey.Names {
		s.pkColNames = append(s.pkColNames, name)
		col := tableDef.Cols[tableDef.Name2ColIndex[name]]
		s.deleteTypes = append(s.deleteTypes, &types.Type{
			Oid:   types.T(col.Typ.Id),
			Width: col.Typ.Width,
			Scale: col.Typ.Scale,
		})
	}

	// rows
	s.insertRow = make([]any, len(s.insertTypes))
	s.deleteRow = make([]any, 1)

	// pre
	s.preRowType = NoOp

	return s, nil
}

func (s *interalSqlConsumer) createTargetTable(ctx context.Context) error {
	createDBSql := fmt.Sprintf("create database if not exists %s", targetDbName)
	srcCreateSql := s.tableInfo.Createsql
	if len(srcCreateSql) < len(createTableIfNotExists) || !strings.EqualFold(srcCreateSql[:len(createTableIfNotExists)], createTableIfNotExists) {
		srcCreateSql = createTableIfNotExists + srcCreateSql[len(createTable):]
	}
	tableStart := len(createTableIfNotExists)
	tableEnd := strings.Index(srcCreateSql, "(")
	newTablePart := fmt.Sprintf("%s.%s", targetDbName, s.targetTableName)
	createTableSql := srcCreateSql[:tableStart] + " " + newTablePart + srcCreateSql[tableEnd:]
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, err := s.internalSqlExecutor.Exec(ctx, createDBSql, executor.Options{})
	if err != nil {
		return err
	}
	_, err = s.internalSqlExecutor.Exec(ctx, createTableSql, executor.Options{})
	return err
}

func (s *interalSqlConsumer) Consume(ctx context.Context, data DataRetriever) error {
	s.dataRetriever = data
	if msg, injected := objectio.CDCExecutorInjected(); injected && msg == "consume" {
		return errors.New(msg)
	}
	if msg, injected := objectio.CDCExecutorInjected(); injected && strings.HasPrefix(msg, "consumeWithIndexName") {
		strs := strings.Split(msg, ":")
		for i := 1; i < len(strs); i++ {
			if s.indexName == strs[i] {
				return errors.New(strs[0])
			}
		}
	}

	err := s.internalSqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		for {
			insertBatch, deleteBatch, noMoreData, err := data.Next()
			if err != nil {
				return err
			}
			if noMoreData {

				if s.dataRetriever.GetDataType() != CDCDataType_Snapshot {
					err := s.dataRetriever.UpdateWatermark(txn, executor.StatementOption{})
					if err != nil {
						return err
					}
				}
				return nil
			}

			if data.GetDataType() == CDCDataType_Snapshot {
				err = s.sinkSnapshot(ctx, insertBatch.Batches[0], txn)
				if err != nil {
					return err
				}
			} else if data.GetDataType() == CDCDataType_Tail {
				err = s.sinkTail(ctx, insertBatch, deleteBatch, txn)
				if err != nil {
					return err
				}
			} else {
				panic("logic error")
			}
		}
	}, executor.Options{})
	return err
}

func (s *interalSqlConsumer) sinkSnapshot(ctx context.Context, bat *batch.Batch, txn executor.TxnExecutor) error {
	var err error

	// if last row is not insert row, means this is the first snapshot batch
	sqlBuffer := make([]byte, 0)

	for i := 0; i < batchRowCount(bat); i++ {
		if len(sqlBuffer) == 0 {
			sqlBuffer = append(sqlBuffer, s.upsertPrefix...)
			s.preRowType = UpsertRow
		}
		// step1: get row from the batch
		if err = extractRowFromEveryVector(ctx, bat, i, s.insertRow); err != nil {
			panic(err)
		}

		// step2: transform rows into sql parts
		if err = s.getInsertRowBuf(ctx); err != nil {
			panic(err)
		}

		// step3: append to sqlBuf, send sql if sqlBuf is full
		if sqlBuffer, err = s.appendSqlBuf(UpsertRow, sqlBuffer, txn); err != nil {
			panic(err)
		}
	}
	err = s.tryFlushSqlBuf(txn, sqlBuffer)
	if err != nil {
		return err
	}
	return nil
}

// insertBatch and deleteBatch is sorted by ts
// for the same ts, delete first, then insert
func (s *interalSqlConsumer) sinkTail(ctx context.Context, insertBatch, deleteBatch *AtomicBatch, txn executor.TxnExecutor) error {
	var err error

	insertIter := insertBatch.GetRowIterator().(*atomicBatchRowIter)
	deleteIter := deleteBatch.GetRowIterator().(*atomicBatchRowIter)
	defer func() {
		insertIter.Close()
		deleteIter.Close()
	}()

	// output sql until one iterator reach the end
	insertIterHasNext, deleteIterHasNext := insertIter.Next(), deleteIter.Next()

	sqlBuffer := make([]byte, 0)
	// output the rest of delete iterator
	for deleteIterHasNext {
		if len(sqlBuffer) == 0 {
			sqlBuffer = append(sqlBuffer, s.deletePrefix...)
			s.preRowType = DeleteRow
		}
		if sqlBuffer, err = s.sinkDelete(ctx, deleteIter, sqlBuffer, txn); err != nil {
			panic(err)
		}
		// get next item
		deleteIterHasNext = deleteIter.Next()
	}
	err = s.tryFlushSqlBuf(txn, sqlBuffer)
	if err != nil {
		return err
	}
	sqlBuffer = make([]byte, 0)
	// output the rest of insert iterator
	for insertIterHasNext {
		if len(sqlBuffer) == 0 {
			sqlBuffer = append(sqlBuffer, s.insertPrefix...)
			s.preRowType = InsertRow
		}
		if sqlBuffer, err = s.sinkInsert(ctx, insertIter, sqlBuffer, txn); err != nil {
			panic(err)
		}
		// get next item
		insertIterHasNext = insertIter.Next()
	}
	err = s.tryFlushSqlBuf(txn, sqlBuffer)
	if err != nil {
		return err
	}
	return nil
}

func (s *interalSqlConsumer) sinkInsert(ctx context.Context, insertIter *atomicBatchRowIter, sqlBuffer []byte, txn executor.TxnExecutor) (res []byte, err error) {
	// if last row is not insert row, need complete the last sql first
	if s.preRowType != InsertRow {
		panic("logic error")
	}

	// step1: get row from the batch
	if err = insertIter.Row(ctx, s.insertRow); err != nil {
		return
	}

	// step2: transform rows into sql parts
	if err = s.getInsertRowBuf(ctx); err != nil {
		return
	}

	// step3: append to sqlBuf
	if res, err = s.appendSqlBuf(InsertRow, sqlBuffer, txn); err != nil {
		return
	}

	return
}

func (s *interalSqlConsumer) sinkDelete(ctx context.Context, deleteIter *atomicBatchRowIter, sqlBuffer []byte, txn executor.TxnExecutor) (res []byte, err error) {
	// if last row is not insert row, need complete the last sql first
	if s.preRowType != DeleteRow {
		panic("logic error")
	}

	// step1: get row from the batch
	if err = deleteIter.Row(ctx, s.deleteRow); err != nil {
		return
	}

	// step2: transform rows into sql parts
	if err = s.getDeleteRowBuf(ctx); err != nil {
		return
	}

	// step3: append to sqlBuf
	if res, err = s.appendSqlBuf(DeleteRow, sqlBuffer, txn); err != nil {
		return
	}

	return
}

func (s *interalSqlConsumer) tryFlushSqlBuf(txn executor.TxnExecutor, sqlBuffer []byte) (err error) {
	if len(sqlBuffer) == 0 {
		return
	}
	if _, err := txn.Exec(string(sqlBuffer), executor.StatementOption{}); err != nil {
		logutil.Errorf("cdc interalSqlConsumer(%v) send sql failed, err: %v, sql: %s", s.tableInfo.Name, err, sqlBuffer[:])
		// record error
		panic(err)
	}
	return
}

// appendSqlBuf appends rowBuf to sqlBuf if not exceed its cap
// otherwise, send sql to downstream first, then reset sqlBuf and append
func (s *interalSqlConsumer) appendSqlBuf(rowType RowType, sqlBuffer []byte, txn executor.TxnExecutor) (res []byte, err error) {
	suffixLen := len(s.insertSuffix)
	if rowType == DeleteRow {
		suffixLen = len(s.deleteSuffix)
	}
	if rowType == UpsertRow {
		suffixLen = len(s.insertSuffix)
	}

	// if s.sqlBuf has no enough space
	if len(sqlBuffer)+len(s.rowBuf)+suffixLen > int(s.maxAllowedPacket) {
		switch rowType {
		case InsertRow:
			if len(sqlBuffer) == len(s.insertPrefix) {
				panic("logic error")
			}
		case DeleteRow:
			if len(sqlBuffer) == len(s.deletePrefix) {
				panic("logic error")
			}
		case UpsertRow:
			if len(sqlBuffer) == len(s.upsertPrefix) {
				panic("logic error")
			}
		default:
			panic(fmt.Sprintf("invalid row type %d", rowType))
		}
		// complete sql statement
		if s.isNonEmptyInsertStmt(sqlBuffer) || s.isNonEmptyUpsertStmt(sqlBuffer) {
			sqlBuffer = appendBytes(sqlBuffer, s.insertSuffix)
		}
		if s.isNonEmptyDeleteStmt(sqlBuffer) {
			sqlBuffer = appendBytes(sqlBuffer, s.deleteSuffix)
		}
		s.tryFlushSqlBuf(txn, sqlBuffer)
		sqlBuffer = make([]byte, 0)

		// reset s.sqlBuf
		switch rowType {
		case InsertRow:
			sqlBuffer = append(sqlBuffer, s.insertPrefix...)
		case DeleteRow:
			sqlBuffer = append(sqlBuffer, s.deletePrefix...)
		case UpsertRow:
			sqlBuffer = append(sqlBuffer, s.upsertPrefix...)
		default:
			panic(fmt.Sprintf("invalid row type %d", rowType))
		}
	}

	// append bytes
	if s.isNonEmptyInsertStmt(sqlBuffer) || s.isNonEmptyUpsertStmt(sqlBuffer) {
		sqlBuffer = appendBytes(sqlBuffer, s.insertRowSeparator)
	}
	if s.isNonEmptyDeleteStmt(sqlBuffer) {
		sqlBuffer = appendBytes(sqlBuffer, s.deleteRowSeparator)
	}
	sqlBuffer = append(sqlBuffer, s.rowBuf...)
	return sqlBuffer, nil
}

func (s *interalSqlConsumer) isNonEmptyDeleteStmt(sqlBuffer []byte) bool {
	return s.preRowType == DeleteRow && len(sqlBuffer) > len(s.deletePrefix)
}

func (s *interalSqlConsumer) isNonEmptyInsertStmt(sqlBuffer []byte) bool {
	return s.preRowType == InsertRow && len(sqlBuffer) > len(s.insertPrefix)
}

func (s *interalSqlConsumer) isNonEmptyUpsertStmt(sqlBuffer []byte) bool {
	return s.preRowType == UpsertRow && len(sqlBuffer) > len(s.upsertPrefix)
}

// getInsertRowBuf convert insert row to string
func (s *interalSqlConsumer) getInsertRowBuf(ctx context.Context) (err error) {
	s.rowBuf = appendBytes(s.rowBuf[:0], s.insertRowPrefix)
	for i := 0; i < len(s.insertRow); i++ {
		if i != 0 {
			s.rowBuf = appendBytes(s.rowBuf, s.insertColSeparator)
		}
		//transform column into text values
		if s.rowBuf, err = convertColIntoSql(ctx, s.insertRow[i], s.insertTypes[i], s.rowBuf); err != nil {
			return
		}
	}
	s.rowBuf = appendBytes(s.rowBuf, s.insertRowSuffix)
	return
}

var unpackWithSchema = types.UnpackWithSchema

// getDeleteRowBuf convert delete row to string
func (s *interalSqlConsumer) getDeleteRowBuf(ctx context.Context) (err error) {
	s.rowBuf = appendBytes(s.rowBuf[:0], s.deleteRowPrefix)

	if len(s.deleteTypes) == 1 {
		// single column pk
		// transform column into text values
		if s.rowBuf, err = convertColIntoSql(ctx, s.deleteRow[0], s.deleteTypes[0], s.rowBuf); err != nil {
			return
		}
	} else {
		// composite pk
		var pkTuple types.Tuple
		if pkTuple, _, err = unpackWithSchema(s.deleteRow[0].([]byte)); err != nil {
			return
		}
		for i, pkEle := range pkTuple {
			if i > 0 {
				s.rowBuf = appendBytes(s.rowBuf, s.deleteColSeparator)
			}
			//transform column into text values
			s.rowBuf = appendBytes(s.rowBuf, []byte(s.pkColNames[i]+"="))
			if s.rowBuf, err = convertColIntoSql(ctx, pkEle, s.deleteTypes[i], s.rowBuf); err != nil {
				return
			}
		}
	}

	s.rowBuf = appendBytes(s.rowBuf, s.deleteRowSuffix)
	return
}

func genPrimaryKeyStr(tableDef *plan.TableDef) string {
	buf := strings.Builder{}
	buf.WriteByte('(')
	for i, pkName := range tableDef.Pkey.Names {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(pkName)
	}
	buf.WriteByte(')')
	return buf.String()
}
