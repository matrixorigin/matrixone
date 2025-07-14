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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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

	dataRetriever DataRetriever
	tableInfo     *plan.TableDef

	targetTableName string

	// buf of sql statement
	sqlBufs      [2][]byte
	curBufIdx    int
	sqlBuf       []byte
	sqlBufSendCh chan []byte

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
	// the length of all completed sql statement in sqlBuf
	preSqlBufLen int

	wg sync.WaitGroup
}

func NewInteralSqlConsumer(
	cnUUID string,
	tableDef *plan.TableDef,
	info *ConsumerInfo,
) (Consumer, error) {
	s := &interalSqlConsumer{
		tableInfo: tableDef,
	}
	maxAllowedPacket := uint64(1024 * 1024)
	logutil.Infof("cdc mysqlSinker(%v) maxAllowedPacket = %d", tableDef.Name, maxAllowedPacket)

	v, ok := moruntime.ServiceRuntime(cnUUID).
		GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	s.internalSqlExecutor = exec
	s.targetTableName = fmt.Sprintf("test_table_%d", tableDef.TblId)
	logutil.Infof("cdc %v->%vs", tableDef.Name, s.targetTableName)
	err := s.createTargetTable(context.Background())
	if err != nil {
		return nil, err
	}

	// sqlBuf
	s.sqlBufs[0] = make([]byte, 0, maxAllowedPacket)
	s.sqlBufs[1] = make([]byte, 0, maxAllowedPacket)
	s.curBufIdx = 0
	s.sqlBuf = s.sqlBufs[s.curBufIdx]
	s.sqlBufSendCh = make(chan []byte)

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
	s.preSqlBufLen = 0

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

func (s *interalSqlConsumer) Run(ctx context.Context) {
	logutil.Infof("cdc interalSqlConsumer(%v).Run: start", s.tableInfo.Name)
	defer func() {
		logutil.Infof("cdc interalSqlConsumer(%v).Run: end", s.tableInfo.Name)
	}()

	s.wg.Add(1)
	defer s.wg.Done()

	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	err := s.internalSqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		for sqlBuffer := range s.sqlBufSendCh {
			if string(sqlBuffer) == "no more data" {
				if s.dataRetriever.GetDataType() != CDCDataType_Snapshot {
					s.dataRetriever.UpdateWatermark(txn, executor.StatementOption{})
				}
				return nil
			}
			if _, err := txn.Exec(string(sqlBuffer), executor.StatementOption{}); err != nil {
				logutil.Errorf("cdc interalSqlConsumer(%v) send sql failed, err: %v, sql: %s", s.tableInfo.Name, err, sqlBuffer[:])
				// record error
				panic(err)
			}
		}
		return nil
	}, executor.Options{})
	if err != nil {
		panic(err)
	}
}
func (s *interalSqlConsumer) Consume(ctx context.Context, data DataRetriever) error {
	s.dataRetriever = data

	go s.Run(context.Background())
	for {
		insertBatch, deleteBatch, noMoreData, err := data.Next()
		if err != nil {
			return err
		}
		if noMoreData {
			// complete sql statement
			if s.isNonEmptyInsertStmt() || s.isNonEmptyUpsertStmt() {
				s.sqlBuf = appendBytes(s.sqlBuf, s.insertSuffix)
				s.preSqlBufLen = len(s.sqlBuf)
			}
			if s.isNonEmptyDeleteStmt() {
				s.sqlBuf = appendBytes(s.sqlBuf, s.deleteSuffix)
				s.preSqlBufLen = len(s.sqlBuf)
			}

			// output the left sql
			if s.preSqlBufLen > 0 {
				s.sqlBufSendCh <- s.sqlBuf[:s.preSqlBufLen]
				s.curBufIdx ^= 1
				s.sqlBuf = s.sqlBufs[s.curBufIdx]
			}

			// reset status
			s.preSqlBufLen = 0
			s.sqlBuf = s.sqlBuf[:s.preSqlBufLen]
			s.preRowType = NoOp

			s.sqlBufSendCh <- []byte("no more data")
			s.wg.Wait()
			return nil
		}

		if data.GetDataType() == CDCDataType_Snapshot {
			s.sinkSnapshot(ctx, insertBatch.Batches[0])
		} else if data.GetDataType() == CDCDataType_Tail {
			s.sinkTail(ctx, insertBatch, deleteBatch)
		}
	}
}

func (s *interalSqlConsumer) sinkSnapshot(ctx context.Context, bat *batch.Batch) {
	var err error

	// if last row is not insert row, means this is the first snapshot batch
	if s.preRowType != UpsertRow {
		s.sqlBuf = append(s.sqlBuf[:0], s.upsertPrefix...)
		s.preRowType = UpsertRow
	}

	for i := 0; i < batchRowCount(bat); i++ {
		// step1: get row from the batch
		if err = extractRowFromEveryVector(ctx, bat, i, s.insertRow); err != nil {
			panic(err)
		}

		// step2: transform rows into sql parts
		if err = s.getInsertRowBuf(ctx); err != nil {
			panic(err)
		}

		// step3: append to sqlBuf, send sql if sqlBuf is full
		if err = s.appendSqlBuf(InsertRow); err != nil {
			panic(err)
		}
	}
}

// insertBatch and deleteBatch is sorted by ts
// for the same ts, delete first, then insert
func (s *interalSqlConsumer) sinkTail(ctx context.Context, insertBatch, deleteBatch *AtomicBatch) {
	var err error

	insertIter := insertBatch.GetRowIterator().(*atomicBatchRowIter)
	deleteIter := deleteBatch.GetRowIterator().(*atomicBatchRowIter)
	defer func() {
		insertIter.Close()
		deleteIter.Close()
	}()

	// output sql until one iterator reach the end
	insertIterHasNext, deleteIterHasNext := insertIter.Next(), deleteIter.Next()

	// output the rest of insert iterator
	for insertIterHasNext {
		if err = s.sinkInsert(ctx, insertIter); err != nil {
			panic(err)
		}
		// get next item
		insertIterHasNext = insertIter.Next()
	}
	s.tryFlushSqlBuf()

	// output the rest of delete iterator
	for deleteIterHasNext {
		if err = s.sinkDelete(ctx, deleteIter); err != nil {
			panic(err)
		}
		// get next item
		deleteIterHasNext = deleteIter.Next()
	}
	s.tryFlushSqlBuf()
}

func (s *interalSqlConsumer) sinkInsert(ctx context.Context, insertIter *atomicBatchRowIter) (err error) {
	// if last row is not insert row, need complete the last sql first
	if s.preRowType != InsertRow {
		if s.isNonEmptyDeleteStmt() {
			s.sqlBuf = appendBytes(s.sqlBuf, s.deleteSuffix)
			s.preSqlBufLen = len(s.sqlBuf)
		}
		s.sqlBuf = append(s.sqlBuf[:s.preSqlBufLen], s.insertPrefix...)
		s.preRowType = InsertRow
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
	if err = s.appendSqlBuf(InsertRow); err != nil {
		return
	}

	return
}

func (s *interalSqlConsumer) sinkDelete(ctx context.Context, deleteIter *atomicBatchRowIter) (err error) {
	// if last row is not insert row, need complete the last sql first
	if s.preRowType != DeleteRow {
		if s.isNonEmptyInsertStmt() {
			s.sqlBuf = appendBytes(s.sqlBuf, s.insertSuffix)
			s.preSqlBufLen = len(s.sqlBuf)
		}
		s.sqlBuf = append(s.sqlBuf[:s.preSqlBufLen], s.deletePrefix...)
		s.preRowType = DeleteRow
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
	if err = s.appendSqlBuf(DeleteRow); err != nil {
		return
	}

	return
}

func (s *interalSqlConsumer) tryFlushSqlBuf() (err error) {
	if s.preSqlBufLen == 0 {
		return
	}
	if s.isNonEmptyInsertStmt() || s.isNonEmptyUpsertStmt() {
		s.sqlBuf = appendBytes(s.sqlBuf, s.insertSuffix)
		s.preSqlBufLen = len(s.sqlBuf)
	}
	if s.isNonEmptyDeleteStmt() {
		s.sqlBuf = appendBytes(s.sqlBuf, s.deleteSuffix)
		s.preSqlBufLen = len(s.sqlBuf)
	}
	// send it to downstream
	s.sqlBufSendCh <- s.sqlBuf[:s.preSqlBufLen]
	s.curBufIdx ^= 1
	s.sqlBuf = s.sqlBufs[s.curBufIdx]

	s.preSqlBufLen = 0
	s.sqlBuf = s.sqlBuf[:s.preSqlBufLen]
	s.preRowType = NoOp
	return
}

// appendSqlBuf appends rowBuf to sqlBuf if not exceed its cap
// otherwise, send sql to downstream first, then reset sqlBuf and append
func (s *interalSqlConsumer) appendSqlBuf(rowType RowType) (err error) {
	suffixLen := len(s.insertSuffix)
	if rowType == DeleteRow {
		suffixLen = len(s.deleteSuffix)
	}

	// if s.sqlBuf has no enough space
	if len(s.sqlBuf)+len(s.rowBuf)+suffixLen > cap(s.sqlBuf) {
		// complete sql statement
		if s.isNonEmptyInsertStmt() || s.isNonEmptyUpsertStmt() {
			s.sqlBuf = appendBytes(s.sqlBuf, s.insertSuffix)
			s.preSqlBufLen = len(s.sqlBuf)
		}
		if s.isNonEmptyDeleteStmt() {
			s.sqlBuf = appendBytes(s.sqlBuf, s.deleteSuffix)
			s.preSqlBufLen = len(s.sqlBuf)
		}

		// send it to downstream
		s.sqlBufSendCh <- s.sqlBuf[:s.preSqlBufLen]
		s.curBufIdx ^= 1
		s.sqlBuf = s.sqlBufs[s.curBufIdx]

		// reset s.sqlBuf
		s.preSqlBufLen = 0
		if rowType == InsertRow {
			s.sqlBuf = append(s.sqlBuf[:s.preSqlBufLen], s.insertPrefix...)
		} else {
			s.sqlBuf = append(s.sqlBuf[:s.preSqlBufLen], s.deletePrefix...)
		}
	}

	// append bytes
	if s.isNonEmptyInsertStmt() || s.isNonEmptyUpsertStmt() {
		s.sqlBuf = appendBytes(s.sqlBuf, s.insertRowSeparator)
	}
	if s.isNonEmptyDeleteStmt() {
		s.sqlBuf = appendBytes(s.sqlBuf, s.deleteRowSeparator)
	}
	s.sqlBuf = append(s.sqlBuf, s.rowBuf...)
	return
}

func (s *interalSqlConsumer) isNonEmptyDeleteStmt() bool {
	return s.preRowType == DeleteRow && len(s.sqlBuf)-s.preSqlBufLen > len(s.deletePrefix)
}

func (s *interalSqlConsumer) isNonEmptyInsertStmt() bool {
	return s.preRowType == InsertRow && len(s.sqlBuf)-s.preSqlBufLen > len(s.insertPrefix)
}

func (s *interalSqlConsumer) isNonEmptyUpsertStmt() bool {
	return s.preRowType == UpsertRow && len(s.sqlBuf)-s.preSqlBufLen > len(s.upsertPrefix)
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
