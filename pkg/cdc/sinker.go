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

package cdc

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/mysql"
)

const (
	// sqlBufReserved leave 5 bytes for mysql driver
	sqlBufReserved         = 5
	sqlPrintLen            = 200
	fakeSql                = "fakeSql"
	createTable            = "create table"
	createTableIfNotExists = "create table if not exists"
)

var (
	begin    = []byte("begin")
	commit   = []byte("commit")
	rollback = []byte("rollback")
	dummy    = []byte("")
)

var NewSinker = func(
	sinkUri UriInfo,
	dbTblInfo *DbTableInfo,
	watermarkUpdater IWatermarkUpdater,
	tableDef *plan.TableDef,
	retryTimes int,
	retryDuration time.Duration,
	ar *ActiveRoutine,
	maxSqlLength uint64,
	sendSqlTimeout string,
) (Sinker, error) {
	//TODO: remove console
	if sinkUri.SinkTyp == ConsoleSink {
		return NewConsoleSinker(dbTblInfo, watermarkUpdater), nil
	}

	sink, err := NewMysqlSink(sinkUri.User, sinkUri.Password, sinkUri.Ip, sinkUri.Port, retryTimes, retryDuration, sendSqlTimeout)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	padding := strings.Repeat(" ", sqlBufReserved)
	// create db
	_ = sink.Send(ctx, ar, []byte(padding+fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbTblInfo.SinkDbName)))
	// use db
	_ = sink.Send(ctx, ar, []byte(padding+fmt.Sprintf("use `%s`", dbTblInfo.SinkDbName)))
	// create table
	createSql := strings.TrimSpace(dbTblInfo.SourceCreateSql)
	if len(createSql) < len(createTableIfNotExists) || !strings.EqualFold(createSql[:len(createTableIfNotExists)], createTableIfNotExists) {
		createSql = createTableIfNotExists + createSql[len(createTable):]
	}
	createSql = strings.ReplaceAll(createSql, dbTblInfo.SourceDbName, dbTblInfo.SinkDbName)
	createSql = strings.ReplaceAll(createSql, dbTblInfo.SourceTblName, dbTblInfo.SinkTblName)
	_ = sink.Send(ctx, ar, []byte(padding+createSql))

	return NewMysqlSinker(sink, dbTblInfo, watermarkUpdater, tableDef, ar, maxSqlLength), nil
}

var _ Sinker = new(consoleSinker)

type consoleSinker struct {
	dbTblInfo        *DbTableInfo
	watermarkUpdater IWatermarkUpdater
}

func NewConsoleSinker(
	dbTblInfo *DbTableInfo,
	watermarkUpdater IWatermarkUpdater,
) Sinker {
	return &consoleSinker{
		dbTblInfo:        dbTblInfo,
		watermarkUpdater: watermarkUpdater,
	}
}

func (s *consoleSinker) Run(_ context.Context, _ *ActiveRoutine) {}

func (s *consoleSinker) Sink(ctx context.Context, data *DecoderOutput) {
	logutil.Info("====console sinker====")

	logutil.Infof("output type %s", data.outputTyp)
	switch data.outputTyp {
	case OutputTypeSnapshot:
		if data.checkpointBat != nil && data.checkpointBat.RowCount() > 0 {
			//FIXME: only test here
			logutil.Info("checkpoint")
			//logutil.Info(data.checkpointBat.String())
		}
	case OutputTypeTail:
		if data.insertAtmBatch != nil && data.insertAtmBatch.Rows.Len() > 0 {
			//FIXME: only test here
			wantedColCnt := len(data.insertAtmBatch.Batches[0].Vecs) - 2
			row := make([]any, wantedColCnt)
			wantedColIndice := make([]int, wantedColCnt)
			for i := 0; i < wantedColCnt; i++ {
				wantedColIndice[i] = i
			}

			iter := data.insertAtmBatch.GetRowIterator()
			for iter.Next() {
				_ = iter.Row(ctx, row)
				logutil.Infof("insert %v", row)
			}
			iter.Close()
		}
	}
}

func (s *consoleSinker) SendBegin() {}

func (s *consoleSinker) SendCommit() {}

func (s *consoleSinker) SendRollback() {}

func (s *consoleSinker) SendDummy() {}

func (s *consoleSinker) Error() error {
	return nil
}

func (s *consoleSinker) Reset() {}

func (s *consoleSinker) Close() {}

var _ Sinker = new(mysqlSinker)

type mysqlSinker struct {
	mysql            Sink
	dbTblInfo        *DbTableInfo
	watermarkUpdater IWatermarkUpdater
	ar               *ActiveRoutine

	// buf of sql statement
	sqlBufs      [2][]byte
	curBufIdx    int
	sqlBuf       []byte
	sqlBufSendCh chan []byte

	// buf of row data from batch, e.g. values part of insert statement (insert into xx values (a), (b), (c))
	// or `where ... in ... ` part of delete statement (delete from xx where pk in ((a), (b), (c)))
	rowBuf         []byte
	insertPrefix   []byte
	deletePrefix   []byte
	tsInsertPrefix []byte
	tsDeletePrefix []byte

	// only contains user defined column types, no mo meta cols
	insertTypes []*types.Type
	// only contains pk columns
	deleteTypes []*types.Type

	// for collect row data, allocate only once
	insertRow []any
	deleteRow []any

	// insert or delete of last record, used for combine inserts and deletes
	preRowType RowType
	// the length of all completed sql statement in sqlBuf
	preSqlBufLen int

	err atomic.Value
}

var NewMysqlSinker = func(
	mysql Sink,
	dbTblInfo *DbTableInfo,
	watermarkUpdater IWatermarkUpdater,
	tableDef *plan.TableDef,
	ar *ActiveRoutine,
	maxSqlLength uint64,
) Sinker {
	s := &mysqlSinker{
		mysql:            mysql,
		dbTblInfo:        dbTblInfo,
		watermarkUpdater: watermarkUpdater,
		ar:               ar,
	}
	var maxAllowedPacket uint64
	_ = mysql.(*mysqlSink).conn.QueryRow("SELECT @@max_allowed_packet").Scan(&maxAllowedPacket)
	maxAllowedPacket = min(maxAllowedPacket, maxSqlLength)
	logutil.Infof("cdc mysqlSinker(%v) maxAllowedPacket = %d", s.dbTblInfo, maxAllowedPacket)

	// sqlBuf
	s.sqlBufs[0] = make([]byte, sqlBufReserved, maxAllowedPacket)
	s.sqlBufs[1] = make([]byte, sqlBufReserved, maxAllowedPacket)
	s.curBufIdx = 0
	s.sqlBuf = s.sqlBufs[s.curBufIdx]
	s.sqlBufSendCh = make(chan []byte)

	s.rowBuf = make([]byte, 0, 1024)

	// prefix
	s.insertPrefix = []byte(fmt.Sprintf("REPLACE INTO `%s`.`%s` VALUES ", s.dbTblInfo.SinkDbName, s.dbTblInfo.SinkTblName))
	s.deletePrefix = []byte(fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s IN (", s.dbTblInfo.SinkDbName, s.dbTblInfo.SinkTblName, genPrimaryKeyStr(tableDef)))
	s.tsInsertPrefix = make([]byte, 0, 1024)
	s.tsDeletePrefix = make([]byte, 0, 1024)

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
	s.preSqlBufLen = sqlBufReserved

	// err
	s.err = atomic.Value{}
	return s
}

func (s *mysqlSinker) Run(ctx context.Context, ar *ActiveRoutine) {
	logutil.Infof("cdc mysqlSinker(%v).Run: start", s.dbTblInfo)
	defer func() {
		logutil.Infof("cdc mysqlSinker(%v).Run: end", s.dbTblInfo)
	}()

	for sqlBuf := range s.sqlBufSendCh {
		// have error, skip
		if s.err.Load() != nil {
			continue
		}

		if bytes.Equal(sqlBuf, dummy) {
			// dummy sql, do nothing
		} else if bytes.Equal(sqlBuf, begin) {
			if err := s.mysql.SendBegin(ctx, ar); err != nil {
				logutil.Errorf("cdc mysqlSinker(%v) SendBegin, err: %v", s.dbTblInfo, err)
				// record error
				s.err.Store(err)
			}
		} else if bytes.Equal(sqlBuf, commit) {
			if err := s.mysql.SendCommit(ctx, ar); err != nil {
				logutil.Errorf("cdc mysqlSinker(%v) SendCommit, err: %v", s.dbTblInfo, err)
				// record error
				s.err.Store(err)
			}
		} else if bytes.Equal(sqlBuf, rollback) {
			if err := s.mysql.SendRollback(ctx, ar); err != nil {
				logutil.Errorf("cdc mysqlSinker(%v) SendRollback, err: %v", s.dbTblInfo, err)
				// record error
				s.err.Store(err)
			}
		} else {
			if err := s.mysql.Send(ctx, ar, sqlBuf); err != nil {
				logutil.Errorf("cdc mysqlSinker(%v) send sql failed, err: %v, sql: %s", s.dbTblInfo, err, sqlBuf[sqlBufReserved:])
				// record error
				s.err.Store(err)
			}
		}
	}
}

func (s *mysqlSinker) Sink(ctx context.Context, data *DecoderOutput) {
	watermark := s.watermarkUpdater.GetFromMem(s.dbTblInfo.SourceDbName, s.dbTblInfo.SourceTblName)
	if data.toTs.LE(&watermark) {
		logutil.Errorf("cdc mysqlSinker(%v): unexpected watermark: %s, current watermark: %s",
			s.dbTblInfo, data.toTs.ToString(), watermark.ToString())
		return
	}

	if data.noMoreData {
		// complete sql statement
		if s.isNonEmptyInsertStmt() {
			s.sqlBuf = appendString(s.sqlBuf, ";")
			s.preSqlBufLen = len(s.sqlBuf)
		}
		if s.isNonEmptyDeleteStmt() {
			s.sqlBuf = appendString(s.sqlBuf, ");")
			s.preSqlBufLen = len(s.sqlBuf)
		}

		// output the left sql
		if s.preSqlBufLen > sqlBufReserved {
			s.sqlBufSendCh <- s.sqlBuf
			s.curBufIdx ^= 1
			s.sqlBuf = s.sqlBufs[s.curBufIdx]
		}

		// reset status
		s.preSqlBufLen = sqlBufReserved
		s.sqlBuf = s.sqlBuf[:s.preSqlBufLen]
		s.preRowType = NoOp
		return
	}

	start := time.Now()
	defer func() {
		v2.CdcSinkDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	tsPrefix := fmt.Sprintf("/* [%s, %s) */", data.fromTs.ToString(), data.toTs.ToString())
	s.tsInsertPrefix = s.tsInsertPrefix[:0]
	s.tsInsertPrefix = append(s.tsInsertPrefix, []byte(tsPrefix)...)
	s.tsInsertPrefix = append(s.tsInsertPrefix, s.insertPrefix...)
	s.tsDeletePrefix = s.tsDeletePrefix[:0]
	s.tsDeletePrefix = append(s.tsDeletePrefix, []byte(tsPrefix)...)
	s.tsDeletePrefix = append(s.tsDeletePrefix, s.deletePrefix...)

	if data.outputTyp == OutputTypeSnapshot {
		s.sinkSnapshot(ctx, data.checkpointBat)
	} else if data.outputTyp == OutputTypeTail {
		s.sinkTail(ctx, data.insertAtmBatch, data.deleteAtmBatch)
	} else {
		s.err.Store(moerr.NewInternalError(ctx, fmt.Sprintf("cdc mysqlSinker unexpected output type: %v", data.outputTyp)))
	}
}

func (s *mysqlSinker) SendBegin() {
	s.sqlBufSendCh <- begin
}

func (s *mysqlSinker) SendCommit() {
	s.sqlBufSendCh <- commit
}

func (s *mysqlSinker) SendRollback() {
	s.sqlBufSendCh <- rollback
}

func (s *mysqlSinker) SendDummy() {
	s.sqlBufSendCh <- dummy
}

func (s *mysqlSinker) Error() error {
	if val := s.err.Load(); val == nil {
		return nil
	} else {
		return val.(error)
	}
}

func (s *mysqlSinker) Reset() {
	s.sqlBufs[0] = s.sqlBufs[0][:sqlBufReserved]
	s.sqlBufs[1] = s.sqlBufs[1][:sqlBufReserved]
	s.curBufIdx = 0
	s.sqlBuf = s.sqlBufs[s.curBufIdx]
	s.preRowType = NoOp
	s.preSqlBufLen = sqlBufReserved
	s.err = atomic.Value{}
}

func (s *mysqlSinker) Close() {
	// stop Run goroutine
	close(s.sqlBufSendCh)
	s.mysql.Close()
	s.sqlBufs[0] = nil
	s.sqlBufs[1] = nil
	s.sqlBuf = nil
	s.rowBuf = nil
	s.insertPrefix = nil
	s.deletePrefix = nil
	s.tsInsertPrefix = nil
	s.tsDeletePrefix = nil
	s.insertTypes = nil
	s.deleteTypes = nil
	s.insertRow = nil
	s.deleteRow = nil
}

func (s *mysqlSinker) sinkSnapshot(ctx context.Context, bat *batch.Batch) {
	var err error

	// if last row is not insert row, means this is the first snapshot batch
	if s.preRowType != InsertRow {
		s.sqlBuf = append(s.sqlBuf[:sqlBufReserved], s.tsInsertPrefix...)
		s.preRowType = InsertRow
	}

	for i := 0; i < batchRowCount(bat); i++ {
		// step1: get row from the batch
		if err = extractRowFromEveryVector(ctx, bat, i, s.insertRow); err != nil {
			s.err.Store(err)
			return
		}

		// step2: transform rows into sql parts
		if err = s.getInsertRowBuf(ctx); err != nil {
			s.err.Store(err)
			return
		}

		// step3: append to sqlBuf, send sql if sqlBuf is full
		if err = s.appendSqlBuf(InsertRow); err != nil {
			s.err.Store(err)
			return
		}
	}
}

// insertBatch and deleteBatch is sorted by ts
// for the same ts, delete first, then insert
func (s *mysqlSinker) sinkTail(ctx context.Context, insertBatch, deleteBatch *AtomicBatch) {
	var err error

	insertIter := insertBatch.GetRowIterator().(*atomicBatchRowIter)
	deleteIter := deleteBatch.GetRowIterator().(*atomicBatchRowIter)
	defer func() {
		insertIter.Close()
		deleteIter.Close()
	}()

	// output sql until one iterator reach the end
	insertIterHasNext, deleteIterHasNext := insertIter.Next(), deleteIter.Next()
	for insertIterHasNext && deleteIterHasNext {
		insertItem, deleteItem := insertIter.Item(), deleteIter.Item()
		// compare ts, ignore pk
		if insertItem.Ts.LT(&deleteItem.Ts) {
			if err = s.sinkInsert(ctx, insertIter); err != nil {
				s.err.Store(err)
				return
			}
			// get next item
			insertIterHasNext = insertIter.Next()
		} else {
			if err = s.sinkDelete(ctx, deleteIter); err != nil {
				s.err.Store(err)
				return
			}
			// get next item
			deleteIterHasNext = deleteIter.Next()
		}
	}

	// output the rest of insert iterator
	for insertIterHasNext {
		if err = s.sinkInsert(ctx, insertIter); err != nil {
			s.err.Store(err)
			return
		}
		// get next item
		insertIterHasNext = insertIter.Next()
	}

	// output the rest of delete iterator
	for deleteIterHasNext {
		if err = s.sinkDelete(ctx, deleteIter); err != nil {
			s.err.Store(err)
			return
		}
		// get next item
		deleteIterHasNext = deleteIter.Next()
	}
}

func (s *mysqlSinker) sinkInsert(ctx context.Context, insertIter *atomicBatchRowIter) (err error) {
	// if last row is not insert row, need complete the last sql first
	if s.preRowType != InsertRow {
		if s.isNonEmptyDeleteStmt() {
			s.sqlBuf = appendString(s.sqlBuf, ");")
			s.preSqlBufLen = len(s.sqlBuf)
		}
		s.sqlBuf = append(s.sqlBuf[:s.preSqlBufLen], s.tsInsertPrefix...)
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

func (s *mysqlSinker) sinkDelete(ctx context.Context, deleteIter *atomicBatchRowIter) (err error) {
	// if last row is not insert row, need complete the last sql first
	if s.preRowType != DeleteRow {
		if s.isNonEmptyInsertStmt() {
			s.sqlBuf = appendString(s.sqlBuf, ";")
			s.preSqlBufLen = len(s.sqlBuf)
		}
		s.sqlBuf = append(s.sqlBuf[:s.preSqlBufLen], s.tsDeletePrefix...)
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

// appendSqlBuf appends rowBuf to sqlBuf if not exceed its cap
// otherwise, send sql to downstream first, then reset sqlBuf and append
func (s *mysqlSinker) appendSqlBuf(rowType RowType) (err error) {
	// insert suffix: `;`, delete suffix: `);`
	suffixLen := 1
	if rowType == DeleteRow {
		suffixLen = 2
	}

	// if s.sqlBuf has no enough space
	if len(s.sqlBuf)+len(s.rowBuf)+suffixLen > cap(s.sqlBuf) {
		// complete sql statement
		if rowType == InsertRow {
			s.sqlBuf = appendString(s.sqlBuf, ";")
		} else {
			s.sqlBuf = appendString(s.sqlBuf, ");")
		}

		// send it to downstream
		s.sqlBufSendCh <- s.sqlBuf
		s.curBufIdx ^= 1
		s.sqlBuf = s.sqlBufs[s.curBufIdx]

		// reset s.sqlBuf
		s.preSqlBufLen = sqlBufReserved
		if rowType == InsertRow {
			s.sqlBuf = append(s.sqlBuf[:s.preSqlBufLen], s.tsInsertPrefix...)
		} else {
			s.sqlBuf = append(s.sqlBuf[:s.preSqlBufLen], s.tsDeletePrefix...)
		}
	}

	// append bytes
	if s.isNonEmptyDeleteStmt() || s.isNonEmptyInsertStmt() {
		s.sqlBuf = appendByte(s.sqlBuf, ',')
	}
	s.sqlBuf = append(s.sqlBuf, s.rowBuf...)
	return
}

func (s *mysqlSinker) isNonEmptyDeleteStmt() bool {
	return s.preRowType == DeleteRow && len(s.sqlBuf)-s.preSqlBufLen > len(s.tsDeletePrefix)
}

func (s *mysqlSinker) isNonEmptyInsertStmt() bool {
	return s.preRowType == InsertRow && len(s.sqlBuf)-s.preSqlBufLen > len(s.tsInsertPrefix)
}

// getInsertRowBuf convert insert row to string
func (s *mysqlSinker) getInsertRowBuf(ctx context.Context) (err error) {
	s.rowBuf = append(s.rowBuf[:0], '(')
	for i := 0; i < len(s.insertRow); i++ {
		if i != 0 {
			s.rowBuf = appendByte(s.rowBuf, ',')
		}
		//transform column into text values
		if s.rowBuf, err = convertColIntoSql(ctx, s.insertRow[i], s.insertTypes[i], s.rowBuf); err != nil {
			return
		}
	}
	s.rowBuf = appendByte(s.rowBuf, ')')
	return
}

var unpackWithSchema = types.UnpackWithSchema

// getDeleteRowBuf convert delete row to string
func (s *mysqlSinker) getDeleteRowBuf(ctx context.Context) (err error) {
	s.rowBuf = append(s.rowBuf[:0], '(')

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
				s.rowBuf = appendByte(s.rowBuf, ',')
			}
			//transform column into text values
			if s.rowBuf, err = convertColIntoSql(ctx, pkEle, s.deleteTypes[i], s.rowBuf); err != nil {
				return
			}
		}
	}

	s.rowBuf = appendByte(s.rowBuf, ')')
	return
}

var _ Sink = new(mysqlSink)

type mysqlSink struct {
	conn           *sql.DB
	tx             *sql.Tx
	user, password string
	ip             string
	port           int

	retryTimes    int
	retryDuration time.Duration
	timeout       string
}

var NewMysqlSink = func(
	user, password string,
	ip string, port int,
	retryTimes int,
	retryDuration time.Duration,
	timeout string,
) (Sink, error) {
	ret := &mysqlSink{
		user:          user,
		password:      password,
		ip:            ip,
		port:          port,
		retryTimes:    retryTimes,
		retryDuration: retryDuration,
		timeout:       timeout,
	}
	err := ret.connect()
	return ret, err
}

// Send must leave 5 bytes at the head of sqlBuf
func (s *mysqlSink) Send(ctx context.Context, ar *ActiveRoutine, sqlBuf []byte) error {
	reuseQueryArg := sql.NamedArg{
		Name:  mysql.ReuseQueryBuf,
		Value: sqlBuf,
	}

	return s.retry(ctx, ar, func() (err error) {
		if s.tx != nil {
			_, err = s.tx.Exec(fakeSql, reuseQueryArg)
		} else {
			_, err = s.conn.Exec(fakeSql, reuseQueryArg)
		}

		if err != nil {
			logutil.Errorf("cdc mysqlSink Send failed, err: %v, sql: %s", err, sqlBuf[sqlBufReserved:min(len(sqlBuf), sqlPrintLen)])
		}
		//logutil.Errorf("----cdc mysqlSink send sql----, success, sql: %s", sql)
		return
	})
}

func (s *mysqlSink) SendBegin(ctx context.Context, ar *ActiveRoutine) (err error) {
	return s.retry(ctx, ar, func() (err error) {
		s.tx, err = s.conn.BeginTx(ctx, nil)
		return err
	})
}

func (s *mysqlSink) SendCommit(ctx context.Context, ar *ActiveRoutine) error {
	defer func() {
		s.tx = nil
	}()

	return s.retry(ctx, ar, func() error {
		return s.tx.Commit()
	})
}

func (s *mysqlSink) SendRollback(ctx context.Context, ar *ActiveRoutine) error {
	defer func() {
		s.tx = nil
	}()

	return s.retry(ctx, ar, func() error {
		return s.tx.Rollback()
	})
}

func (s *mysqlSink) Close() {
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
}

func (s *mysqlSink) connect() (err error) {
	s.conn, err = OpenDbConn(s.user, s.password, s.ip, s.port, s.timeout)
	return err
}

func (s *mysqlSink) retry(ctx context.Context, ar *ActiveRoutine, fn func() error) (err error) {
	needRetry := func(retry int, startTime time.Time) bool {
		// retryTimes == -1 means retry forever
		// do not exceed retryTimes and retryDuration
		return (s.retryTimes == -1 || retry < s.retryTimes) && time.Since(startTime) < s.retryDuration
	}
	for retry, startTime := 0, time.Now(); needRetry(retry, startTime); retry++ {
		select {
		case <-ctx.Done():
			return
		case <-ar.Pause:
			return
		case <-ar.Cancel:
			return
		default:
		}

		start := time.Now()
		err = fn()
		v2.CdcSendSqlDurationHistogram.Observe(time.Since(start).Seconds())

		// return if success
		if err == nil {
			return
		}

		logutil.Errorf("cdc mysqlSink retry failed, err: %v", err)
		v2.CdcMysqlSinkErrorCounter.Inc()
		time.Sleep(time.Second)
	}
	return moerr.NewInternalError(ctx, "cdc mysqlSink retry exceed retryTimes or retryDuration")
}

//type matrixoneSink struct {
//}
//
//func (*matrixoneSink) Send(ctx context.Context, data *DecoderOutput) error {
//	return nil
//}

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
