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
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	// DefaultMaxAllowedPacket of mysql is 64 MB
	DefaultMaxAllowedPacket uint64 = 64 * 1024 * 1024
	// SqlBufReserved leave some space of sqlBuf for mysql connector or other usage
	SqlBufReserved       = 128
	DefaultRetryTimes    = -1
	DefaultRetryDuration = 30 * time.Minute

	sqlPrintLen = 200
)

func NewSinker(
	sinkUri UriInfo,
	dbTblInfo *DbTableInfo,
	watermarkUpdater *WatermarkUpdater,
	tableDef *plan.TableDef,
	retryTimes int,
	retryDuration time.Duration,
	ar *ActiveRoutine,
) (Sinker, error) {
	//TODO: remove console
	if sinkUri.SinkTyp == ConsoleSink {
		return NewConsoleSinker(dbTblInfo, watermarkUpdater), nil
	}

	sink, err := NewMysqlSink(sinkUri.User, sinkUri.Password, sinkUri.Ip, sinkUri.Port, retryTimes, retryDuration)
	if err != nil {
		return nil, err
	}

	return NewMysqlSinker(sink, dbTblInfo, watermarkUpdater, tableDef, ar), nil
}

var _ Sinker = new(consoleSinker)

type consoleSinker struct {
	dbTblInfo        *DbTableInfo
	watermarkUpdater *WatermarkUpdater
}

func NewConsoleSinker(
	dbTblInfo *DbTableInfo,
	watermarkUpdater *WatermarkUpdater,
) Sinker {
	return &consoleSinker{
		dbTblInfo:        dbTblInfo,
		watermarkUpdater: watermarkUpdater,
	}
}

func (s *consoleSinker) Sink(ctx context.Context, data *DecoderOutput) error {
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

	return nil
}

func (s *consoleSinker) SendBegin(_ context.Context) error {
	return nil
}

func (s *consoleSinker) SendCommit(_ context.Context) error {
	return nil
}

func (s *consoleSinker) SendRollback(_ context.Context) error {
	return nil
}

func (s *consoleSinker) Close() {}

var _ Sinker = new(mysqlSinker)

type mysqlSinker struct {
	mysql            Sink
	dbTblInfo        *DbTableInfo
	watermarkUpdater *WatermarkUpdater
	ar               *ActiveRoutine

	// buffers, allocate only once
	maxAllowedPacket uint64
	// buf of sql statement
	sqlBuf []byte
	// buf of row data from batch, e.g. values part of insert statement (insert into xx values (a), (b), (c))
	// or where ... in part of delete statement (delete from xx where pk in ((a), (b), (c)))
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
}

var NewMysqlSinker = func(
	mysql Sink,
	dbTblInfo *DbTableInfo,
	watermarkUpdater *WatermarkUpdater,
	tableDef *plan.TableDef,
	ar *ActiveRoutine,
) Sinker {
	s := &mysqlSinker{
		mysql:            mysql,
		dbTblInfo:        dbTblInfo,
		watermarkUpdater: watermarkUpdater,
		maxAllowedPacket: DefaultMaxAllowedPacket,
		ar:               ar,
	}
	_ = mysql.(*mysqlSink).conn.QueryRow("SELECT @@max_allowed_packet").Scan(&s.maxAllowedPacket)

	// buf
	s.sqlBuf = make([]byte, 0, s.maxAllowedPacket)
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
	s.preRowType = NoOp
	return s
}

func (s *mysqlSinker) Sink(ctx context.Context, data *DecoderOutput) (err error) {
	watermark := s.watermarkUpdater.GetFromMem(s.dbTblInfo.SourceTblIdStr)
	if data.toTs.LE(&watermark) {
		logutil.Errorf("^^^^^ Sinker: unexpected watermark: %s, current watermark: %s",
			data.toTs.ToString(), watermark.ToString())
		return
	}

	if data.noMoreData {
		// output the left sql
		if s.preRowType == InsertRow && len(s.sqlBuf) != len(s.tsInsertPrefix) {
			if err = s.mysql.Send(ctx, s.ar, string(s.sqlBuf)); err != nil {
				return
			}
		} else if s.preRowType == DeleteRow && len(s.sqlBuf) != len(s.tsDeletePrefix) {
			s.sqlBuf = appendByte(s.sqlBuf, ')')
			if err = s.mysql.Send(ctx, s.ar, string(s.sqlBuf)); err != nil {
				return
			}
		}
		// reset status
		s.sqlBuf = s.sqlBuf[:0]
		s.preRowType = NoOp

		s.watermarkUpdater.UpdateMem(s.dbTblInfo.SourceTblIdStr, data.toTs)
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
		return s.sinkSnapshot(ctx, data.checkpointBat)
	} else if data.outputTyp == OutputTypeTail {
		return s.sinkTail(ctx, data.insertAtmBatch, data.deleteAtmBatch)
	}
	return
}

func (s *mysqlSinker) SendBegin(ctx context.Context) error {
	return s.mysql.SendBegin(ctx)
}

func (s *mysqlSinker) SendCommit(ctx context.Context) error {
	return s.mysql.SendCommit(ctx)
}

func (s *mysqlSinker) SendRollback(ctx context.Context) error {
	return s.mysql.SendRollback(ctx)
}

func (s *mysqlSinker) sinkSnapshot(ctx context.Context, bat *batch.Batch) (err error) {
	// if last row is not insert row, means this is the first snapshot batch
	if s.preRowType != InsertRow {
		s.sqlBuf = append(s.sqlBuf[:0], s.tsInsertPrefix...)
		s.preRowType = InsertRow
	}

	for i := 0; i < batchRowCount(bat); i++ {
		// step1: get row from the batch
		if err = extractRowFromEveryVector(ctx, bat, i, s.insertRow); err != nil {
			return
		}

		// step2: transform rows into sql parts
		if err = s.getInsertRowBuf(ctx); err != nil {
			return
		}

		// step3: append to sqlBuf, send sql if sqlBuf is full
		if err = s.appendSqlBuf(ctx, InsertRow); err != nil {
			return
		}
	}
	return
}

// insertBatch and deleteBatch is sorted by ts
// for the same ts, delete first, then insert
func (s *mysqlSinker) sinkTail(ctx context.Context, insertBatch, deleteBatch *AtomicBatch) (err error) {
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
				return
			}
			// get next item
			insertIterHasNext = insertIter.Next()
		} else {
			if err = s.sinkDelete(ctx, deleteIter); err != nil {
				return
			}
			// get next item
			deleteIterHasNext = deleteIter.Next()
		}
	}

	// output the rest of insert iterator
	for insertIterHasNext {
		if err = s.sinkInsert(ctx, insertIter); err != nil {
			return
		}
		// get next item
		insertIterHasNext = insertIter.Next()
	}

	// output the rest of delete iterator
	for deleteIterHasNext {
		if err = s.sinkDelete(ctx, deleteIter); err != nil {
			return
		}
		// get next item
		deleteIterHasNext = deleteIter.Next()
	}

	// output the last sql
	if s.preRowType == InsertRow && len(s.sqlBuf) != len(s.tsInsertPrefix) {
		if err = s.mysql.Send(ctx, s.ar, string(s.sqlBuf)); err != nil {
			return
		}
	} else if s.preRowType == DeleteRow && len(s.sqlBuf) != len(s.tsDeletePrefix) {
		s.sqlBuf = appendByte(s.sqlBuf, ')')
		if err = s.mysql.Send(ctx, s.ar, string(s.sqlBuf)); err != nil {
			return
		}
	}

	// reset status
	s.sqlBuf = s.sqlBuf[:0]
	s.preRowType = NoOp
	return
}

func (s *mysqlSinker) sinkInsert(ctx context.Context, insertIter *atomicBatchRowIter) (err error) {
	// if last row is not insert row, need output sql first
	if s.preRowType != InsertRow {
		if s.preRowType == DeleteRow && len(s.sqlBuf) != len(s.tsDeletePrefix) {
			s.sqlBuf = appendByte(s.sqlBuf, ')')
			if err = s.mysql.Send(ctx, s.ar, string(s.sqlBuf)); err != nil {
				return
			}
		}
		s.sqlBuf = append(s.sqlBuf[:0], s.tsInsertPrefix...)
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
	if err = s.appendSqlBuf(ctx, InsertRow); err != nil {
		return
	}

	return
}

func (s *mysqlSinker) sinkDelete(ctx context.Context, deleteIter *atomicBatchRowIter) (err error) {
	// if last row is not delete row, need output sql first
	if s.preRowType != DeleteRow {
		if s.preRowType == InsertRow && len(s.sqlBuf) != len(s.tsInsertPrefix) {
			if err = s.mysql.Send(ctx, s.ar, string(s.sqlBuf)); err != nil {
				return
			}
		}
		s.sqlBuf = append(s.sqlBuf[:0], s.tsDeletePrefix...)
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
	if err = s.appendSqlBuf(ctx, DeleteRow); err != nil {
		return
	}

	return
}

// appendSqlBuf appends rowBuf to sqlBuf if not exceed its cap
// otherwise, send sql to downstream first, then reset sqlBuf and append
func (s *mysqlSinker) appendSqlBuf(ctx context.Context, rowType RowType) (err error) {
	prefixLen := len(s.tsInsertPrefix)
	if rowType == DeleteRow {
		prefixLen = len(s.tsDeletePrefix)
	}

	if len(s.sqlBuf)+len(s.rowBuf)+SqlBufReserved > cap(s.sqlBuf) {
		if rowType == DeleteRow {
			s.sqlBuf = appendByte(s.sqlBuf, ')')
		}

		// if s.sqlBuf has no enough space, send it to downstream
		if err = s.mysql.Send(ctx, s.ar, string(s.sqlBuf)); err != nil {
			return
		}

		// reset s.sqlBuf
		s.sqlBuf = s.sqlBuf[:prefixLen]
	}

	// append bytes
	if len(s.sqlBuf) != prefixLen {
		s.sqlBuf = appendByte(s.sqlBuf, ',')
	}
	s.sqlBuf = append(s.sqlBuf, s.rowBuf...)
	return
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

func (s *mysqlSinker) Close() {
	s.mysql.Close()
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

type mysqlSink struct {
	conn           *sql.DB
	tx             *sql.Tx
	user, password string
	ip             string
	port           int

	retryTimes    int
	retryDuration time.Duration
}

var NewMysqlSink = func(
	user, password string,
	ip string, port int,
	retryTimes int,
	retryDuration time.Duration,
) (Sink, error) {
	ret := &mysqlSink{
		user:          user,
		password:      password,
		ip:            ip,
		port:          port,
		retryTimes:    retryTimes,
		retryDuration: retryDuration,
	}
	err := ret.connect()
	return ret, err
}

func (s *mysqlSink) connect() (err error) {
	s.conn, err = openDbConn(s.user, s.password, s.ip, s.port)
	return err
}

func (s *mysqlSink) Send(ctx context.Context, ar *ActiveRoutine, sql string) (err error) {
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
		if s.tx != nil {
			_, err = s.tx.Exec(sql)
		} else {
			_, err = s.conn.Exec(sql)
		}
		v2.CdcSendSqlDurationHistogram.Observe(time.Since(start).Seconds())
		// return if success
		if err == nil {
			//logutil.Errorf("----mysql send sql----, success, sql: %s", sql)
			return
		}

		logutil.Errorf("----mysql send sql----, failed, err: %v, sql: %s", err, sql[:min(len(sql), sqlPrintLen)])
		v2.CdcMysqlSinkErrorCounter.Inc()
		time.Sleep(time.Second)
	}
	return moerr.NewInternalError(ctx, "mysql sink retry exceed retryTimes or retryDuration")
}

func (s *mysqlSink) SendBegin(ctx context.Context) (err error) {
	s.tx, err = s.conn.BeginTx(ctx, nil)
	return
}

func (s *mysqlSink) SendCommit(_ context.Context) (err error) {
	defer func() {
		s.tx = nil
	}()

	return s.tx.Commit()
}

func (s *mysqlSink) SendRollback(_ context.Context) (err error) {
	defer func() {
		s.tx = nil
	}()

	return s.tx.Rollback()
}

func (s *mysqlSink) Close() {
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
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
