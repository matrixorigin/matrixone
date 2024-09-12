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
)

const (
	// DefaultMaxAllowedPacket of mysql is 64 MB
	DefaultMaxAllowedPacket uint64 = 64 * 1024 * 1024
	DefaultRetryTimes              = -1
	DefaultRetryDuration           = 30 * time.Minute
)

func NewSinker(
	sinkUri UriInfo,
	dbTblInfo *DbTableInfo,
	watermarkUpdater *WatermarkUpdater,
	tableDef *plan.TableDef,
	retryTimes int,
	retryDuration time.Duration,
) (Sinker, error) {
	//TODO: remove console
	if sinkUri.SinkTyp == ConsoleSink {
		return NewConsoleSinker(dbTblInfo, watermarkUpdater), nil
	}

	sink, err := NewMysqlSink(sinkUri.User, sinkUri.Password, sinkUri.Ip, sinkUri.Port, retryTimes, retryDuration)
	if err != nil {
		return nil, err
	}

	return NewMysqlSinker(sink, dbTblInfo, watermarkUpdater, tableDef), nil
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
	case OutputTypeCheckpoint:
		if data.checkpointBat != nil && data.checkpointBat.RowCount() > 0 {
			//FIXME: only test here
			logutil.Info("checkpoint")
			logutil.Info(data.checkpointBat.String())
		}
	case OutputTypeTailDone:
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
		}
	case OutputTypeUnfinishedTailWIP:
		logutil.Info("====tail wip====")
	}

	return nil
}

var _ Sinker = new(mysqlSinker)

type mysqlSinker struct {
	mysql            Sink
	dbTblInfo        *DbTableInfo
	watermarkUpdater *WatermarkUpdater

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

	// iterators
	insertIters []RowIterator
	deleteIters []RowIterator

	// only contains user defined column types, no mo meta cols
	insertTypes []*types.Type
	// only contains pk columns
	deleteTypes []*types.Type

	// for collect row data, allocate only once
	insertRow []any
	deleteRow []any

	preRowType RowType
}

var NewMysqlSinker = func(
	mysql Sink,
	dbTblInfo *DbTableInfo,
	watermarkUpdater *WatermarkUpdater,
	tableDef *plan.TableDef,
) Sinker {
	s := &mysqlSinker{
		mysql:            mysql,
		dbTblInfo:        dbTblInfo,
		watermarkUpdater: watermarkUpdater,
		maxAllowedPacket: DefaultMaxAllowedPacket,
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
	watermark := s.watermarkUpdater.GetFromMem(s.dbTblInfo.SourceTblId)
	if data.toTs.LessEq(&watermark) {
		logutil.Errorf("^^^^^ Sinker: unexpected watermark: %s, current watermark: %s",
			data.toTs.ToString(), watermark.ToString())
		return
	}

	//_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: [%s, %s) ckpBatSize() = %d, insertBatSize() = %d, deleteBatSize() = %d\n",
	//	data.fromTs.ToString(), data.toTs.ToString(), data.ckpBatSize(), data.insertBatSize(), data.deleteBatSize())

	tsPrefix := fmt.Sprintf("/* [%s, %s) */", data.fromTs.ToString(), data.toTs.ToString())
	s.tsInsertPrefix = s.tsInsertPrefix[:0]
	s.tsInsertPrefix = append(s.tsInsertPrefix, []byte(tsPrefix)...)
	s.tsInsertPrefix = append(s.tsInsertPrefix, s.insertPrefix...)
	s.tsDeletePrefix = s.tsDeletePrefix[:0]
	s.tsDeletePrefix = append(s.tsDeletePrefix, []byte(tsPrefix)...)
	s.tsDeletePrefix = append(s.tsDeletePrefix, s.deletePrefix...)

	if data.noMoreData {
		if err = s.sinkRemain(ctx); err != nil {
			return
		}
		s.watermarkUpdater.UpdateMem(s.dbTblInfo.SourceTblId, data.toTs)
		return
	}

	if data.outputTyp == OutputTypeCheckpoint {
		return s.sinkCkp(ctx, data.checkpointBat)
	} else if data.outputTyp == OutputTypeTailDone {
		return s.sinkTail(ctx, data.insertAtmBatch, data.deleteAtmBatch)
	}
	return
}

func (s *mysqlSinker) sinkCkp(ctx context.Context, bat *batch.Batch) (err error) {
	s.sqlBuf = append(s.sqlBuf[:0], s.tsInsertPrefix...)

	for i := 0; i < bat.RowCount(); i++ {
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
	if len(s.sqlBuf) != len(s.tsInsertPrefix) {
		if err = s.mysql.Send(ctx, string(s.sqlBuf)); err != nil {
			return
		}
	}

	s.sqlBuf = s.sqlBuf[:0]
	return
}

func (s *mysqlSinker) sinkTail(ctx context.Context, insertBatch, deleteBatch *AtomicBatch) (err error) {
	// merge-sort like process:
	//
	//                   tail                   head
	// insertIter queue: ins_itn ... ins_it2, ins_it1  \
	//                                                  compare and output the less one
	// deleteIter queue: del_itm ... del_it2, del_it1  /
	// do until one iterator reach the end, then remove this iterator and get the next one from the queue
	//
	//
	// do until one queue is empty, remain the non-empty queue and remember cursor location of the first iterator:
	// insertIter queue:        (empty)       \
	//                                         compare and output the less one
	// deleteIter queue: del_itm ... del_itx  /
	//                                  ^
	//                                  |  cursor in iterator
	//
	//
	// when receive new tail data, push new iters:
	// insertIter queue: ins_itN ... ins_it(n+2), ins_it(n+1)           \
	//                                                                   compare and output the less one
	// deleteIter queue: del_itM ... del_it(m+1), del_itm ... del_itx  /
	//                                                            ^
	//                                                            |  cursor in iterator
	// continue with the remembered cursor location

	if insertBatch != nil {
		s.insertIters = append(s.insertIters, insertBatch.GetRowIterator())
	}
	if deleteBatch != nil {
		s.deleteIters = append(s.deleteIters, deleteBatch.GetRowIterator())
	}

	// do until one queue is empty
	for len(s.insertIters) > 0 && len(s.deleteIters) > 0 {
		// pick the head ones from two queues
		insertIter, deleteIter := s.insertIters[0].(*atomicBatchRowIter), s.deleteIters[0].(*atomicBatchRowIter)

		// output sql until one iterator reach the end
		insertIterHasNext, deleteIterHasNext := insertIter.Next(), deleteIter.Next()
		for insertIterHasNext && deleteIterHasNext {
			insertItem, deleteItem := insertIter.Item(), deleteIter.Item()
			// compare the smallest item of insert and delete tree
			if insertItem.Less(deleteItem) {
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

		// iterator reach the end, remove it
		if !insertIterHasNext {
			s.insertIters = s.insertIters[1:]
		} else {
			// did an extra Next() before, need to move back here
			if !insertIter.Prev() {
				insertIter.Reset()
			}
		}
		if !deleteIterHasNext {
			s.deleteIters = s.deleteIters[1:]
		} else {
			if !deleteIter.Prev() {
				deleteIter.Reset()
			}
		}
	}
	return
}

func (s *mysqlSinker) sinkInsert(ctx context.Context, insertIter *atomicBatchRowIter) (err error) {
	// if last row is not insert row, need output sql first
	if s.preRowType != InsertRow {
		if s.preRowType == DeleteRow && len(s.sqlBuf) != len(s.tsDeletePrefix) {
			s.sqlBuf = appendByte(s.sqlBuf, ')')
			if err = s.mysql.Send(ctx, string(s.sqlBuf)); err != nil {
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

func (s *mysqlSinker) sinkDelete(ctx context.Context, insertIter *atomicBatchRowIter) (err error) {
	// if last row is not delete row, need output sql first
	if s.preRowType != DeleteRow {
		if s.preRowType == InsertRow && len(s.sqlBuf) != len(s.tsInsertPrefix) {
			if err = s.mysql.Send(ctx, string(s.sqlBuf)); err != nil {
				return
			}
		}
		s.sqlBuf = append(s.sqlBuf[:0], s.tsDeletePrefix...)
		s.preRowType = DeleteRow
	}

	// step1: get row from the batch
	if err = insertIter.Row(ctx, s.deleteRow); err != nil {
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

// sinkRemain sinks the remain rows in insertBatch and deleteBatch when receive NoMoreData signal
func (s *mysqlSinker) sinkRemain(ctx context.Context) (err error) {
	// if queue is not empty
	for len(s.insertIters) > 0 {
		insertIter := s.insertIters[0].(*atomicBatchRowIter)

		// output sql until one iterator reach the end
		insertIterHasNext := insertIter.Next()
		for insertIterHasNext {
			if err = s.sinkInsert(ctx, insertIter); err != nil {
				return
			}
			// get next item
			insertIterHasNext = insertIter.Next()
		}

		// iterator reach the end, remove it
		s.insertIters = s.insertIters[1:]
	}

	// if queue is not empty
	for len(s.deleteIters) > 0 {
		deleteIter := s.deleteIters[0].(*atomicBatchRowIter)

		// output sql until one iterator reach the end
		deleteIterHasNext := deleteIter.Next()
		for deleteIterHasNext {
			if err = s.sinkDelete(ctx, deleteIter); err != nil {
				return
			}
			// get next item
			deleteIterHasNext = deleteIter.Next()
		}

		// iterator reach the end, remove it
		s.deleteIters = s.deleteIters[1:]
	}

	if s.preRowType == InsertRow && len(s.sqlBuf) != len(s.tsInsertPrefix) {
		if err = s.mysql.Send(ctx, string(s.sqlBuf)); err != nil {
			return
		}
	} else if s.preRowType == DeleteRow && len(s.sqlBuf) != len(s.tsDeletePrefix) {
		s.sqlBuf = appendByte(s.sqlBuf, ')')
		if err = s.mysql.Send(ctx, string(s.sqlBuf)); err != nil {
			return
		}
	}

	// reset status
	s.sqlBuf = s.sqlBuf[:0]
	s.preRowType = NoOp
	return
}

// appendSqlBuf appends rowBuf to sqlBuf if not exceed its cap
// otherwise, send sql to downstream first, then reset sqlBuf and append
func (s *mysqlSinker) appendSqlBuf(ctx context.Context, rowType RowType) (err error) {
	prefixLen := len(s.tsInsertPrefix)
	if rowType == DeleteRow {
		prefixLen = len(s.tsDeletePrefix)
	}

	// insert comma if not the first item
	commaLen := 0
	if len(s.sqlBuf) != prefixLen {
		commaLen = 1
	}

	// deleteType need an additional right parenthesis
	parLen := 0
	if rowType == DeleteRow {
		parLen = 1
	}

	// when len(sql) == max_allowed_packet, mysql will return error, so add equal here
	if len(s.sqlBuf)+commaLen+len(s.rowBuf)+parLen >= cap(s.sqlBuf) {
		if rowType == DeleteRow {
			s.sqlBuf = appendByte(s.sqlBuf, ')')
		}

		// if s.sqlBuf has no enough space, send it to downstream
		if err = s.mysql.Send(ctx, string(s.sqlBuf)); err != nil {
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

type mysqlSink struct {
	conn           *sql.DB
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

func (s *mysqlSink) Send(ctx context.Context, sql string) (err error) {
	needRetry := func(retry int, startTime time.Time) bool {
		// retryTimes == -1 means retry forever
		// do not exceed retryTimes and retryDuration
		return (s.retryTimes == -1 || retry < s.retryTimes) && time.Since(startTime) < s.retryDuration
	}
	for retry, startTime := 0, time.Now(); needRetry(retry, startTime); retry++ {
		//fmt.Fprintf(os.Stderr, "----mysql send sql----, len:%d, sql:%s\n", len(sql), sql[:min(200, len(sql))])
		// return if success
		if _, err = s.conn.Exec(sql); err == nil {
			logutil.Errorf("----mysql send sql----, success")
			return
		}
		//fmt.Fprintf(os.Stderr, "----mysql send sql----, failed, err = %v\n", err)
		time.Sleep(time.Second)
	}
	return moerr.NewInternalError(ctx, "mysql sink retry exceed retryTimes or retryDuration")
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
