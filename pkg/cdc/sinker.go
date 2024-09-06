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
	"os"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/tools"
)

const (
	// DefaultMaxAllowedPacket of mysql is 64 MB
	DefaultMaxAllowedPacket uint64 = 64 * 1024 * 1024
)

func NewSinker(
	ctx context.Context,
	sinkUri UriInfo,
	inputCh chan tools.Pair[*TableCtx, *DecoderOutput],
	dbTblInfo *DbTableInfo,
	watermarkUpdater *WatermarkUpdater,
	tableDef *plan.TableDef,
) (Sinker, error) {
	//TODO: remove console
	if sinkUri.SinkTyp == "console" {
		return NewConsoleSinker(inputCh, dbTblInfo, watermarkUpdater), nil
	}

	sink, err := NewMysqlSink(sinkUri.User, sinkUri.Password, sinkUri.Ip, sinkUri.Port)
	if err != nil {
		return nil, err
	}

	return NewMysqlSinker(sink, inputCh, dbTblInfo, watermarkUpdater, tableDef), nil
}

var _ Sinker = new(consoleSinker)

type consoleSinker struct {
	inputCh          chan tools.Pair[*TableCtx, *DecoderOutput]
	dbTblInfo        *DbTableInfo
	watermarkUpdater *WatermarkUpdater
}

func NewConsoleSinker(
	inputCh chan tools.Pair[*TableCtx, *DecoderOutput],
	dbTblInfo *DbTableInfo,
	watermarkUpdater *WatermarkUpdater,
) Sinker {
	return &consoleSinker{
		inputCh:          inputCh,
		dbTblInfo:        dbTblInfo,
		watermarkUpdater: watermarkUpdater,
	}
}

func (s *consoleSinker) Sink(ctx context.Context, data *DecoderOutput) error {
	fmt.Fprintln(os.Stderr, "====console sinker====")

	fmt.Fprintln(os.Stderr, "output type", data.outputTyp)
	switch data.outputTyp {
	case OutputTypeCheckpoint:
		if data.checkpointBat != nil && data.checkpointBat.RowCount() > 0 {
			//FIXME: only test here
			fmt.Fprintln(os.Stderr, "checkpoint")
			fmt.Fprintln(os.Stderr, data.checkpointBat.String())
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
				fmt.Fprintln(os.Stderr, "insert", row)
			}
		}
	case OutputTypeUnfinishedTailWIP:
		fmt.Fprintln(os.Stderr, "====tail wip====")
	}

	return nil
}

func (s *consoleSinker) Run(ctx context.Context, ar *ActiveRoutine) {
	for {
		select {
		case <-ar.Cancel:
			return

		case entry := <-s.inputCh:
			tableCtx := entry.Key
			decodeOutput := entry.Value
			_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: [%v(%v)].[%v(%v)]\n",
				tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())

			if decodeOutput.noMoreData {
				s.watermarkUpdater.UpdateTableWatermark(tableCtx.TableId(), decodeOutput.toTs)
				continue
			}

			err := s.Sink(ctx, decodeOutput)
			if err != nil {
				return
			}

			if decodeOutput.insertAtmBatch != nil {
				s.watermarkUpdater.UpdateTableWatermark(tableCtx.TableId(), decodeOutput.insertAtmBatch.To)
			} else if decodeOutput.deleteAtmBatch != nil {
				s.watermarkUpdater.UpdateTableWatermark(tableCtx.TableId(), decodeOutput.deleteAtmBatch.To)
			}
		}
	}
}

var _ Sinker = new(mysqlSinker)

type mysqlSinker struct {
	mysql            Sink
	inputCh          chan tools.Pair[*TableCtx, *DecoderOutput]
	dbTblInfo        *DbTableInfo
	watermarkUpdater *WatermarkUpdater

	// buffers, allocate only once
	maxAllowedPacket uint64
	// buf of sql statement
	sqlBuf []byte
	// buf of row data from batch, e.g. values part of insert statement (insert into xx values (a), (b), (c))
	// or where ... in part of delete statement (delete from xx where pk in ((a), (b), (c)))
	rowBuf       []byte
	insertPrefix string
	deletePrefix string

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

func NewMysqlSinker(
	mysql Sink,
	inputCh chan tools.Pair[*TableCtx, *DecoderOutput],
	dbTblInfo *DbTableInfo,
	watermarkUpdater *WatermarkUpdater,
	tableDef *plan.TableDef,
) Sinker {
	s := &mysqlSinker{
		mysql:            mysql,
		inputCh:          inputCh,
		dbTblInfo:        dbTblInfo,
		watermarkUpdater: watermarkUpdater,
		// TODO: get precise value from downstream
		maxAllowedPacket: DefaultMaxAllowedPacket,
	}

	// buf
	s.sqlBuf = make([]byte, 0, s.maxAllowedPacket)
	s.rowBuf = make([]byte, 0, 1024)

	// prefix
	s.insertPrefix = fmt.Sprintf("REPLACE INTO `%s`.`%s` VALUES ", s.dbTblInfo.SinkDbName, s.dbTblInfo.SinkTblName)
	s.deletePrefix = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s IN (", s.dbTblInfo.SinkDbName, s.dbTblInfo.SinkTblName, genPrimaryKeyStr(tableDef))

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
	s.preRowType = Invalid
	return s
}

func (s *mysqlSinker) Sink(ctx context.Context, data *DecoderOutput) (err error) {
	if data.outputTyp == OutputTypeCheckpoint {
		return s.sinkCkp(ctx, data.checkpointBat)
	} else if data.outputTyp == OutputTypeTailDone {
		return s.sinkTail(ctx, data.insertAtmBatch, data.deleteAtmBatch)
	} else {
		// wip
		s.insertIters = append(s.insertIters, data.insertAtmBatch.GetRowIterator())
		s.deleteIters = append(s.deleteIters, data.deleteAtmBatch.GetRowIterator())
	}
	return
}

func (s *mysqlSinker) Run(ctx context.Context, ar *ActiveRoutine) {
	_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: start\n")
	defer func() {
		s.mysql.Close()
		_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: end\n")
	}()

	for {
		select {
		case <-ar.Cancel:
			return

		case entry := <-s.inputCh:
			tableCtx := entry.Key
			decodeOutput := entry.Value

			//_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: [%v(%v)].[%v(%v)], to %v\n",
			//	tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId(), decodeOutput.toTs.ToString())

			watermark := s.watermarkUpdater.GetTableWatermark(tableCtx.TableId())
			if decodeOutput.toTs.LessEq(&watermark) {
				_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: unexpected watermark: %s, current watermark: %s\n",
					decodeOutput.toTs.ToString(), watermark.ToString())
			}

			if decodeOutput.noMoreData {
				// TODO temporary use here, for there's no TailDone part now
				if err := s.sinkTail(ctx, decodeOutput.insertAtmBatch, decodeOutput.deleteAtmBatch); err != nil {
					// TODO handle error
				}
				if err := s.sinkRemain(ctx); err != nil {
					// TODO handle error
				}
				s.watermarkUpdater.UpdateTableWatermark(tableCtx.TableId(), decodeOutput.toTs)
				continue
			}

			if err := s.Sink(ctx, decodeOutput); err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: [%v(%v)].[%v(%v)], sink error: %v\n",
					tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId(),
					err,
				)
				// TODO handle error
				continue
			}
			//_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: [%v(%v)].[%v(%v)], sink over\n",
			//	tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())
		}
	}
}

func (s *mysqlSinker) sinkCkp(ctx context.Context, bat *batch.Batch) (err error) {
	s.sqlBuf = append(s.sqlBuf[:0], s.insertPrefix...)

	for i := 0; i < bat.RowCount(); i++ {
		// step1: get row from the batch
		if err = extractRowFromEveryVector2(ctx, bat, i, s.insertRow); err != nil {
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
	if len(s.sqlBuf) != len(s.insertPrefix) {
		if err = s.mysql.Send(ctx, string(s.sqlBuf)); err != nil {
			return
		}
	}
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
	// when receive new data, push new iters:
	// insertIter queue: ins_itN ... ins_it(n+2), ins_it(n+1)           \
	//                                                                   compare and output the less one
	// deleteIter queue: del_itM ... del_it(m+1) , del_itm ... del_itx  /
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
		if s.preRowType == DeleteRow && len(s.sqlBuf) != len(s.deletePrefix) {
			s.sqlBuf = appendByte(s.sqlBuf, ')')
			if err = s.mysql.Send(ctx, string(s.sqlBuf)); err != nil {
				return
			}
		}
		s.sqlBuf = append(s.sqlBuf[:0], s.insertPrefix...)
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
		if s.preRowType == InsertRow && len(s.sqlBuf) != len(s.insertPrefix) {
			if err = s.mysql.Send(ctx, string(s.sqlBuf)); err != nil {
				return
			}
		}
		s.sqlBuf = append(s.sqlBuf[:0], s.deletePrefix...)
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

	if s.preRowType == InsertRow && len(s.sqlBuf) != len(s.insertPrefix) {
		if err = s.mysql.Send(ctx, string(s.sqlBuf)); err != nil {
			return
		}
	} else if s.preRowType == DeleteRow && len(s.sqlBuf) != len(s.deletePrefix) {
		s.sqlBuf = appendByte(s.sqlBuf, ')')
		if err = s.mysql.Send(ctx, string(s.sqlBuf)); err != nil {
			return
		}
	}

	// reset status
	s.sqlBuf = s.sqlBuf[:0]
	s.preRowType = Invalid
	return
}

// appendSqlBuf appends rowBuf to sqlBuf if not exceed its cap
// otherwise, send sql to downstream first, then reset sqlBuf and append
func (s *mysqlSinker) appendSqlBuf(ctx context.Context, rowType RowType) (err error) {
	prefixLen := len(s.insertPrefix)
	if rowType == DeleteRow {
		prefixLen = len(s.deletePrefix)
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
		if pkTuple, _, err = types.UnpackWithSchema(s.deleteRow[0].([]byte)); err != nil {
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
}

func NewMysqlSink(
	user, password string,
	ip string, port int) (Sink, error) {
	ret := &mysqlSink{
		user:     user,
		password: password,
		ip:       ip,
		port:     port,
	}
	err := ret.connect()
	return ret, err
}

func (s *mysqlSink) connect() (err error) {
	s.conn, err = openDbConn(s.user, s.password, s.ip, s.port)
	return err
}

func (s *mysqlSink) Send(ctx context.Context, sql string) (err error) {
	fmt.Fprintf(os.Stderr, "----mysql send sql----, len:%d, sql:%s\n", len(sql), sql[:min(200, len(sql))])
	_, err = s.conn.ExecContext(ctx, sql)
	fmt.Fprintf(os.Stderr, "----mysql send sql----, err = %v\n", err)
	return
}

func (s *mysqlSink) Close() {
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
}

type matrixoneSink struct {
}

func (*matrixoneSink) Send(ctx context.Context, data *DecoderOutput) error {
	return nil
}

func extractUriInfo(ctx context.Context, uri string) (user string, pwd string, ip string, port int, err error) {
	slashIdx := strings.Index(uri, "//")
	if slashIdx == -1 {
		return "", "", "", 0, moerr.NewInternalError(ctx, "invalid format of uri 1")
	}
	atIdx := strings.Index(uri[slashIdx+2:], "@")
	if atIdx == -1 {
		return "", "", "", 0, moerr.NewInternalError(ctx, "invalid format of uri 2")
	}
	userPwd := uri[slashIdx+2:][:atIdx]
	seps := strings.Split(userPwd, ":")
	if len(seps) != 2 {
		return "", "", "", 0, moerr.NewInternalError(ctx, "invalid format of uri 3")
	}
	user = seps[0]
	pwd = seps[1]
	ipPort := uri[slashIdx+2:][atIdx+1:]
	seps = strings.Split(ipPort, ":")
	if len(seps) != 2 {
		return "", "", "", 0, moerr.NewInternalError(ctx, "invalid format of uri 4")
	}
	ip = seps[0]
	portStr := seps[1]
	var portInt int64
	portInt, err = strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return "", "", "", 0, moerr.NewInternalErrorf(ctx, "invalid format of uri 5 %v", portStr)
	}
	if portInt < 0 || portInt > 65535 {
		return "", "", "", 0, moerr.NewInternalError(ctx, "invalid format of uri 6")
	}
	port = int(portInt)
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
