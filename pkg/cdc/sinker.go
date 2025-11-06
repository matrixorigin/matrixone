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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/mysql"
	"go.uber.org/zap"
)

const (
	// sqlBufReserved leave 5 bytes for mysql driver (used by mysqlSink)
	sqlBufReserved = 5
	sqlPrintLen    = 200
	fakeSql        = "fakeSql"
)

var NewSinker = func(
	sinkUri UriInfo,
	accountId uint64,
	taskId string,
	dbTblInfo *DbTableInfo,
	watermarkUpdater *CDCWatermarkUpdater,
	tableDef *plan.TableDef,
	retryTimes int,
	retryDuration time.Duration,
	ar *ActiveRoutine,
	maxSqlLength uint64,
	sendSqlTimeout string,
) (Sinker, error) {
	//TODO: remove console
	if sinkUri.SinkTyp == CDCSinkType_Console {
		return NewConsoleSinker(dbTblInfo, watermarkUpdater), nil
	}

	// Use the new v2 architecture
	return CreateMysqlSinker2(
		sinkUri,
		accountId,
		taskId,
		dbTblInfo,
		watermarkUpdater,
		tableDef,
		retryTimes,
		retryDuration,
		ar,
		maxSqlLength,
		sendSqlTimeout,
	)
}

var _ Sinker = new(consoleSinker)

type consoleSinker struct {
	dbTblInfo        *DbTableInfo
	watermarkUpdater *CDCWatermarkUpdater
}

func NewConsoleSinker(
	dbTblInfo *DbTableInfo,
	watermarkUpdater *CDCWatermarkUpdater,
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
			logutil.Info("checkpoint")
			//logutil.Info(data.checkpointBat.String())
		}
	case OutputTypeTail:
		if data.insertAtmBatch != nil && data.insertAtmBatch.Rows.Len() > 0 {
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

func (s *consoleSinker) ClearError() {}

func (s *consoleSinker) Reset() {}

func (s *consoleSinker) Close() {}

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

	debugTxnRecorder struct {
		doRecord bool
		txnSQL   []string
		sqlBytes int
	}
}

var NewMysqlSink = func(
	user, password string,
	ip string, port int,
	retryTimes int,
	retryDuration time.Duration,
	timeout string,
	doRecord bool,
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

	ret.debugTxnRecorder.doRecord = doRecord

	err := ret.connect()
	return ret, err
}

func (s *mysqlSink) Reset() {
	s.tx = nil
}

func (s *mysqlSink) recordTxnSQL(sqlBuf []byte) {
	if !s.debugTxnRecorder.doRecord {
		return
	}

	s.debugTxnRecorder.sqlBytes += len(sqlBuf)
	s.debugTxnRecorder.txnSQL = append(
		s.debugTxnRecorder.txnSQL, string(sqlBuf[sqlBufReserved:]))
}

func (s *mysqlSink) infoRecordedTxnSQLs(err error) {
	if !s.debugTxnRecorder.doRecord {
		return
	}

	if len(s.debugTxnRecorder.txnSQL) == 0 {
		return
	}

	if s.debugTxnRecorder.sqlBytes <= mpool.MB {
		buf := bytes.Buffer{}
		for _, sqlStr := range s.debugTxnRecorder.txnSQL {
			buf.WriteString(sqlStr)
			buf.WriteString("; ")
		}

		logutil.Info("CDC-RECORDED-TXN",
			zap.Error(err),
			zap.String("details", buf.String()))
	}

	s.resetRecordedTxn()
}

func (s *mysqlSink) resetRecordedTxn() {
	if !s.debugTxnRecorder.doRecord {
		return
	}
	s.debugTxnRecorder.sqlBytes = 0
	s.debugTxnRecorder.txnSQL = s.debugTxnRecorder.txnSQL[:0]
}

// Send must leave 5 bytes at the head of sqlBuf
func (s *mysqlSink) Send(ctx context.Context, ar *ActiveRoutine, sqlBuf []byte, needRetry bool) error {
	reuseQueryArg := sql.NamedArg{
		Name:  mysql.ReuseQueryBuf,
		Value: sqlBuf,
	}

	s.recordTxnSQL(sqlBuf)

	f := func() (err error) {
		if s.tx != nil {
			_, err = s.tx.Exec(fakeSql, reuseQueryArg)
		} else {
			_, err = s.conn.Exec(fakeSql, reuseQueryArg)
		}

		if err != nil {

			s.infoRecordedTxnSQLs(err)

			logutil.Errorf("CDC-MySQLSink Send failed, err: %v, sql: %s", err, sqlBuf[sqlBufReserved:min(len(sqlBuf), sqlPrintLen)])
			//logutil.Errorf("cdc mysqlSink Send failed, err: %v, sql: %s", err, sqlBuf[sqlBufReserved:])
		}
		//logutil.Infof("cdc mysqlSink Send success, sql: %s", sqlBuf[sqlBufReserved:])
		return
	}

	if !needRetry {
		return f()
	}
	return retry(ctx, ar, f, s.retryTimes, s.retryDuration)
}

func (s *mysqlSink) SendBegin(ctx context.Context) (err error) {
	s.resetRecordedTxn()
	s.tx, err = s.conn.BeginTx(ctx, nil)
	return err
}

func (s *mysqlSink) SendCommit(_ context.Context) error {
	s.resetRecordedTxn()
	return s.tx.Commit()
}

func (s *mysqlSink) SendRollback(_ context.Context) error {
	s.resetRecordedTxn()
	return s.tx.Rollback()
}

func (s *mysqlSink) Close() {
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}

	s.debugTxnRecorder.txnSQL = nil
}

func (s *mysqlSink) connect() (err error) {
	s.conn, err = OpenDbConn(s.user, s.password, s.ip, s.port, s.timeout)
	return err
}

func retry(ctx context.Context, ar *ActiveRoutine, fn func() error, retryTimes int, retryDuration time.Duration) (err error) {
	needRetry := func(retry int, startTime time.Time) bool {
		// retryTimes == -1 means retry forever
		// do not exceed retryTimes and retryDuration
		return (retryTimes == -1 || retry < retryTimes) && time.Since(startTime) < retryDuration
	}
	dur := time.Second
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

		logutil.Errorf("CDC-MySQLSink retry failed, err: %v", err)
		v2.CdcMysqlSinkErrorCounter.Inc()
		time.Sleep(dur)
		dur *= 2
	}
	return moerr.NewInternalError(ctx, "CDC-MySQLSink retry exceed retryTimes or retryDuration")
}

//type matrixoneSink struct {
//}
//
//func (*matrixoneSink) Send(ctx context.Context, data *DecoderOutput) error {
//	return nil
//}
