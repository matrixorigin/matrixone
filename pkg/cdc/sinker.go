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

package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/tools"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func NewSinker(
	ctx context.Context,
	sinkUri string,
	inputCh chan tools.Pair[*disttae.TableCtx, *DecoderOutput],
	curWaterMark timestamp.Timestamp,
	updateWatermarkFunc func(tableId uint64, watermark timestamp.Timestamp),
) (Sinker, error) {
	//TODO: remove console
	if strings.HasPrefix(strings.ToLower(sinkUri), "console://") {
		return NewConsoleSinker(inputCh), nil
	}

	//extract the info from the sink uri
	userName, pwd, ip, port, err := extractUriInfo(ctx, sinkUri)
	if err != nil {
		return nil, err
	}
	sink, err := NewMysqlSink(userName, pwd, ip, port)
	if err != nil {
		return nil, err
	}

	return NewMysqlSinker(sink, inputCh, curWaterMark, updateWatermarkFunc), nil
}

var _ Sinker = new(consoleSinker)

type consoleSinker struct {
	inputCh chan tools.Pair[*disttae.TableCtx, *DecoderOutput]
}

func NewConsoleSinker(inputCh chan tools.Pair[*disttae.TableCtx, *DecoderOutput]) Sinker {
	return &consoleSinker{
		inputCh: inputCh,
	}
}

func (s *consoleSinker) Sink(_ context.Context, data *DecoderOutput) error {
	fmt.Fprintln(os.Stderr, "====console sinker====")
	//fmt.Fprintln(os.Stderr, cdcCtx.Db(), cdcCtx.DBId(), cdcCtx.Table(), cdcCtx.TableId(), data.ts)
	if value, ok := data.sqlOfRows.Load().([][]byte); !ok {
		fmt.Fprintln(os.Stderr, "no sqlOfrows")
	} else {
		fmt.Fprintln(os.Stderr, "total rows sql", len(value))
		for i, sqlBytes := range value {
			plen := min(len(sqlBytes), 200)
			fmt.Fprintln(os.Stderr, i, string(sqlBytes[:plen]))
		}
	}

	if value, ok := data.sqlOfObjects.Load().([][]byte); !ok {
		fmt.Fprintln(os.Stderr, "no sqlOfObjects")
	} else {
		fmt.Fprintln(os.Stderr, "total objects sql", len(value))
		for i, sqlBytes := range value {
			plen := min(len(sqlBytes), 200)
			fmt.Fprintln(os.Stderr, i, string(sqlBytes[:plen]))
		}
	}

	if value, ok := data.sqlOfDeletes.Load().([][]byte); !ok {
		fmt.Fprintln(os.Stderr, "no sqlOfDeltas")
	} else {
		fmt.Fprintln(os.Stderr, "total deltas sql", len(value))
		for i, sqlBytes := range value {
			plen := min(len(sqlBytes), 200)
			fmt.Fprintln(os.Stderr, i, string(sqlBytes[:plen]))
		}
	}

	return nil
}

func (s *consoleSinker) Run(ctx context.Context, ar *ActiveRoutine) {
	for {
		select {
		case <-ar.Pause:
			return

		case <-ar.Cancel:
			return

		case entry := <-s.inputCh:
			tableCtx := entry.Key
			decodeOutput := entry.Value
			_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: {%s} [%v(%v)].[%v(%v)]\n",
				decodeOutput.ts.DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())

			err := s.Sink(ctx, decodeOutput)
			if err != nil {
				return
			}
		}
	}
}

var _ Sinker = new(mysqlSinker)

type mysqlSinker struct {
	mysql               Sink
	inputCh             chan tools.Pair[*disttae.TableCtx, *DecoderOutput]
	watermark           timestamp.Timestamp
	updateWatermarkFunc func(tableId uint64, watermark timestamp.Timestamp)
}

func NewMysqlSinker(
	mysql Sink,
	inputCh chan tools.Pair[*disttae.TableCtx, *DecoderOutput],
	watermark timestamp.Timestamp,
	updateWatermarkFunc func(tableId uint64, watermark timestamp.Timestamp),
) Sinker {
	return &mysqlSinker{
		mysql:               mysql,
		inputCh:             inputCh,
		watermark:           watermark,
		updateWatermarkFunc: updateWatermarkFunc,
	}
}

func (s *mysqlSinker) Sink(ctx context.Context, data *DecoderOutput) error {
	return s.mysql.Send(ctx, data)
}

func (s *mysqlSinker) Run(ctx context.Context, ar *ActiveRoutine) {
	_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: start\n")
	defer func() {
		s.mysql.Close()
		_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: end\n")
	}()

	for {
		select {
		case <-ar.Pause:
			return

		case <-ar.Cancel:
			return

		case entry := <-s.inputCh:
			tableCtx := entry.Key
			decodeOutput := entry.Value

			if s.watermark.GreaterEq(decodeOutput.ts) {
				_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: Unexpect watermark: %v, cur watermark: %v \n", decodeOutput.ts, s.watermark)
				continue
			}

			_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: {%s} [%v(%v)].[%v(%v)]\n",
				decodeOutput.ts.DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())

			err := s.Sink(ctx, decodeOutput)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: {%s} [%v(%v)].[%v(%v)], sink error: %v\n",
					decodeOutput.ts.DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId(),
					err,
				)
				// TODO handle error
				continue
			}
			_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Sinker: {%s} [%v(%v)].[%v(%v)], sink over\n",
				decodeOutput.ts.DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())

			s.watermark = decodeOutput.ts
			s.updateWatermarkFunc(tableCtx.TableId(), s.watermark)
		}
	}
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
	if err != nil {
		return err
	}
	return err
}

func (s *mysqlSink) Send(ctx context.Context, data *DecoderOutput) (err error) {
	sendRows := func(info string, rows [][]byte) (serr error) {
		fmt.Fprintln(os.Stderr, "----mysql sink----", info, len(rows))
		for _, row := range rows {
			if len(row) == 0 {
				continue
			}
			plen := min(len(row), 200)
			fmt.Fprintln(os.Stderr, "----mysql sink----", info, string(row[:plen]))
			_, serr = s.conn.ExecContext(ctx, util.UnsafeBytesToString(row))
			if serr != nil {
				return serr
			}
		}
		return
	}
	fmt.Fprintln(os.Stderr, "----mysql sink begin----")
	defer fmt.Fprintln(os.Stderr, "----mysql sink end----")
	sqlOfRows := data.sqlOfRows.Load().([][]byte)
	err = sendRows("rows", sqlOfRows)
	if err != nil {
		return err
	}

	sqlOfObjects := data.sqlOfObjects.Load().([][]byte)
	err = sendRows("objects", sqlOfObjects)
	if err != nil {
		return err
	}

	sqlOfDeletes := data.sqlOfDeletes.Load().([][]byte)
	err = sendRows("deletes", sqlOfDeletes)
	if err != nil {
		return err
	}
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
