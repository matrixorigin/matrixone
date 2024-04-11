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

package frontend

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"io"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func processLoadLocal(ctx context.Context, ses FeSession, param *tree.ExternParam, writer *io.PipeWriter) (err error) {
	proto := ses.GetMysqlProtocol()
	defer func() {
		err2 := writer.Close()
		if err == nil {
			err = err2
		}
	}()
	err = plan2.InitInfileParam(param)
	if err != nil {
		return
	}
	err = proto.sendLocalInfileRequest(param.Filepath)
	if err != nil {
		return
	}
	start := time.Now()
	var msg interface{}
	msg, err = proto.GetTcpConnection().Read(goetty.ReadOptions{})
	if err != nil {
		proto.SetSequenceID(proto.GetSequenceId() + 1)
		if errors.Is(err, errorInvalidLength0) {
			return nil
		}
		if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
			err = moerr.NewInvalidInput(ctx, "cannot read '%s' from client,please check the file path, user privilege and if client start with --local-infile", param.Filepath)
		}
		return
	}

	packet, ok := msg.(*Packet)
	if !ok {
		proto.SetSequenceID(proto.GetSequenceId() + 1)
		err = moerr.NewInvalidInput(ctx, "invalid packet")
		return
	}

	proto.SetSequenceID(uint8(packet.SequenceID + 1))
	seq := uint8(packet.SequenceID + 1)
	length := packet.Length
	if length == 0 {
		return
	}
	ses.CountPayload(len(packet.Payload))

	skipWrite := false
	// If inner error occurs(unexpected or expected(ctrl-c)), proc.LoadLocalReader will be closed.
	// Then write will return error, but we need to read the rest of the data and not write it to pipe.
	// So we need a flag[skipWrite] to tell us whether we need to write the data to pipe.
	// https://github.com/matrixorigin/matrixone/issues/6665#issuecomment-1422236478

	_, err = writer.Write(packet.Payload)
	if err != nil {
		skipWrite = true // next, we just need read the rest of the data,no need to write it to pipe.
		logError(ses, ses.GetDebugString(),
			"Failed to load local file",
			zap.String("path", param.Filepath),
			zap.Error(err))
	}
	epoch, printEvery, minReadTime, maxReadTime, minWriteTime, maxWriteTime := uint64(0), uint64(1024), 24*time.Hour, time.Nanosecond, 24*time.Hour, time.Nanosecond
	for {
		readStart := time.Now()
		msg, err = proto.GetTcpConnection().Read(goetty.ReadOptions{})
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
				seq += 1
				proto.SetSequenceID(seq)
				err = nil
			}
			break
		}
		readTime := time.Since(readStart)
		if readTime > maxReadTime {
			maxReadTime = readTime
		}
		if readTime < minReadTime {
			minReadTime = readTime
		}
		packet, ok = msg.(*Packet)
		if !ok {
			err = moerr.NewInvalidInput(ctx, "invalid packet")
			seq += 1
			proto.SetSequenceID(seq)
			break
		}
		seq = uint8(packet.SequenceID + 1)
		proto.SetSequenceID(seq)
		ses.CountPayload(len(packet.Payload))

		writeStart := time.Now()
		if !skipWrite {
			_, err = writer.Write(packet.Payload)
			if err != nil {
				logError(ses, ses.GetDebugString(),
					"Failed to load local file",
					zap.String("path", param.Filepath),
					zap.Uint64("epoch", epoch),
					zap.Error(err))
				skipWrite = true
			}
			writeTime := time.Since(writeStart)
			if writeTime > maxWriteTime {
				maxWriteTime = writeTime
			}
			if writeTime < minWriteTime {
				minWriteTime = writeTime
			}
		}
		if epoch%printEvery == 0 {
			logDebugf(ses.GetDebugString(), "load local '%s', epoch: %d, skipWrite: %v, minReadTime: %s, maxReadTime: %s, minWriteTime: %s, maxWriteTime: %s,", param.Filepath, epoch, skipWrite, minReadTime.String(), maxReadTime.String(), minWriteTime.String(), maxWriteTime.String())
			minReadTime, maxReadTime, minWriteTime, maxWriteTime = 24*time.Hour, time.Nanosecond, 24*time.Hour, time.Nanosecond
		}
		epoch += 1
	}
	logDebugf(ses.GetDebugString(), "load local '%s', read&write all data from client cost: %s", param.Filepath, time.Since(start))
	return
}
