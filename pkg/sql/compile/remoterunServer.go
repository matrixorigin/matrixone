// Copyright 2021 Matrix Origin
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

package compile

import (
	"context"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
	"time"
)

// CnServerMessageHandler receive and deal the message from cn-client.
//
// The message should always *pipeline.Message here.
// there are 2 types of pipeline message now.
//
//  1. notify message :
//     a message to tell the dispatch pipeline where its remote receiver are.
//     and we use this connection's write-back method to send the data.
//
//  2. scope message :
//     a message contains the encoded pipeline.
//     we decoded it and run it locally.
func CnServerMessageHandler(
	ctx context.Context,
	serverAddress string,
	message morpc.Message,
	cs morpc.ClientSession,
	storageEngine engine.Engine, fileService fileservice.FileService, lockService lockservice.LockService,
	queryClient qclient.QueryClient,
	HaKeeper logservice.CNHAKeeperClient, udfService udf.Service, txnClient client.TxnClient,
	autoIncreaseCM *defines.AutoIncrCacheManager,
	messageAcquirer func() morpc.Message) (err error) {

	startTime := time.Now()
	defer func() {
		v2.PipelineServerDurationHistogram.Observe(time.Since(startTime).Seconds())

		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(ctx, e)
			getLogger().Error("panic in CnServerMessageHandler",
				zap.String("error", err.Error()))
			err = errors.Join(err, cs.Close())
		}
	}()

	// check message is valid.
	msg, ok := message.(*pipeline.Message)
	if !ok {
		logutil.Errorf("cn server should receive *pipeline.Message, but get %v", message)
		panic("cn server receive a message with unexpected type")
	}

	// prepare the receiver structure, just for easy using the `send` method.
	receiver := newMessageReceiverOnServer(ctx, serverAddress, msg,
		cs, messageAcquirer, storageEngine, fileService, lockService, queryClient, HaKeeper, udfService, txnClient, autoIncreaseCM)

	// how to handle the *pipeline.Message.
	if receiver.needNotReply {
		err = handlePipelineMessage(&receiver)
	} else {
		if err = handlePipelineMessage(&receiver); err != nil {
			err = receiver.sendError(err)
		} else {
			err = receiver.sendEndMessage()
		}
		colexec.Get().RemoveRunningPipeline(receiver.clientSession)
	}

	return err
}

func handlePipelineMessage(receiver *messageReceiverOnServer) error {

	switch receiver.messageTyp {
	case pipeline.Method_PrepareDoneNotifyMessage:
		dispatchProc, err := receiver.GetProcByUuid(receiver.messageUuid)
		if err != nil || dispatchProc == nil {
			return err
		}
		colexec.Get().RecordRunningPipeline(receiver.clientSession, dispatchProc)

		infoToDispatchOperator := process.WrapCs{
			MsgId: receiver.messageId,
			Uid:   receiver.messageUuid,
			Cs:    receiver.clientSession,
			Err:   make(chan error, 1),
		}

		// todo : the timeout should be removed.
		//		but I keep it here because I don't know whether it will cause hung sometimes.
		timeLimit, cancel := context.WithTimeout(context.TODO(), HandleNotifyTimeout)

		succeed := false
		select {
		case <-timeLimit.Done():
			err = moerr.NewInternalError(receiver.ctx, "send notify msg to dispatch operator timeout")
		case dispatchProc.DispatchNotifyCh <- infoToDispatchOperator:
			succeed = true
		case <-receiver.ctx.Done():
		case <-dispatchProc.Ctx.Done():
		}
		cancel()

		if err != nil || !succeed {
			dispatchProc.Cancel()
			return err
		}

		select {
		case <-receiver.ctx.Done():
			dispatchProc.Cancel()

		// there is no need to check the dispatchProc.Ctx.Done() here.
		// because we need to receive the error from dispatchProc.DispatchNotifyCh.
		case err = <-infoToDispatchOperator.Err:
		}
		return err

	case pipeline.Method_PipelineMessage:
		runCompile := receiver.newCompile()
		colexec.Get().RecordRunningPipeline(receiver.clientSession, runCompile.proc)

		// decode and running the pipeline.
		s, err := decodeScope(receiver.scopeData, runCompile.proc, true, runCompile.e)
		if err != nil {
			return err
		}

		defer func() {
			runCompile.proc.FreeVectors()
			runCompile.proc.CleanValueScanBatchs()
			runCompile.proc.AnalInfos = nil
			runCompile.anal.analInfos = nil

			runCompile.Release()
			s.release()
		}()
		err = s.ParallelRun(runCompile)
		if err == nil {
			// record the number of s3 requests
			runCompile.proc.AnalInfos[runCompile.anal.curr].S3IOInputCount += runCompile.counterSet.FileService.S3.Put.Load()
			runCompile.proc.AnalInfos[runCompile.anal.curr].S3IOInputCount += runCompile.counterSet.FileService.S3.List.Load()
			runCompile.proc.AnalInfos[runCompile.anal.curr].S3IOOutputCount += runCompile.counterSet.FileService.S3.Head.Load()
			runCompile.proc.AnalInfos[runCompile.anal.curr].S3IOOutputCount += runCompile.counterSet.FileService.S3.Get.Load()
			runCompile.proc.AnalInfos[runCompile.anal.curr].S3IOOutputCount += runCompile.counterSet.FileService.S3.Delete.Load()
			runCompile.proc.AnalInfos[runCompile.anal.curr].S3IOOutputCount += runCompile.counterSet.FileService.S3.DeleteMulti.Load()

			receiver.finalAnalysisInfo = runCompile.proc.AnalInfos
		} else {
			// there are 3 situations to release analyzeInfo
			// 1 is free analyzeInfo of Local CN when release analyze
			// 2 is free analyzeInfo of remote CN before transfer back
			// 3 is free analyzeInfo of remote CN when errors happen before transfer back
			// this is situation 3
			for i := range runCompile.proc.AnalInfos {
				reuse.Free[process.AnalyzeInfo](runCompile.proc.AnalInfos[i], nil)
			}
		}
		return err

	case pipeline.Method_StopSending:
		colexec.Get().CancelRunningPipeline(receiver.clientSession)

	default:
		panic(fmt.Sprintf("unknown pipeline message type %d.", receiver.messageTyp))
	}
	return nil
}
