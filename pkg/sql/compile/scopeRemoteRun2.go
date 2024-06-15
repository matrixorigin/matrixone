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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"go.uber.org/zap"
)

// remoteRun sends a scope to remote node for running.
// and keep receiving the back results.
//
// we assume that, result message is always *pipeline.Message, and there are 3 cases for that:
// first, Message with error information.
// second, Message with EndFlag and Analysis Information.
// third, Message with batch data.
func (s *Scope) remoteRun2(c *Compile) (sender *messageSenderOnClient, err error) {
	// a defer for safety.
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(s.Proc.Ctx, e)
			getLogger().Error("panic in scope remoteRun",
				zap.String("sql", c.sql),
				zap.String("error", err.Error()))
		}
	}()

	// encode structures which need to send.
	var scopeEncodeData, processEncodeData []byte
	scopeEncodeData, processEncodeData, err = prepareRemoteRunSendingData(c.sql, s)
	if err != nil {
		return nil, err
	}

	// generate a new sender to do send work.
	sender, err = newMessageSenderOnClient(s.Proc.Ctx, s.NodeInfo.Addr, s.Proc.Mp(), c.anal)
	if err != nil {
		c.proc.Errorf(s.Proc.Ctx, "Failed to newMessageSenderOnClient sql=%s, txnID=%s, err=%v",
			c.sql, c.proc.TxnOperator.Txn().DebugString(), err)

		return nil, err
	}

	if err = sender.sendPipeline(scopeEncodeData, processEncodeData); err != nil {
		return sender, err
	}

	sender.safeToClose = false
	sender.alreadyClose = false
	err = receiveMessageFromCnServer2(c, s, sender)
	return nil, nil
}

func prepareRemoteRunSendingData(sqlStr string, s *Scope) (scopeData []byte, processData []byte, err error) {
	// 1.
	// Encode the Scope related.
	// encode all operators in the scope except the last one.
	// because we need to keep it local for receiving and sending batch to next pipeline.
	LastIndex := len(s.Instructions) - 1
	LastOperator := s.Instructions[LastIndex]
	s.Instructions = s.Instructions[:LastIndex]
	defer func() {
		s.Instructions = append(s.Instructions, LastOperator)
	}()

	if scopeData, err = encodeScope(s); err != nil {
		return nil, nil, err
	}

	// 2.
	// Encode the Process related information.
	if processData, err = encodeProcessInfo(s.Proc, sqlStr); err != nil {
		return nil, nil, err
	}

	return scopeData, processData, nil
}

func receiveMessageFromCnServer2(c *Compile, s *Scope, sender *messageSenderOnClient) error {
	// generate a new pipeline to send data in local.
	// value_scan -> connector / dispatch -> next pipeline.
	fakeValueScanOperator := value_scan.NewArgument()
	defer func() {
		fakeValueScanOperator.Release()
	}()

	LastOperator := s.Instructions[len(s.Instructions)-1]
	lastAnalyze := c.proc.GetAnalyze(LastOperator.Idx, -1, false)
	switch arg := LastOperator.Arg.(type) {
	case *connector.Argument:
		oldChildren := arg.Children
		arg.Children = nil
		arg.AppendChild(fakeValueScanOperator)
		defer func() {
			arg.Children = oldChildren
		}()

	case *dispatch.Argument:
		oldChildren := arg.Children
		arg.Children = nil
		arg.AppendChild(fakeValueScanOperator)
		defer func() {
			arg.Children = oldChildren
		}()

	default:
		panic(
			fmt.Sprintf("remote run pipeline has an unexpected operator [id = %d] at last.", LastOperator.Op))
	}

	// receive back result and send.
	var bat *batch.Batch
	var end bool
	var err error
	for {
		bat, end, err = sender.receiveBatch()
		if err != nil {
			return err
		}
		if end {
			return nil
		}

		lastAnalyze.Network(bat)
		fakeValueScanOperator.Batchs = append(fakeValueScanOperator.Batchs, bat)

		result, errCall := LastOperator.Arg.Call(s.Proc)
		if errCall != nil || result.Status == vm.ExecStop {
			return errCall
		}
	}

	return nil
}

func generateStopSendingMessage() *pipeline.Message {
	message := cnclient.AcquireMessage()
	message.SetMessageType(pipeline.Method_StopSending)
	message.NeedNotReply = true
	return message
}
