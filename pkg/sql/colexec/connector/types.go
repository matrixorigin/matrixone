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

package connector

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Connector)

// Connector pipe connector
type Connector struct {
	ctr container

	Reg          *process.WaitRegister
	cleanupSpool *pSpool.PipelineSpool
	vm.OperatorBase
}

type container struct {
	sp *pSpool.PipelineSpool
}

func (connector *Connector) GetOperatorBase() *vm.OperatorBase {
	return &connector.OperatorBase
}

func init() {
	reuse.CreatePool[Connector](
		func() *Connector {
			return &Connector{}
		},
		func(a *Connector) {
			*a = Connector{}
		},
		reuse.DefaultOptions[Connector]().
			WithEnableChecker(),
	)
}

func (connector Connector) TypeName() string {
	return opName
}

func (connector *Connector) OpType() vm.OpType {
	return vm.Connector
}

func NewArgument() *Connector {
	return reuse.Alloc[Connector](nil)
}

func (connector *Connector) WithReg(reg *process.WaitRegister) *Connector {
	connector.Reg = reg
	return connector
}

func (connector *Connector) Release() {
	if connector != nil {
		reuse.Free[Connector](connector, nil)
	}
}

func (connector *Connector) Reset(proc *process.Process, pipelineFailed bool, err error) {
	newDirectSignal := func() process.PipelineSignal {
		if proc == nil {
			return process.NewPipelineSignalToDirectly(nil, err, nil)
		}
		return process.NewPipelineSignalToDirectly(nil, err, proc.Mp())
	}

	if connector.ctr.sp != nil {
		sp := connector.ctr.sp
		sendCtx, cancel := context.WithTimeout(context.TODO(), process.PipelineSignalSendTimeout)
		queryDone, sendErr := sp.SendBatch(sendCtx, pSpool.SendToAllLocal, nil, err)
		cancel()

		terminalSignal := newDirectSignal()
		terminalSignalName := "direct"
		if sendErr == nil && !queryDone {
			terminalSignal = process.NewPipelineSignalToGetFromSpool(sp, 0)
			terminalSignalName = "spool"
		} else {
			process.WarnPipelineCleanupf(
				proc,
				"connector_cleanup_enqueue_terminal_spool",
				"connector cleanup timed out enqueueing terminal spool message: timeout=%s query_done=%t err=%v",
				process.PipelineSignalSendTimeout,
				queryDone,
				sendErr)
		}

		sentTerminalSignal := process.SendPipelineSignalWithTimeout(connector.Reg, terminalSignal, process.PipelineSignalSendTimeout)
		if !sentTerminalSignal {
			chLen, chCap := process.WaitRegisterChannelState(connector.Reg)
			process.WarnPipelineCleanupf(
				proc,
				"connector_cleanup_send_terminal_signal",
				"connector cleanup timed out sending terminal %s signal: timeout=%s channel_len=%d channel_cap=%d pipeline_failed=%t err=%v",
				terminalSignalName,
				process.PipelineSignalSendTimeout,
				chLen,
				chCap,
				pipelineFailed,
				err)
		}

		spoolClosed := false
		if terminalSignalName == "spool" && sentTerminalSignal {
			spoolClosed = sp.CloseWithTimeout(process.PipelineSignalSendTimeout)
			if !spoolClosed {
				process.WarnPipelineCleanupf(
					proc,
					"connector_cleanup_close_spool",
					"connector cleanup timed out closing pipeline spool: timeout=%s pipeline_failed=%t err=%v",
					process.PipelineSignalSendTimeout,
					pipelineFailed,
					err)
			}
		} else {
			process.WarnPipelineCleanupf(
				proc,
				"connector_cleanup_skip_spool_close",
				"connector cleanup skipped waiting for pipeline spool close after terminal %s signal delivery failed or fell back: delivered=%t pipeline_failed=%t err=%v",
				terminalSignalName,
				sentTerminalSignal,
				pipelineFailed,
				err)
		}
		if !spoolClosed {
			connector.cleanupSpool = sp
		}
		connector.ctr.sp = nil
	} else {
		if !process.SendPipelineSignalWithTimeout(
			connector.Reg,
			newDirectSignal(),
			process.PipelineSignalSendTimeout) {
			chLen, chCap := process.WaitRegisterChannelState(connector.Reg)
			process.WarnPipelineCleanupf(
				proc,
				"connector_cleanup_send_terminal_signal",
				"connector cleanup timed out sending terminal direct signal: timeout=%s channel_len=%d channel_cap=%d pipeline_failed=%t err=%v",
				process.PipelineSignalSendTimeout,
				chLen,
				chCap,
				pipelineFailed,
				err)
		}
	}
}

func (connector *Connector) CleanupDeferredSpool() {
	if connector.cleanupSpool == nil {
		return
	}
	connector.cleanupSpool.ForceCleanup()
	connector.cleanupSpool = nil
}

func (connector *Connector) Free(proc *process.Process, pipelineFailed bool, err error) {
}

func (connector *Connector) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
