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
	terminalSignal := process.BuildCleanupSignal(pipelineFailed, err)

	if connector.ctr.sp != nil {
		sp := connector.ctr.sp
		connector.sendTerminalWithLog(proc, terminalSignal, pipelineFailed, err)

		// Since we send typed terminal signals directly (not via spool),
		// the receiver exits without draining the spool. Use Abort()
		// for immediate resource release instead of CloseWithTimeout.
		sp.Abort()
		connector.ctr.sp = nil
	} else {
		connector.sendTerminalWithLog(proc, terminalSignal, pipelineFailed, err)
	}
}

// sendTerminalWithLog sends a terminal signal to Reg, logging a warning on failure.
func (connector *Connector) sendTerminalWithLog(proc *process.Process, signal process.PipelineSignal, pipelineFailed bool, err error) {
	if connector.Reg == nil {
		process.WarnPipelineCleanupf(
			proc,
			"connector_cleanup_nil_reg",
			"connector cleanup skipped terminal %s signal because Reg is nil: pipeline_failed=%t err=%v",
			signal.EventType.String(),
			pipelineFailed,
			err)
		return
	}
	if process.SendPipelineSignalWithTimeout(connector.Reg, signal, process.PipelineSignalSendTimeout) {
		return
	}
	chLen, chCap := process.WaitRegisterChannelState(connector.Reg)
	process.WarnPipelineCleanupf(
		proc,
		"connector_cleanup_send_terminal_signal",
		"connector cleanup timed out sending terminal %s signal: timeout=%s channel_len=%d channel_cap=%d pipeline_failed=%t err=%v",
		signal.EventType.String(),
		process.PipelineSignalSendTimeout,
		chLen,
		chCap,
		pipelineFailed,
		err)
}

// cleanupSpool is deprecated. With typed terminal signals + sp.Abort() in Reset(),
// deferred cleanup is no longer needed. The field and method exist only to satisfy
// the vm.Operator interface (called by pkg/vm/pipeline/types.go cleanup walks).
// Kept for backward compatibility — will be removed when the interface is updated.
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
