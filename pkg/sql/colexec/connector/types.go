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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

	Reg               *process.WaitRegister
	cleanupSpool      *pSpool.PipelineSpool
	allocationAccount *mpool.AllocationAccount
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

func (connector *Connector) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if connector.allocationAccount != nil && connector.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	connector.allocationAccount = account
	return nil
}

// ActivatesAllocationAccountLifecycle reports that Connector only participates
// in an account already required by an allocation-producing operator.
func (connector *Connector) ActivatesAllocationAccountLifecycle() bool {
	return false
}

func (connector *Connector) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if connector.allocationAccount == nil {
		return nil
	}
	if connector.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if connector.ctr.sp != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if connector.cleanupSpool != nil {
		connector.cleanupSpool.FinalizeAfterConsumersQuiesced()
		connector.cleanupSpool = nil
	}
	connector.allocationAccount = nil
	return nil
}

func (connector *Connector) Release() {
	if connector != nil {
		reuse.Free[Connector](connector, nil)
	}
}

func (connector *Connector) Reset(proc *process.Process, pipelineFailed bool, err error) {
	terminalSignal := process.BuildCleanupSignal(pipelineFailed, err)
	terminalErr := terminalSignal.TerminalErr()
	effective, published := connector.publishTerminalWithLog(proc, terminalSignal)
	if published && effective.EventType != process.EventEnd {
		terminalErr = effective.TerminalErr()
	}

	if connector.ctr.sp != nil {
		sp := connector.ctr.sp

		if terminalSignal.EventType == process.EventEnd && published && effective.EventType == process.EventEnd {
			connector.cleanupSpool = sp
		} else {
			abortErr := terminalErr
			if !published && abortErr == nil {
				abortErr = process.ResolvePipelineSpoolAbortError(connector.Reg)
			}
			sp.Abort(abortErr)
			if connector.allocationAccount != nil {
				connector.cleanupSpool = sp
			} else {
				connector.cleanupSpool = nil
			}
		}
		connector.ctr.sp = nil
	}
}

func (connector *Connector) publishTerminalWithLog(proc *process.Process, signal process.PipelineSignal) (process.PipelineSignal, bool) {
	if connector.Reg == nil {
		process.WarnPipelineCleanupf(
			proc,
			"connector_cleanup_nil_reg",
			"connector cleanup skipped terminal %s signal because Reg is nil: pipeline_failed=%t err=%v",
			signal.EventType.String(),
			signal.EventType != process.EventEnd,
			signal.TerminalErr())
		return process.PipelineSignal{}, false
	}
	if effective, ok := connector.Reg.PublishTerminal(signal); ok {
		return effective, true
	}
	chLen, chCap := process.WaitRegisterChannelState(connector.Reg)
	process.WarnPipelineCleanupf(
		proc,
		"connector_cleanup_send_terminal_signal",
		"connector cleanup could not publish terminal %s signal: channel_len=%d channel_cap=%d err=%v",
		signal.EventType.String(),
		chLen,
		chCap,
		signal.TerminalErr())
	return process.PipelineSignal{}, false
}

// CleanupDeferredSpool reclaims spool cache memory after the paired Merge
// cleanup has returned on a normal End path. The normal path drains queued
// GetFromSpool signals; a cleanup-time timeout releases the current reference
// and leaves no receiver goroutine that can read pending signals later.
func (connector *Connector) CleanupDeferredSpool() {
	if connector.cleanupSpool == nil {
		return
	}
	if connector.allocationAccount != nil {
		connector.cleanupSpool.ReleaseReusableCacheAfterProducerQuiesced()
		return
	}
	connector.cleanupSpool.ForceCleanupAfterTerminalSignal()
	connector.cleanupSpool = nil
}

func (connector *Connector) Free(proc *process.Process, pipelineFailed bool, err error) {
}

func (connector *Connector) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
