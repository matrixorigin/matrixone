// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
)

type pipelineSpoolRegistry struct {
	mu        sync.Mutex
	spools    []*pSpool.PipelineSpool
	edges     []*WaitRegister
	receivers []*PipelineSignalReceiver
}

// RegisterPipelineSpool transfers final teardown responsibility to the
// query attempt. Call immediately after construction, before any
// subsequent Prepare operation can fail. Ordinary last-reader cleanup remains
// effective. Idle cache policy is independent of this terminal ownership.
func (proc *Process) RegisterPipelineSpool(spool *pSpool.PipelineSpool, edges ...*WaitRegister) {
	if proc == nil || proc.Base == nil || spool == nil {
		return
	}
	registry := &proc.Base.pipelineSpools
	registry.mu.Lock()
	registry.spools = append(registry.spools, spool)
	registry.edges = append(registry.edges, edges...)
	registry.mu.Unlock()
}

// RegisterPipelineSignalReceiver retains current-data ownership even if an
// earlier operator cleanup panics before this receiver's Reset can run.
func (proc *Process) RegisterPipelineSignalReceiver(receiver *PipelineSignalReceiver) {
	if proc == nil || proc.Base == nil || receiver == nil {
		return
	}
	registry := &proc.Base.pipelineSpools
	registry.mu.Lock()
	registry.receivers = append(registry.receivers, receiver)
	registry.edges = append(registry.edges, receiver.srcReg...)
	registry.mu.Unlock()
}

// ReleasePipelineCurrentsAfterReadersStopped is the first quiescent phase.
// Compile runs it for every owned Base before draining any graph edge. It is
// idempotent when ordinary Reset already released the receiver's current data.
func (bp *BaseProcess) ReleasePipelineCurrentsAfterReadersStopped() {
	registry := &bp.pipelineSpools
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, receiver := range registry.receivers {
		receiver.releaseCurrent()
	}
}

// ClosePipelineSpoolsAfterReadersStopped belongs to the query-attempt root,
// after all local scopes and their cleanup have joined and before its memory
// promise is released. Process.Free cannot do this: child processes share Base.
func (bp *BaseProcess) ClosePipelineSpoolsAfterReadersStopped() {
	registry := &bp.pipelineSpools
	registry.mu.Lock()
	defer registry.mu.Unlock()
	// Every producer/receiver and remote terminal settlement has joined. Drain
	// pending notifications before destroying their spool generations, then drop
	// the registry's references before the caller returns the root promise.
	for _, receiver := range registry.receivers {
		receiver.releaseCurrent()
	}
	for _, edge := range registry.edges {
		edge.DrainForCleanup()
	}
	for _, spool := range registry.spools {
		spool.CloseAfterReadersStopped()
	}
	registry.receivers = nil
	registry.edges = nil
	registry.spools = nil
}
