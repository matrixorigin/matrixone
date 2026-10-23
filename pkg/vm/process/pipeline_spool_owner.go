// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package process

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
)

type pipelineSpoolRegistry struct {
	mu     sync.Mutex
	spools []*pSpool.PipelineSpool
}

// RegisterPipelineSpool transfers final teardown responsibility to the
// query attempt. Call immediately after construction, before any
// subsequent Prepare operation can fail. Ordinary last-reader cleanup remains
// effective. Idle cache policy is independent of this terminal ownership.
func (proc *Process) RegisterPipelineSpool(spool *pSpool.PipelineSpool) {
	if proc == nil || proc.Base == nil || spool == nil {
		return
	}
	registry := &proc.Base.pipelineSpools
	registry.mu.Lock()
	registry.spools = append(registry.spools, spool)
	registry.mu.Unlock()
}

// ClosePipelineSpoolsAfterReadersStopped belongs to the query-attempt root,
// after all local scopes and their cleanup have joined and before its memory
// promise is released. Process.Free cannot do this: child processes share Base.
func (bp *BaseProcess) ClosePipelineSpoolsAfterReadersStopped() {
	registry := &bp.pipelineSpools
	registry.mu.Lock()
	spools := registry.spools
	registry.spools = nil
	registry.mu.Unlock()
	for _, spool := range spools {
		spool.CloseAfterReadersStopped()
	}
}
