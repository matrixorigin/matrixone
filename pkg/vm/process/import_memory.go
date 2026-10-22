// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package process

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
)

// ImportMemory is shared by the local pipelines of one execution. Admission
// happens before execution locks; the compiler closes its root reference after
// operator/job cleanup. Active leases keep their promises alive independently.
type ImportMemory struct {
	WorkingSet     *rscthrottler.WorkingSet
	UnitBytes      int64
	SourceBytes    int64
	OutputBytes    int64
	JoinInputBytes int64
	IOBytes        int64
}

func (m *ImportMemory) AcquireInput(ctx context.Context) (*rscthrottler.InputLease, error) {
	if m == nil || m.WorkingSet == nil {
		return nil, nil
	}
	return m.WorkingSet.AcquireInput(ctx, 0)
}
func (m *ImportMemory) AcquireOutput(ctx context.Context) (*rscthrottler.InputLease, error) {
	if m == nil || m.WorkingSet == nil {
		return nil, nil
	}
	return m.WorkingSet.AcquireInput(ctx, 1)
}
func (m *ImportMemory) AcquireIO(ctx context.Context) (*rscthrottler.WorkspaceLease, error) {
	if m == nil || m.WorkingSet == nil {
		return nil, nil
	}
	return m.WorkingSet.AcquireIO(ctx, m.IOBytes)
}

// AcquireJoinInput obtains one fixed build/probe slot. It is separate from CSV
// parser storage, whose maximum single-record envelope may be much larger.
func (m *ImportMemory) AcquireJoinInput(ctx context.Context) (*rscthrottler.InputLease, error) {
	if m == nil || m.WorkingSet == nil {
		return nil, nil
	}
	return m.WorkingSet.AcquireInput(ctx, 2)
}
