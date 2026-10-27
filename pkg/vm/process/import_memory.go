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
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
)

// ImportMemory is shared by the local pipelines of one execution. Admission
// happens before execution locks; the compiler closes its root reference after
// operator/job cleanup. Active leases keep their promises alive independently.
type ImportMemory struct {
	WorkingSet *rscthrottler.WorkingSet
	UnitBytes  int64
	// Initial layout bounds. Runtime owners read the monotonic WorkingSet
	// capacities after a wider record has grown the complete reservation.
	SourceBytes    int64
	OutputBytes    int64
	JoinInputBytes int64
	IOBytes        int64
	RecordBytes    int64
	GrowRecord     func(context.Context, int64) (int64, error)
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
	return m.WorkingSet.AcquireIO(ctx, m.WorkingSet.IOBytes())
}

func (m *ImportMemory) JoinInputCapacity() int64 {
	if m.WorkingSet == nil {
		return m.JoinInputBytes
	}
	return m.WorkingSet.InputCapacity(2)
}

// AcquireJoinInput obtains one fixed build/probe slot. It is separate from CSV
// parser storage, whose maximum single-record envelope may be much larger.
func (m *ImportMemory) AcquireJoinInput(ctx context.Context) (*rscthrottler.InputLease, error) {
	if m == nil || m.WorkingSet == nil {
		return nil, nil
	}
	return m.WorkingSet.AcquireInput(ctx, 2)
}
