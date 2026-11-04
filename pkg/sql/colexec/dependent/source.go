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

package dependent

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"os"
	"unsafe"
)

// sourceEdge owns one invocation's producer and every independent scan cursor.
// Demand schedules a producer only when the requested position is unavailable;
// materialized.Next is never allowed to wait for work on this same lane.
type sourceEdge struct {
	source  *materialized.Source
	budget  *process.ExecutionResourceGeneration
	charged uint64
	count   int
	done    bool
}

func (r *region) newSource(readers int) (*sourceEdge, error) {
	if r.execution.account == nil {
		return nil, moerr.NewInternalError(r.proc.Ctx, "dependent source requires statement account")
	}
	size := uint64(unsafe.Sizeof(sourceEdge{})) + uint64(unsafe.Sizeof(materialized.Source{})) + uint64(readers)*32
	if err := r.budget.AcquireAllocationCapacity(size); err != nil {
		return nil, err
	}
	s := materialized.NewSource(readers)
	budget := r.budget
	err := s.Begin(r.proc.Mp(), materialized.SpillConfig{
		AllocationAccount: r.execution.account,
		FileFactory: func(name string) (*os.File, error) {
			fs, err := r.proc.GetSpillFileService()
			if err != nil {
				return nil, err
			}
			return fs.CreateAndRemoveFile(r.proc.Ctx, name)
		},
		Budget: materialized.SpillBudget{
			ReserveMemory: func(n uint64) (materialized.Reservation, error) { return budget.ReserveTransientMemory(n) },
			ReserveDisk:   func(n uint64) (materialized.GrowingReservation, error) { return budget.ReserveSpillDisk(n) },
			ReserveFD:     func(n uint64) (materialized.Reservation, error) { return budget.ReserveSpillFD(n) },
		},
	})
	if err != nil {
		s.Close()
		budget.ReleaseAllocationCapacity(size)
		return nil, err
	}
	return &sourceEdge{source: s, budget: budget, charged: size}, nil
}

// A recursive frontier is retired at each round, before the containing region
// ends. Its control charge must follow that same lifetime instead of retaining
// one charge for every completed round.
func (s *sourceEdge) close() {
	if s == nil || s.source == nil {
		return
	}
	s.source.Close()
	s.source = nil
	s.budget.ReleaseAllocationCapacity(s.charged)
	s.charged = 0
}
func (s *sourceEdge) accept(b *batch.Batch) error {
	if b == nil {
		s.done = true
		s.source.Finish(nil)
		return nil
	}
	if b.IsEmpty() {
		return nil
	}
	if err := s.source.Append(b); err != nil {
		return err
	}
	s.count++
	return nil
}

func (c *cursor) readSource(ctx context.Context) (*pullTask, error) {
	r := c.region
	for c.sourceArm < len(c.template.node.SourceStep) {
		step := c.template.node.SourceStep[c.sourceArm]
		var edge *sourceEdge
		if c.template.node.NodeType == plan.Node_RECURSIVE_SCAN {
			if r.frontier == nil || r.frontierStep != step {
				return nil, moerr.NewInternalError(ctx, "recursive frontier ownership")
			}
			edge = r.frontier
		} else {
			edge = r.sources[step]
			if edge == nil {
				var err error
				edge, err = r.newSource(r.template.sourceReaders[step])
				if err != nil {
					return nil, err
				}
				r.sources[step] = edge
			}
		}
		c.edge = edge
		if c.sourcePosition < edge.count || edge.done {
			b, end, err := edge.source.Next(ctx, c.template.readerIDs[c.sourceArm], c.sourcePosition)
			if err != nil {
				return nil, err
			}
			if end {
				c.sourceArm++
				c.sourcePosition = 0
				continue
			}
			c.sourcePosition++
			c.input, c.ownedInput = b, b
			return nil, nil
		}
		c.sourcePending = true
		return r.pull(r.execution.template.query.Steps[step])
	}
	c.eof = true
	return nil, nil
}
