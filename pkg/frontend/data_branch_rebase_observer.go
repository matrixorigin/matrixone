// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

// RebasePhaseObservation is diagnostic evidence, not an additive CPU or I/O
// accounting record. The total/admission intervals contain their child work.
type RebasePhaseObservation struct {
	Phase   string        `json:"phase"`
	Table   string        `json:"table"`
	SQL     string        `json:"sql,omitempty"`
	Elapsed time.Duration `json:"elapsed_ns"`
	Failed  bool          `json:"failed"`
	// Raw cumulative snapshots retain schema/quality flags. Memory peaks are
	// not additive and must not be subtracted to invent per-query peaks.
	StatsBefore *statistic.StatsArray `json:"stats_before,omitempty"`
	StatsAfter  *statistic.StatsArray `json:"stats_after,omitempty"`
}

var rebasePhaseHook atomic.Pointer[func(RebasePhaseObservation)]

// SetRebasePhaseHookForTest enables bounded diagnostic fixtures only. Tests
// that install a hook must be serial and restore it before closing their sink.
func SetRebasePhaseHookForTest(hook func(RebasePhaseObservation)) func() {
	old := rebasePhaseHook.Swap(&hook)
	return func() { rebasePhaseHook.Store(old) }
}

func beginRebasePhase(phase, table, query string) func(error) {
	return beginRebaseObservedPhase(phase, table, query, nil)
}

func beginRebaseStatementPhase(bh BackgroundExec, phase, table, query string) func(error) {
	return beginRebaseObservedPhase(phase, table, query, bh)
}

func beginRebaseObservedPhase(phase, table, query string, bh BackgroundExec) func(error) {
	hook := rebasePhaseHook.Load()
	if hook == nil || *hook == nil {
		return func(error) {}
	}
	start := time.Now()
	var before *statistic.StatsArray
	if bh != nil {
		snapshot := bh.GetExecStatsArray()
		before = &snapshot
	}
	finished := false
	return func(err error) {
		if finished {
			return
		}
		finished = true
		event := RebasePhaseObservation{Phase: phase, Table: table, SQL: query, Elapsed: time.Since(start), Failed: err != nil, StatsBefore: before}
		if bh != nil {
			snapshot := bh.GetExecStatsArray()
			event.StatsAfter = &snapshot
		}
		(*hook)(event)
	}
}

func readRebasePhaseCounts(ctx context.Context, bh BackgroundExec, phase, table, query string, columns uint64) (counts []uint64, err error) {
	finish := beginRebaseStatementPhase(bh, phase, table, query)
	defer func() { finish(err) }()
	return readRebaseCounts(ctx, bh, query, columns)
}
