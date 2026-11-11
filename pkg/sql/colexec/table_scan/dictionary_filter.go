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

package table_scan

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"time"
)

func (tableScan *TableScan) selectReaderFilter(proc *process.Process, bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
	start := time.Now()
	defer func() { tableScan.ctr.filterActiveDuration += time.Since(start) }()
	tableScan.recordFilterInput(bat, loaded)
	tableScan.ctr.filterLateMaterialized = loaded != nil && len(loaded) < len(tableScan.Types)
	first := !tableScan.ctr.subjectReuseAdmissionDone
	tableScan.ctr.subjectReuseAdmissionDone = true
	var before func(int, int)
	if first {
		before = func(i, rows int) {
			if i >= len(tableScan.ctr.runtimeFilterExecutors) {
				index := i - len(tableScan.ctr.runtimeFilterExecutors)
				executor := colexec.ReuseFilterExpressionExecutor(tableScan.FilterExprs[index], tableScan.ctr.filterExecutors[index], rows)
				tableScan.ctr.filterExecutors[index] = executor
				tableScan.ctr.allFilterExecutors[i] = executor
			}
		}
	}
	rows, all, err := colexec.SelectPredicateRows(proc, bat, loaded, tableScan.ctr.allFilterExecutors, tableScan.OutputAllocation,
		&tableScan.ctr.filterRows, &tableScan.ctr.filterBs, tableScan.OpAnalyzer, before, &tableScan.ctr.dictionaryInputs)
	return engine.ReaderFilterResult{Sels: rows, All: all}, err
}

// prepareColumnUses distinguishes values leaving projection from private inputs.
// Directly forwarded columns take priority over computed uses of the same input.
func (tableScan *TableScan) prepareColumnUses() {
	if tableScan.ctr.columnUsesReady {
		return
	}
	tableScan.ctr.columnUsesReady = true
	if len(tableScan.ProjectList) == 0 || len(tableScan.ProjectExecutors) != len(tableScan.ProjectList) {
		return
	}
	uses := make([]engine.ReaderColumnUse, len(tableScan.Types))
	for i := range uses {
		uses[i] = engine.ReaderColumnUnused
	}
	seen := make([]bool, len(uses))
	columns := make([]int, 0, len(uses))
	for i, executor := range tableScan.ProjectExecutors {
		clear(seen)
		columns = columns[:0]
		if !colexec.CollectSingleBatchInputColumns(executor, seen, &columns) {
			return
		}
		use := engine.ReaderColumnOwned
		if _, direct := tableScan.ProjectList[i].Expr.(*plan.Expr_Col); direct && executor.IsColumnExpr() {
			use = engine.ReaderColumnRetain
		}
		for _, column := range columns {
			if use == engine.ReaderColumnRetain || uses[column] == engine.ReaderColumnUnused {
				uses[column] = use
			}
		}
	}
	// Domain probes can subsequently shrink the whole batch. Preserve aligned
	// values for every column in that mode instead of publishing empty slots.
	if len(tableScan.ctr.domainSources)+len(tableScan.ctr.domainProbes) > 0 {
		for i, use := range uses {
			if use == engine.ReaderColumnUnused {
				uses[i] = engine.ReaderColumnOwned
			}
		}
	}
	tableScan.ctr.columnUses = uses
}
