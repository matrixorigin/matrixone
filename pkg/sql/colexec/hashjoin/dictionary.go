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

package hashjoin

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (hashJoin *HashJoin) dictionaryInputSupported() bool {
	return hashJoin.resultAllocation != nil && hashJoin.NonEqCond == nil && !hashJoin.AsofBuildLeft &&
		(hashJoin.IsInner() || hashJoin.IsLeftSemi() || hashJoin.IsLeftAnti()) &&
		colexec.ExpressionsAcceptDictionary(hashJoin.ctr.eqCondExecs)
}
func (hashJoin *HashJoin) AcceptsDictionary(child int) bool {
	return child == 0 && hashJoin.dictionaryInputSupported()
}

// Results stay owned and flat. Recursive marker forwarding still declares its
// possible representation so unknown destinations retain the boundary contract.
func (hashJoin *HashJoin) DictionaryOutput() (bool, *vector.AllocationAccountSelection) {
	return hashJoin.dictionaryInputSupported(), hashJoin.resultAllocation
}
func (hashJoin *HashJoin) DictionaryOutputState() *vm.DictionaryOutputState {
	return &hashJoin.ctr.dictionaryBoundary
}
func (hashJoin *HashJoin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return hashJoin.ctr.dictionaryBoundary.Normalize(input, proc.Mp())
}

// Supported probe joins copy matching values into their owned result buffers;
// retaining per-input-column dictionaries is unnecessary for that lifetime.
// This preference does not weaken capability: retained inputs remain supported.
func (hashJoin *HashJoin) PrefersOwnedDictionaryInput(child int) bool {
	return child == 0 && hashJoin.dictionaryInputSupported()
}
