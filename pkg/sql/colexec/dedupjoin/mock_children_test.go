// Copyright 2026 Matrix Origin
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

package dedupjoin

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

// freeTestChildren frees the batches held by the mock children of op. An
// operator never frees its children; the batches are freed with the pool
// their vectors were created with.
func freeTestChildren(op vm.Operator) {
	if op == nil {
		return
	}
	base := op.GetOperatorBase()
	if base == nil {
		return
	}
	seen := make(map[*batch.Batch]struct{})
	for i := 0; i < base.NumChildren(); i++ {
		mock, ok := base.GetChildren(i).(*colexec.MockOperator)
		if !ok {
			continue
		}
		for _, bat := range mock.GetBatchs() {
			if bat == nil || bat == batch.EmptyBatch {
				continue
			}
			if _, done := seen[bat]; done {
				continue
			}
			seen[bat] = struct{}{}
			for j, vec := range bat.Vecs {
				if vec != nil {
					vec.Free(vec.MPool())
					bat.Vecs[j] = nil
				}
			}
			bat.Clean(nil)
		}
	}
}
