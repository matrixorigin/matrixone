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

package compile

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/stretchr/testify/require"
)

func TestMergeRunAsyncEventStateRunsOrdinaryAndLazyUnion(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "lazy"}[lazy], func(t *testing.T) {
			c := newLazyUnionAllTestCompile(t)
			left := newLazyUnionAllLeaf(c, colexec.NewMockOperator().WithBatchs([]*batch.Batch{newLazyUnionAllInt8Batch(c, 1)}))
			right := newLazyUnionAllLeaf(c, colexec.NewMockOperator().WithBatchs([]*batch.Batch{newLazyUnionAllInt8Batch(c, 2)}))
			root := c.compileUnionAll(&planpb.Node{}, []*Scope{left}, []*Scope{right}, lazy)[0]
			var got []int8
			root.setRootOperator(output.NewArgument().WithFunc(
				func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
					if bat != nil {
						got = append(got, vector.MustFixedColWithTypeCheck[int8](bat.Vecs[0])...)
					}
					return nil
				},
			))
			c.scopes = []*Scope{root}
			c.InitPipelineContextToExecuteQuery()
			done := make(chan error, 1)
			require.NoError(t, root.mergeRunAsync(c, func(err error) { done <- err }))
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("event-driven MergeRun did not complete")
			}
			c.waitScopeTaskScheduler()
			require.ElementsMatch(t, []int8{1, 2}, got)
			freeLazyUnionAllTestScope(c, root)
		})
	}
}
