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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestCompileGenerateSeriesParallelPartitionsOffsetsAcrossCNs(t *testing.T) {
	offsets := [][2]int64{
		{1, 100_000},
		{100_001, 200_000},
		{200_001, 300_000},
		{300_001, 400_000},
	}
	c := NewMockCompile(t)
	c.addr = "ingress:6001"
	c.anal = &AnalyzeModule{}
	c.cnList = engine.Nodes{
		{Addr: "cn-1:6001", Mcpu: 2},
		{Addr: "cn-2:6001", Mcpu: 3},
	}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{}}}
	node := &plan.Node{TableDef: &plan.TableDef{
		Cols:    []*plan.ColDef{{Name: "result"}},
		TblFunc: &plan.TableFunction{Name: "generate_series"},
	}}

	scopes, err := c.compileGenerateSeriesParallel(
		node,
		nil,
		len(offsets),
		true,
		offsets,
		1,
	)
	require.NoError(t, err)
	require.Len(t, scopes, 2)

	first := scopes[0].RootOp.(*table_function.TableFunction)
	second := scopes[1].RootOp.(*table_function.TableFunction)
	t.Cleanup(first.Release)
	t.Cleanup(second.Release)

	require.Equal(t, offsets[:2], first.OffsetTotal)
	require.Equal(t, offsets[2:], second.OffsetTotal)
}
