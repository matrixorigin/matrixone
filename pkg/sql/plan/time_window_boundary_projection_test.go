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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestTimeWindowRepeatedBoundaryProjectionKeepsType(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.tables["bind_select"].Cols[0].Typ = planpb.Type{
		Id:          int32(types.T_timestamp),
		Scale:       3,
		NotNullable: true,
	}

	p, err := runOneStmt(mock, t, `select _wstart, _wend, max(b), _wstart
		from select_test.bind_select
		interval(a, 10, minute) sliding(5, minute) fill(linear)`)
	require.NoError(t, err)
	require.NotEmpty(t, p.GetQuery().Nodes)

	root := p.GetQuery().Nodes[len(p.GetQuery().Nodes)-1]
	require.Equal(t, planpb.Node_PROJECT, root.NodeType)
	require.Len(t, root.ProjectList, 4)
	for _, idx := range []int{0, 1, 3} {
		require.Equal(t, root.ProjectList[0].Typ.Id, root.ProjectList[idx].Typ.Id, "boundary %d", idx)
		require.Equal(t, root.ProjectList[0].Typ.Scale, root.ProjectList[idx].Typ.Scale, "boundary %d", idx)
	}
	require.Equal(t, int32(types.T_timestamp), root.ProjectList[0].Typ.Id)
	require.Equal(t, int32(3), root.ProjectList[0].Typ.Scale)
}
