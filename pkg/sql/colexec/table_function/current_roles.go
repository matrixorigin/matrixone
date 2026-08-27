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

package table_function

import (
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const currentRoleGrantQuery = "SELECT cast(granted_id AS bigint), cast(grantee_id AS bigint) FROM mo_catalog.mo_role_grant"

func runCurrentRolesSQL(proc *process.Process) (executor.Result, error) {
	return sqlexec.RunSql(sqlexec.NewSqlProcess(proc), currentRoleGrantQuery)
}

type roleGrantEdge struct {
	grantedID int64
	granteeID int64
}

type currentRolesState struct {
	simpleOneBatchState
	loadEdges func(*process.Process) ([]roleGrantEdge, error)
}

func currentRolesPrepare(_ *process.Process, _ *TableFunction) (tvfState, error) {
	return &currentRolesState{loadEdges: loadCurrentRoleGrantEdges}, nil
}

func currentRoleClosure(root int64, edges []roleGrantEdge) []int64 {
	grants := make(map[int64][]int64)
	for _, edge := range edges {
		grants[edge.granteeID] = append(grants[edge.granteeID], edge.grantedID)
	}

	visited := map[int64]struct{}{root: {}}
	queue := []int64{root}
	for len(queue) > 0 {
		roleID := queue[0]
		queue = queue[1:]
		for _, grantedID := range grants[roleID] {
			if _, ok := visited[grantedID]; ok {
				continue
			}
			visited[grantedID] = struct{}{}
			queue = append(queue, grantedID)
		}
	}

	roles := make([]int64, 0, len(visited))
	for roleID := range visited {
		roles = append(roles, roleID)
	}
	sort.Slice(roles, func(i, j int) bool { return roles[i] < roles[j] })
	return roles
}

func decodeCurrentRoleGrantEdges(result executor.Result) []roleGrantEdge {
	edges := make([]roleGrantEdge, 0)
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		grantedIDs := vector.MustFixedColWithTypeCheck[int64](cols[0])
		granteeIDs := vector.MustFixedColWithTypeCheck[int64](cols[1])
		for i := 0; i < rows; i++ {
			edges = append(edges, roleGrantEdge{
				grantedID: grantedIDs[i],
				granteeID: granteeIDs[i],
			})
		}
		return true
	})
	return edges
}

func loadCurrentRoleGrantEdges(proc *process.Process) ([]roleGrantEdge, error) {
	result, err := runCurrentRolesSQL(proc)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	return decodeCurrentRoleGrantEdges(result), nil
}

func (s *currentRolesState) start(
	tf *TableFunction,
	proc *process.Process,
	nthRow int,
	_ process.Analyzer,
) error {
	s.startPreamble(tf, proc, nthRow)
	if nthRow != 0 {
		s.batch.SetRowCount(0)
		return nil
	}

	edges, err := s.loadEdges(proc)
	if err != nil {
		return err
	}
	roles := currentRoleClosure(int64(defines.GetRoleId(proc.Ctx)), edges)
	for _, roleID := range roles {
		if err := vector.AppendFixed(s.batch.Vecs[0], roleID, false, proc.Mp()); err != nil {
			return err
		}
	}
	s.batch.SetRowCount(len(roles))
	return nil
}
