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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	currentRoleGrantQueryPrefix       = "SELECT cast(granted_id AS bigint) FROM mo_catalog.mo_role_grant WHERE grantee_id IN ("
	currentRoleGrantFrontierBatchSize = 256
)

type currentRoleSQLRunner func(*process.Process, string) (executor.Result, error)

type currentRoleFrontierExpander func(*process.Process, []int64, func(int64)) error

type currentRolesState struct {
	simpleOneBatchState
	expandFrontier currentRoleFrontierExpander
}

func currentRolesPrepare(_ *process.Process, _ *TableFunction) (tvfState, error) {
	return &currentRolesState{expandFrontier: expandCurrentRoleFrontier}, nil
}

func buildCurrentRoleGrantQuery(frontier []int64) string {
	var sql strings.Builder
	sql.Grow(len(currentRoleGrantQueryPrefix) + len(frontier)*12 + 1)
	sql.WriteString(currentRoleGrantQueryPrefix)
	for i, roleID := range frontier {
		if i > 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(strconv.FormatInt(roleID, 10))
	}
	sql.WriteByte(')')
	return sql.String()
}

func runCurrentRolesSQL(proc *process.Process, sql string) (executor.Result, error) {
	return sqlexec.RunSql(sqlexec.NewSqlProcess(proc), sql)
}

func visitCurrentRoleGrants(result executor.Result, visit func(int64)) {
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		grantedIDs := vector.MustFixedColWithTypeCheck[int64](cols[0])
		for i := 0; i < rows; i++ {
			visit(grantedIDs[i])
		}
		return true
	})
}

func expandCurrentRoleFrontierWithRunner(
	proc *process.Process,
	frontier []int64,
	visit func(int64),
	run currentRoleSQLRunner,
) error {
	for start := 0; start < len(frontier); start += currentRoleGrantFrontierBatchSize {
		end := min(start+currentRoleGrantFrontierBatchSize, len(frontier))
		result, err := run(proc, buildCurrentRoleGrantQuery(frontier[start:end]))
		if err != nil {
			return err
		}
		visitCurrentRoleGrants(result, visit)
		result.Close()
	}
	return nil
}

func expandCurrentRoleFrontier(proc *process.Process, frontier []int64, visit func(int64)) error {
	return expandCurrentRoleFrontierWithRunner(proc, frontier, visit, runCurrentRolesSQL)
}

func currentRoleClosure(
	proc *process.Process,
	root int64,
	expand currentRoleFrontierExpander,
) ([]int64, error) {
	visited := map[int64]struct{}{root: {}}
	frontier := []int64{root}
	for len(frontier) > 0 {
		next := make([]int64, 0)
		if err := expand(proc, frontier, func(grantedID int64) {
			if _, ok := visited[grantedID]; ok {
				return
			}
			visited[grantedID] = struct{}{}
			next = append(next, grantedID)
		}); err != nil {
			return nil, err
		}
		sort.Slice(next, func(i, j int) bool { return next[i] < next[j] })
		frontier = next
	}

	roles := make([]int64, 0, len(visited))
	for roleID := range visited {
		roles = append(roles, roleID)
	}
	sort.Slice(roles, func(i, j int) bool { return roles[i] < roles[j] })
	return roles, nil
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

	roles, err := currentRoleClosure(
		proc,
		int64(defines.GetRoleId(proc.Ctx)),
		s.expandFrontier,
	)
	if err != nil {
		return err
	}
	for _, roleID := range roles {
		if err := vector.AppendFixed(s.batch.Vecs[0], roleID, false, proc.Mp()); err != nil {
			return err
		}
	}
	s.batch.SetRowCount(len(roles))
	return nil
}
