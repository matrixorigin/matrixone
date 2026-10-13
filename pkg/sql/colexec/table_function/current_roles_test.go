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
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func roleGraphExpander(graph map[int64][]int64, expanded *[]int64) currentRoleFrontierExpander {
	return func(_ *process.Process, frontier []int64, visit func(int64) error) error {
		for _, roleID := range frontier {
			if expanded != nil {
				*expanded = append(*expanded, roleID)
			}
			for _, grantedID := range graph[roleID] {
				if err := visit(grantedID); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func TestCurrentRoleClosure(t *testing.T) {
	graph := map[int64][]int64{
		10: {20},
		20: {30, 50, 30}, // duplicate
		30: {40},
		40: {10}, // cycle
		98: {99}, // disconnected
	}

	roles, err := currentRoleClosure(nil, 10, roleGraphExpander(graph, nil))
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20, 30, 40, 50}, roles)

	roles, err = currentRoleClosure(nil, 98, roleGraphExpander(graph, nil))
	require.NoError(t, err)
	require.Equal(t, []int64{98, 99}, roles)

	roles, err = currentRoleClosure(nil, 77, roleGraphExpander(graph, nil))
	require.NoError(t, err)
	require.Equal(t, []int64{77}, roles)
}

func TestCurrentRoleClosureAdmissionBoundary(t *testing.T) {
	chainExpander := func(limit int64) currentRoleFrontierExpander {
		return func(_ *process.Process, frontier []int64, visit func(int64) error) error {
			for _, roleID := range frontier {
				if roleID+1 < limit {
					if err := visit(roleID + 1); err != nil {
						return err
					}
				}
			}
			return nil
		}
	}

	roles, err := currentRoleClosure(nil, 0, chainExpander(currentRoleClosureMaxRoles))
	require.NoError(t, err)
	require.Len(t, roles, currentRoleClosureMaxRoles)

	roles, err = currentRoleClosure(nil, 0, chainExpander(currentRoleClosureMaxRoles+1))
	require.ErrorContains(t, err, "current role closure exceeds the 4096-role query limit")
	require.Nil(t, roles, "an over-limit closure must not publish a partial role set")
}

func TestCurrentRoleClosureCancellation(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	cancel()
	queries := 0
	expand := func(proc *process.Process, frontier []int64, visit func(int64) error) error {
		queries++
		return expandCurrentRoleFrontierWithRunner(proc, frontier, visit,
			func(*process.Process, string) (executor.Result, error) {
				t.Fatal("cancellation must be checked before internal SQL")
				return executor.Result{}, nil
			})
	}
	roles, err := currentRoleClosure(proc, 10, expand)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, roles)
	require.Zero(t, queries)

	proc = testutil.NewProc(t)
	ctx, cancel = context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	calls := 0
	expand = func(proc *process.Process, _ []int64, visit func(int64) error) error {
		calls++
		if calls == 1 {
			require.NoError(t, visit(20))
			cancel()
			return nil
		}
		t.Fatal("cancellation between frontiers must prevent another expansion")
		return nil
	}
	roles, err = currentRoleClosure(proc, 10, expand)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, roles)
	require.Equal(t, 1, calls)
}

func TestCurrentRoleClosureCapacityRejectionClosesResult(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	run := func(_ *process.Process, _ string) (executor.Result, error) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		for roleID := int64(1); roleID <= currentRoleClosureMaxRoles; roleID++ {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], roleID, false, mp))
		}
		bat.SetRowCount(currentRoleClosureMaxRoles)
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	expand := func(proc *process.Process, frontier []int64, visit func(int64) error) error {
		return expandCurrentRoleFrontierWithRunner(proc, frontier, visit, run)
	}

	roles, err := currentRoleClosure(nil, 0, expand)
	require.ErrorContains(t, err, "4096-role query limit")
	require.Nil(t, roles)
	require.Zero(t, mp.CurrNB(), "capacity rejection must close the current internal executor result")
}

func TestCurrentRoleClosureConcurrentAdmissionGenerations(t *testing.T) {
	const workers = 8
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(workers)
	var done sync.WaitGroup
	done.Add(workers)
	errs := make(chan error, workers)

	for worker := 0; worker < workers; worker++ {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			expand := func(_ *process.Process, frontier []int64, visit func(int64) error) error {
				for _, roleID := range frontier {
					if roleID < currentRoleClosureMaxRoles {
						if err := visit(roleID + 1); err != nil {
							return err
						}
					}
				}
				return nil
			}
			roles, err := currentRoleClosure(nil, 0, expand)
			if roles != nil && err != nil {
				errs <- errors.New("over-limit closure published partial roles")
				return
			}
			errs <- err
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	close(errs)
	for err := range errs {
		require.ErrorContains(t, err, "4096-role query limit")
	}
}

func TestCurrentRoleClosureDoesNotVisitLargeDisconnectedGraph(t *testing.T) {
	graph := make(map[int64]int64, 100_002)
	graph[10] = 20
	graph[20] = 30
	for i := int64(0); i < 100_000; i++ {
		graph[1_000_000+i] = 2_000_000 + i
	}

	lookups := 0
	expand := func(_ *process.Process, frontier []int64, visit func(int64) error) error {
		for _, roleID := range frontier {
			lookups++
			if grantedID, ok := graph[roleID]; ok {
				if err := visit(grantedID); err != nil {
					return err
				}
			}
		}
		return nil
	}
	roles, err := currentRoleClosure(nil, 10, expand)
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20, 30}, roles)
	require.Equal(t, 3, lookups)

	allocs := testing.AllocsPerRun(100, func() {
		_, closureErr := currentRoleClosure(nil, 10, expand)
		if closureErr != nil {
			panic(closureErr)
		}
	})
	require.Less(t, allocs, float64(100))
}

func BenchmarkCurrentRoleClosureLargeDisconnectedGraph(b *testing.B) {
	graph := make(map[int64]int64, 100_002)
	graph[10] = 20
	graph[20] = 30
	for i := int64(0); i < 100_000; i++ {
		graph[1_000_000+i] = 2_000_000 + i
	}
	expand := func(_ *process.Process, frontier []int64, visit func(int64) error) error {
		for _, roleID := range frontier {
			if grantedID, ok := graph[roleID]; ok {
				if err := visit(grantedID); err != nil {
					return err
				}
			}
		}
		return nil
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := currentRoleClosure(nil, 10, expand); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCurrentRoleClosureInternalSQLBoundary(b *testing.B) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	run := func(_ *process.Process, sql string) (executor.Result, error) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		var grantedID int64
		switch {
		case strings.HasSuffix(sql, "(10)"):
			grantedID = 20
		case strings.HasSuffix(sql, "(20)"):
			grantedID = 30
		case strings.HasSuffix(sql, "(30)"):
		default:
			return executor.Result{}, errors.New("unexpected role-grant query")
		}
		if grantedID != 0 {
			if err := vector.AppendFixed(bat.Vecs[0], grantedID, false, mp); err != nil {
				return executor.Result{}, err
			}
			bat.SetRowCount(1)
		}
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	expand := func(proc *process.Process, frontier []int64, visit func(int64) error) error {
		return expandCurrentRoleFrontierWithRunner(proc, frontier, visit, run)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roles, err := currentRoleClosure(nil, 10, expand)
		if err != nil {
			b.Fatal(err)
		}
		if len(roles) != 3 {
			b.Fatalf("expected three roles, got %d", len(roles))
		}
	}
}

func TestBuildCurrentRoleGrantQuery(t *testing.T) {
	require.Equal(t,
		"SELECT cast(granted_id AS bigint) FROM mo_catalog.mo_role_grant WHERE grantee_id IN (10,20,30)",
		buildCurrentRoleGrantQuery([]int64{10, 20, 30}),
	)
}

func TestVisitCurrentRoleGrants(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for _, roleID := range []int64{20, 30} {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], roleID, false, mp))
	}
	bat.SetRowCount(2)
	result := executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
	defer result.Close()

	var roles []int64
	require.NoError(t, visitCurrentRoleGrants(result, func(roleID int64) error {
		roles = append(roles, roleID)
		return nil
	}))
	require.Equal(t, []int64{20, 30}, roles)
}

func TestExpandCurrentRoleFrontierChunksQueries(t *testing.T) {
	frontier := make([]int64, currentRoleGrantFrontierBatchSize+2)
	for i := range frontier {
		frontier[i] = int64(i + 1)
	}
	var queries []string
	run := func(_ *process.Process, sql string) (executor.Result, error) {
		queries = append(queries, sql)
		return executor.Result{}, nil
	}

	require.NoError(t, expandCurrentRoleFrontierWithRunner(nil, frontier, func(int64) error { return nil }, run))
	require.Len(t, queries, 2)
	require.True(t, strings.HasSuffix(queries[0], ",255,256)"))
	require.Equal(t, currentRoleGrantQueryPrefix+"257,258)", queries[1])
	for _, query := range queries {
		require.Contains(t, query, "WHERE grantee_id IN (")
		require.NotContains(t, query, "grantee_id AS bigint")
	}
}

func TestCurrentRolesState(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		proc := testutil.NewProc(t)
		proc.Ctx = defines.AttachRoleId(proc.Ctx, 10)
		tf := &TableFunction{
			FuncName: "mo_current_roles",
			Attrs:    []string{"role_id"},
			Rets: []*planpb.ColDef{{
				Name: "role_id",
				Typ:  planpb.Type{Id: int32(types.T_int64)},
			}},
			OperatorBase: vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
		}
		require.NoError(t, tf.Prepare(proc))
		state := tf.ctr.state.(*currentRolesState)
		state.expandFrontier = roleGraphExpander(map[int64][]int64{
			10: {20},
			20: {30},
			30: {10},
		}, nil)

		require.NoError(t, state.start(tf, proc, 0, nil))
		require.Equal(t, []int64{10, 20, 30}, vector.MustFixedColWithTypeCheck[int64](state.batch.Vecs[0]))
		result, err := state.call(tf, proc)
		require.NoError(t, err)
		require.Equal(t, 3, result.Batch.RowCount())
		_, err = state.call(tf, proc)
		require.NoError(t, err)

		require.NoError(t, state.start(tf, proc, 1, nil))
		require.Zero(t, state.batch.RowCount())

		expected := errors.New("role grant read failed")
		state.expandFrontier = func(*process.Process, []int64, func(int64) error) error { return expected }
		require.ErrorIs(t, state.start(tf, proc, 0, nil), expected)
		require.Zero(t, state.batch.RowCount(), "a failed closure must not publish a partial batch")
		tf.Free(proc, false, nil)
	})
}
