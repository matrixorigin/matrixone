//go:build gpu

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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// A parallel cagra_create / ivfpq_create instance that received no rows has no
// builder; its end persists nothing.
func TestCreateEndWithoutRows(t *testing.T) {
	require.NotPanics(t, func() {
		require.NoError(t, (&cagraCreateState{}).end(nil, nil))
	})
	require.NotPanics(t, func() {
		require.NoError(t, (&ivfpqCreateState{}).end(nil, nil))
	})
}

func TestGPUCreateResetEndDoesNotReuseAdmission(t *testing.T) {
	cagra := &cagraCreateState{policy: &executor.IndexRelationPolicy{}}
	cagra.reset(nil, nil)
	require.NoError(t, cagra.end(nil, nil))
	ivfpq := &ivfpqCreateState{policy: &executor.IndexRelationPolicy{}}
	ivfpq.reset(nil, nil)
	require.NoError(t, ivfpq.end(nil, nil))
}

func TestGPUCreateReadmitsConfigAfterReset(t *testing.T) {
	cagra, ivfpq := &cagraCreateState{}, &ivfpqCreateState{}
	for _, tc := range []struct {
		name   string
		state  tvfState
		policy **executor.IndexRelationPolicy
	}{
		{"cagra_create", cagra, &cagra.policy},
		{"ivfpq_create", ivfpq, &ivfpq.policy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			lookups := installOrdinaryMaintenanceRelations(t, proc)
			oldCagra, oldIvfpq := cagra_runSql, ivfpq_runSql
			t.Cleanup(func() { cagra_runSql, ivfpq_runSql = oldCagra, oldIvfpq })
			emptySource := func(*sqlexec.SqlProcess, string) (executor.Result, error) { return int64Result(t, mp, 0), nil }
			cagra_runSql, ivfpq_runSql = emptySource, emptySource
			config, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(`{"db":"db","src":"src","metadata":"meta","index":"store","part":"v"}`), 1, mp)
			require.NoError(t, err)
			defer config.Free(mp)
			tf := &TableFunction{FuncName: tc.name, Params: []byte(`{"op_type":"vector_l2_ops"}`)}
			tf.ctr.argVecs = []*vector.Vector{config}
			defer tc.state.free(tf, proc, false, nil)
			require.NoError(t, tc.state.start(tf, proc, 0, nil))
			checkVectorMaintenanceReuse(t, tf, proc, tc.state, lookups, tc.policy)
		})
	}
}
