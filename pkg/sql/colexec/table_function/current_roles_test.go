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
	"errors"
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

func TestCurrentRoleClosure(t *testing.T) {
	edges := []roleGrantEdge{
		{grantedID: 20, granteeID: 10},
		{grantedID: 30, granteeID: 20},
		{grantedID: 40, granteeID: 30},
		{grantedID: 10, granteeID: 40}, // cycle
		{grantedID: 50, granteeID: 20},
		{grantedID: 99, granteeID: 98}, // disconnected
		{grantedID: 30, granteeID: 20}, // duplicate
	}

	require.Equal(t, []int64{10, 20, 30, 40, 50}, currentRoleClosure(10, edges))
	require.Equal(t, []int64{98, 99}, currentRoleClosure(98, edges))
	require.Equal(t, []int64{77}, currentRoleClosure(77, edges))
}

func TestDecodeCurrentRoleGrantEdges(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	for _, edge := range []roleGrantEdge{{20, 10}, {30, 20}} {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], edge.grantedID, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], edge.granteeID, false, mp))
	}
	bat.SetRowCount(2)
	result := executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
	defer result.Close()

	require.Equal(t, []roleGrantEdge{{20, 10}, {30, 20}}, decodeCurrentRoleGrantEdges(result))
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
		state.loadEdges = func(*process.Process) ([]roleGrantEdge, error) {
			return []roleGrantEdge{{20, 10}, {30, 20}, {10, 30}}, nil
		}

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
		state.loadEdges = func(*process.Process) ([]roleGrantEdge, error) { return nil, expected }
		require.ErrorIs(t, state.start(tf, proc, 0, nil), expected)
		tf.Free(proc, false, nil)
	})
}
