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

package function

import (
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

type uuidShortClient struct {
	logservice.CNHAKeeperClient
	allocate func(context.Context, string, uint64) (uint64, error)
}

func (c *uuidShortClient) AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error) {
	return c.allocate(ctx, key, batch)
}

func TestUUIDShort(t *testing.T) {
	proc := testutil.NewProcess(t)
	registry, err := GetFunctionByName(proc.Ctx, "uuid_short", nil)
	require.NoError(t, err)
	require.Equal(t, types.T_uint64, registry.GetReturnType().Oid)
	_, err = GetFunctionByName(proc.Ctx, "uuid_short", []types.Type{types.T_int64.ToType()})
	require.Error(t, err)
	for _, tc := range []struct {
		name   string
		length int
		mask   *FunctionSelectList
		calls  int
	}{
		{"empty", 0, nil, 0}, {"all", 3, nil, 3},
		{"ignored", 3, &FunctionSelectList{AllNull: true, AnyNull: true}, 0},
		{"partial", 3, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(tc.length))
			calls := 0
			var child context.Context
			proc.Base.Hakeeper = &uuidShortClient{allocate: func(ctx context.Context, key string, batch uint64) (uint64, error) {
				require.Equal(t, uuidShortAllocationKey, key)
				require.Equal(t, uint64(objectio.BlockMaxRows), batch)
				_, ok := ctx.Deadline()
				require.True(t, ok)
				child = ctx
				calls++
				return math.MaxUint64 - uint64(calls-1), nil
			}}
			require.NoError(t, builtInUUIDShort(nil, result, proc, tc.length, tc.mask))
			require.Equal(t, tc.calls, calls)
			require.Equal(t, tc.length, result.GetResultVector().Length())
			if child != nil {
				require.ErrorIs(t, child.Err(), context.Canceled)
			}
			next := uint64(math.MaxUint64)
			for i := 0; i < tc.length; i++ {
				ignored := tc.mask.IgnoreAllRow() || (!tc.mask.ShouldEvalAllRow() && tc.mask.Contains(uint64(i)))
				require.Equal(t, ignored, nulls.Contains(result.GetResultVector().GetNulls(), uint64(i)))
				if !ignored {
					require.Equal(t, next, vector.GetFixedAtNoTypeCheck[uint64](result.GetResultVector(), i))
					next--
				}
			}
		})
	}
	injected := errors.New("allocator failed")
	for _, tc := range []struct {
		name   string
		client logservice.CNHAKeeperClient
		want   error
	}{
		{"nil", nil, nil},
		{"zero", &uuidShortClient{allocate: func(context.Context, string, uint64) (uint64, error) { return 0, nil }}, nil},
		{"failure", &uuidShortClient{allocate: func(context.Context, string, uint64) (uint64, error) { return 0, injected }}, injected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc.Base.Hakeeper = tc.client
			r := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), proc.Mp())
			defer r.Free()
			require.NoError(t, r.PreExtendAndReset(2))
			err := builtInUUIDShort(nil, r, proc, 2, nil)
			require.Error(t, err)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
			}
			// Ignored rows must not reach even a missing/failing allocator.
			require.NoError(t, builtInUUIDShort(nil, r, proc, 2, &FunctionSelectList{AllNull: true}))
		})
	}
	parent, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = parent
	proc.Base.Hakeeper = &uuidShortClient{allocate: func(ctx context.Context, _ string, _ uint64) (uint64, error) { return 0, ctx.Err() }}
	r := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), proc.Mp())
	defer r.Free()
	require.NoError(t, r.PreExtendAndReset(1))
	require.ErrorIs(t, builtInUUIDShort(nil, r, proc, 1, nil), context.Canceled)
}
