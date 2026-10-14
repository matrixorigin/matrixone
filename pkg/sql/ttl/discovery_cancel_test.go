// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type canceledDiscoveryExecutor struct {
	executor.SQLExecutor
	result    executor.Result
	cancel    context.CancelFunc
	handedOff *bool
}

func (e *canceledDiscoveryExecutor) Exec(context.Context, string, executor.Options) (executor.Result, error) {
	if e.handedOff != nil {
		*e.handedOff = true
	}
	e.cancel() // Cancellation wins immediately before the completed result handoff.
	return e.result, nil
}

// Keep this probe compatible with the old Discover API/result shape so it can
// prove the review counterexample on the unmodified pre-fix implementation.
func TestTTLDiscoveryCanceledHandoff(t *testing.T) {
	for _, n := range []int{32, 64} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			p := Policy{Version: 1, Column: "ts", Seconds: 1}
			c := engine.ConstraintDef{Cts: []engine.Constraint{&engine.StreamConfigsDef{Configs: []*plan.Property{{Key: PropertyKey, Value: Encode(&p)}, {Key: "padding", Value: strings.Repeat("x", 4096)}}}}}
			data, err := c.MarshalBinary()
			require.NoError(t, err)
			m := executor.NewMemResult([]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint32.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_blob.ToType()}, mp)
			handedOff := false
			t.Cleanup(func() {
				if !handedOff {
					m.GetResult().Close()
				}
			})
			m.NewBatchWithRowCount(n)
			accounts, ids, versions := make([]uint32, n), make([]uint64, n), make([]uint32, n)
			dbs, tables, constraints := make([]string, n), make([]string, n), make([][]byte, n)
			for i := range ids {
				ids[i], dbs[i], tables[i], constraints[i] = uint64(i+1), "d", "t", data
			}
			require.NoError(t, executor.AppendFixedRows(m, 0, accounts))
			require.NoError(t, executor.AppendFixedRows(m, 1, ids))
			require.NoError(t, executor.AppendFixedRows(m, 2, versions))
			require.NoError(t, executor.AppendStringRows(m, 3, dbs))
			require.NoError(t, executor.AppendStringRows(m, 4, tables))
			require.NoError(t, executor.AppendBytesRows(m, 5, constraints))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			got, err := Discover(ctx, &canceledDiscoveryExecutor{result: m.GetResult(), cancel: cancel, handedOff: &handedOff})
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, got)
			require.Zero(t, mp.CurrNB(), "completed result must be released on cancellation")
		})
	}
}
