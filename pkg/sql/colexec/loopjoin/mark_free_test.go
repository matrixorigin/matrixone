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

package loopjoin

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMarkHashFreeWithoutReset(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		join := &LoopJoin{JoinType: plan.Node_MARK, NonEqCond: markEquality(t, types.T_int32, "=")}
		require.NoError(t, join.Prepare(proc))
		bat := makeInt32LoopJoinBatch(proc.Mp(), []int32{1})
		jm := markTestMap(t, proc, []*batch.Batch{bat})
		join.ctr.mp = jm
		if canceled {
			ctx, cancel := context.WithCancel(proc.Ctx)
			proc.Ctx = ctx
			cancel()
			require.ErrorIs(t, join.ctr.markHash.build(proc, jm), context.Canceled)
			cancel()
		} else {
			require.NoError(t, join.ctr.markHash.build(proc, jm))
		}
		join.Free(proc, canceled, nil)
		join.Free(proc, canceled, nil)
		require.Nil(t, join.ctr.mp)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}
}
