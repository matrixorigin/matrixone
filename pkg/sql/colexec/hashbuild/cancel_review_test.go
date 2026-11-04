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

package hashbuild

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type cancelOnBuildEOF struct {
	vm.Operator
	cancel context.CancelFunc
}

func (c *cancelOnBuildEOF) Call(proc *process.Process) (vm.CallResult, error) {
	r, err := c.Operator.Call(proc)
	if r.Batch == nil {
		c.cancel()
	}
	return r, err
}

func TestHashBuildCancelAtEOF(t *testing.T) {
	for _, tc := range []struct {
		name     string
		optional bool
	}{{"required", false}, {"optional", true}} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			proc.SetMessageBoard(message.NewMessageBoard())
			ctx, cancel := context.WithCancel(proc.Ctx)
			proc.Ctx = ctx
			defer cancel()
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
			bat.SetRowCount(2)
			child := &cancelOnBuildEOF{colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat}), cancel}
			b := &HashBuild{NeedHashMap: true, NeedBatches: true, OptionalIndex: tc.optional, Conditions: []*plan.Expr{newExpr(0, types.T_int32.ToType())}, JoinMapTag: 125, JoinMapRefCnt: 1}
			b.AppendChild(child)
			t.Cleanup(func() {
				b.Free(proc, true, context.Canceled)
				child.Free(proc, false, nil)
				proc.GetMessageBoard().Reset()
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			require.NoError(t, b.Prepare(proc))
			_, err := b.Call(proc)
			require.ErrorIs(t, err, context.Canceled)
			require.NotEqual(t, SendSucceed, b.ctr.state, "cancelled build cannot publish a join map")
			require.Nil(t, b.ctr.hashmapBuilder.IntHashMap, "EOF cancellation must precede index allocation")
		})
	}
}
