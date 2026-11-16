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

package table_scan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDictionaryFrameOwnerReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(nil))
	input := batch.NewWithSize(1)
	t.Cleanup(func() { input.Clean(mp) })
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(7), false, mp))
	input.SetRowCount(1)

	// A never-used zero-value owner has no optional frame to clean.
	var zero TableScan
	zero.Free(proc, false, nil)
	require.Nil(t, zero.ctr.dictionaryInputs)

	owner := newTableScanArgument()
	t.Cleanup(func() { owner.Free(proc, false, nil) })
	frame := owner.ctr.dictionaryInputs
	require.NotNil(t, frame)
	other := newTableScanArgument()
	require.NotSame(t, frame, other.ctr.dictionaryInputs)

	for _, failure := range []error{nil, context.Canceled} {
		owner.TopValueMsgTag = 17
		frame.Begin(input, nil, mp)
		frame.Gathers = 1
		frame.End()
		owner.Free(proc, failure != nil, failure)
		require.Equal(t, colexec.DictionaryInputFrame{}, *frame)
		require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](input.Vecs[0], 0), "the borrowed input remains source-owned")

		// Exercise the exact callback registered with the existing pool,
		// without depending on sync.Pool scheduling or a later GC.
		resetTableScanArgument(owner)
		require.Same(t, frame, owner.ctr.dictionaryInputs)
		expected := TableScan{}
		expected.ctr.dictionaryInputs = frame
		require.Equal(t, expected, *owner)
	}
}
