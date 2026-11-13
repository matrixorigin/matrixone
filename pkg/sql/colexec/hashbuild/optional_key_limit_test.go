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

package hashbuild

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Exercise the real producer: its input key is column 1, whereas its
// runtime-filter key is slot 0 in the collected equality keys.
func TestOptionalRuntimeFilterKeyByteLimitPublication(t *testing.T) {
	keys := make([]string, 128)
	for i := range keys {
		keys[i] = strings.Repeat("x", 505) + fmt.Sprintf("%07d", i)
	}
	overLimit := append(append([]string(nil), keys...), strings.Repeat("y", 512))
	for _, tc := range []struct {
		name     string
		values   []string
		hashOnPK bool
		want     int32
	}{
		{"boundary", []string{strings.Repeat("x", 512)}, false, message.RuntimeFilter_IN},
		{"over_boundary", []string{strings.Repeat("x", 513)}, false, message.RuntimeFilter_PASS},
		{"wide_external_value", []string{strings.Repeat("x", 1<<20)}, false, message.RuntimeFilter_PASS},
		{"full_count_and_width", keys, true, message.RuntimeFilter_IN},
		{"over_count", overLimit, true, message.RuntimeFilter_PASS},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := types.T_varchar.ToType()
			typ.Width = 64 // Actual external bytes can exceed this declaration.
			fixture := newTestCase(t, []types.Type{typ, typ}, []*plan.Expr{newExpr(1, typ)})
			fixture.arg.HashOnPK = tc.hashOnPK
			fixture.arg.RuntimeFilterSpec = rawRuntimeFilterSpec(fixture.arg.JoinMapTag+812, 128, typ)
			fixture.arg.OptionalRuntimeFilterMaxKeyBytes = 512
			fixture.arg.SetChildren([]vm.Operator{fixture.marg})
			require.NoError(t, fixture.marg.Prepare(fixture.proc))
			require.NoError(t, fixture.arg.Prepare(fixture.proc))
			input := batch.NewWithSize(2)
			other := make([]string, len(tc.values))
			for i := range other {
				other[i] = "unrelated"
			}
			input.Vecs[0] = testutil.MakeVarcharVector(other, nil, fixture.proc.Mp())
			input.Vecs[1] = testutil.MakeVarcharVector(tc.values, nil, fixture.proc.Mp())
			input.Vecs[0].GetType().Width = 64
			input.Vecs[1].GetType().Width = 64
			input.SetRowCount(len(tc.values))
			fixture.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(input, nil, fixture.proc.Mp())
			fixture.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, fixture.proc.Mp())
			result, err := vm.Exec(fixture.arg, fixture.proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecStop, result.Status)

			receiver := message.NewMessageReceiver([]int32{fixture.arg.RuntimeFilterSpec.Tag}, message.AddrBroadCastOnCurrentCN(), fixture.proc.GetMessageBoard())
			msgs, canceled, err := receiver.ReceiveMessage(false, fixture.proc.Ctx)
			require.NoError(t, err)
			require.False(t, canceled)
			require.Len(t, msgs, 1)
			filter := msgs[0].(message.RuntimeFilterMessage)
			require.Equal(t, tc.want, filter.Typ)
			if tc.want == message.RuntimeFilter_IN {
				require.Less(t, len(filter.Data), 128<<10)
				payload := vector.NewVec(types.T_any.ToType())
				require.NoError(t, payload.UnmarshalBinary(filter.Data))
				require.Equal(t, int32(len(tc.values)), filter.Card)
				got := make([]string, payload.Length())
				for i := range got {
					got[i] = payload.GetStringAt(i)
				}
				require.ElementsMatch(t, tc.values, got)
				payload.Free(fixture.proc.Mp())
			} else {
				require.Empty(t, filter.Data)
				require.Zero(t, fixture.arg.ctr.hashmapBuilder.uniqueKeys.count())
			}

			join, err := message.ReceiveJoinMapResult(fixture.arg.JoinMapTag, false, 0, fixture.proc.GetMessageBoard(), fixture.proc.Ctx)
			require.NoError(t, err)
			require.True(t, join.IsSuccess())
			require.Equal(t, int64(len(tc.values)), join.JoinMap().GetRowCount())
			join.JoinMap().Free()
			fixture.arg.Reset(fixture.proc, false, nil)
			fixture.arg.Free(fixture.proc, false, nil)
			fixture.marg.Reset(fixture.proc, false, nil)
			fixture.proc.GetMessageBoard().Reset()
			fixture.proc.Free()
			require.Zero(t, fixture.proc.Mp().CurrNB())
		})
	}
}

func TestOptionalRuntimeFilterKeyLimitBeforeCopy(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb := newTestHashmapBuilder(t)
	src := vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() {
		hb.curVecs = nil // The test owns the source.
		hb.Free(proc)
		src.Free(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	hb.optionalRuntimeFilterMaxKeyBytes = 512
	hb.optionalRuntimeFilterMaxKeys = 128
	hb.curVecs = []*vector.Vector{src}
	require.NoError(t, vector.AppendBytes(src, []byte(strings.Repeat("x", 1<<20)), false, proc.Mp()))
	require.NoError(t, vector.SetBytesAt(src, 0, []byte("short"), proc.Mp()))
	require.Greater(t, len(src.GetArea()), 1<<19)
	// A contiguous copy could copy the dead megabyte. The optional filter
	// must retain just the live value.
	require.NoError(t, hb.appendUniqueKeys(proc, 0, 1, nil))
	require.Equal(t, "short", hb.uniqueKeys.first.vecs[0].GetStringAt(0))
	require.Less(t, len(hb.uniqueKeys.first.vecs[0].GetArea()), 1024)
	for i := 0; i < 128; i++ {
		require.NoError(t, vector.AppendBytes(src, []byte("key"), false, proc.Mp()))
	}
	// The count bound applies before any new allocation even when a PK path
	// has duplicate or NULL values and its group count is still small.
	require.ErrorIs(t, hb.appendUniqueKeys(proc, 1, 128, nil), runtimefilter.ErrOptionalKeySize)
	require.Equal(t, 1, hb.uniqueKeys.count())
	require.NoError(t, hb.appendUniqueKeys(proc, 1, 127, nil))
	require.Equal(t, 128, hb.uniqueKeys.count())
	require.ErrorIs(t, hb.appendUniqueKeys(proc, 128, 1, nil), runtimefilter.ErrOptionalKeySize)
	require.Equal(t, 128, hb.uniqueKeys.count())
	// Reuse cannot carry this optional policy into another generation.
	hb.curVecs = nil
	hb.Reset(proc, true)
	require.Zero(t, hb.optionalRuntimeFilterMaxKeyBytes)
	require.Zero(t, hb.optionalRuntimeFilterMaxKeys)
	hb.optionalRuntimeFilterMaxKeyBytes = 1
	hb.optionalRuntimeFilterMaxKeys = 1
	hb.Free(proc)
	require.Zero(t, hb.optionalRuntimeFilterMaxKeyBytes)
	require.Zero(t, hb.optionalRuntimeFilterMaxKeys)
}

// A failed optional append must discard keys retained from earlier batches,
// while leaving the mandatory join-map owner untouched.
func TestOptionalRuntimeFilterKeyByteFallbackReleasesPriorKeys(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb := newTestHashmapBuilder(t)
	src := vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() {
		hb.curVecs = nil
		hb.Free(proc)
		src.Free(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	hb.optionalRuntimeFilterMaxKeyBytes = 512
	hb.optionalRuntimeFilterMaxKeys = 128
	hb.curVecs = []*vector.Vector{src}
	require.NoError(t, vector.AppendBytes(src, []byte("retained"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(src, []byte(strings.Repeat("x", 513)), false, proc.Mp()))
	require.NoError(t, hb.appendUniqueKeys(proc, 0, 1, nil))
	require.Greater(t, hb.mapAllocationAccount.Snapshot().Used, uint64(0))
	err := hb.appendUniqueKeys(proc, 1, 1, nil)
	require.ErrorIs(t, err, runtimefilter.ErrOptionalKeySize)
	require.Equal(t, 1, hb.uniqueKeys.count())
	require.NoError(t, hb.fallbackOptionalRuntimeFilterCollection(proc, err))
	require.True(t, hb.runtimeFilterCollectionFallback)
	require.Zero(t, hb.uniqueKeys.count())
	require.Zero(t, hb.mapAllocationAccount.Snapshot().Used)
}
