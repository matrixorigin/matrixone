// Copyright 2023 Matrix Origin
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

package executor

import (
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/stretchr/testify/require"
)

func TestOptionsStreaming(t *testing.T) {
	var opts Options
	var ch chan Result
	var errors chan error

	opts = opts.WithStreaming(ch, errors)
	ret, err_chan, streaming := opts.Streaming()
	require.True(t, ret == ch)
	require.True(t, streaming)
	require.True(t, err_chan == errors)
}

func TestOptionsCNLabelsUseIndependentSnapshots(t *testing.T) {
	labels := map[string]string{"account": "tp", "role": "tp"}
	opts := Options{}.WithCNLabels(labels)

	labels["role"] = "ap"
	first := opts.CNLabels()
	require.Equal(t, map[string]string{"account": "tp", "role": "tp"}, first)

	first["account"] = "other"
	require.Equal(t, "tp", opts.CNLabels()["account"])
	require.Nil(t, (Options{}).CNLabels())
	require.NotNil(t, (Options{}).WithCNLabels(map[string]string{}).CNLabels())
}

func TestOptionsQuerySchedulingIntent(t *testing.T) {
	intent := schedule.SchedulingIntent{
		Explicit:          true,
		RequestedPool:     "account:tp",
		PoolFallback:      schedule.PoolFallbackStrict,
		EmptyWorkerPolicy: schedule.EmptyWorkerFail,
		CurrentCNPolicy:   schedule.CurrentCNPreferred,
		WorkerSet: schedule.WorkerSetPolicy{
			Mode:             schedule.WorkerSetMax,
			MaxWorkers:       2,
			SelectionKey:     "statement-id",
			AlgorithmVersion: schedule.WorkerSelectionAlgorithmV1,
		},
	}
	require.Equal(t, intent, (Options{}).WithQuerySchedulingIntent(intent).QuerySchedulingIntent())
	require.Equal(t, schedule.SchedulingIntent{}, (Options{}).QuerySchedulingIntent())
}

func TestOptionsLockWaitTimeout(t *testing.T) {
	var opts Options
	require.False(t, opts.HasLockWaitTimeout())

	opts = opts.WithLockWaitTimeout(1500 * time.Millisecond)
	require.True(t, opts.HasLockWaitTimeout())
	require.Equal(t, 1500*time.Millisecond, opts.LockWaitTimeout())
	require.Len(t, opts.ExtraTxnOptions(), 1)

	opts = opts.WithLockWaitTimeout(0)
	require.True(t, opts.HasLockWaitTimeout())
	require.Zero(t, opts.LockWaitTimeout())
	require.Len(t, opts.ExtraTxnOptions(), 2)
}

func TestStatementOptionParamsPreserveNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	vec, err := StatementOption{}.
		WithParamsAndNulls([]string{"7", "", "value", ""}, []bool{false, true, false, false}).
		Params(mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	require.Equal(t, 4, vec.Length())
	require.Equal(t, []byte("7"), vec.GetRawBytesAt(0))
	require.True(t, vec.IsNull(1))
	require.Equal(t, []byte("value"), vec.GetRawBytesAt(2))
	require.False(t, vec.IsNull(3))
	require.Empty(t, vec.GetRawBytesAt(3))
}
func TestStatementOptionSessionIdentity(t *testing.T) {
	var base StatementOption
	account, role := base.SessionIdentity()
	require.Empty(t, account)
	require.Empty(t, role)

	set := base.WithSessionIdentity("sys", "moadmin")
	account, role = set.SessionIdentity()
	require.Equal(t, "sys", account)
	require.Equal(t, "moadmin", role)
	account, role = base.SessionIdentity()
	require.Empty(t, account)
	require.Empty(t, role)
}

func TestStatementOptionParamsRejectsInvalidBitmap(t *testing.T) {
	mp := mpool.MustNewZero()
	vec, err := StatementOption{}.WithParamsAndNulls([]string{"a", "b"}, []bool{false}).Params(mp)
	require.Error(t, err)
	require.Nil(t, vec)
	require.Zero(t, mp.CurrNB())
}

func TestStatementOptionParamsAllocationFailure(t *testing.T) {
	// MPool caps govern off-heap admission; parameter vectors use on-heap
	// allocations. Exercise their allocator size limit without exhausting RAM.
	previous := mpool.CapLimit
	mpool.CapLimit = 1 << 20
	t.Cleanup(func() { mpool.CapLimit = previous })
	mp := mpool.MustNewZero()
	vec, err := StatementOption{}.WithParams([]string{strings.Repeat("a", 32), strings.Repeat("x", 2<<20)}).Params(mp)
	if vec != nil {
		defer vec.Free(mp)
	}
	require.Error(t, err)
	require.Nil(t, vec)
	require.Zero(t, mp.CurrNB())
	require.Zero(t, mp.OnHeapCurrNB(), "partial parameter allocations must be freed")
}

func TestStatementOptionParamsVectorAllocationFailure(t *testing.T) {
	previous := mpool.CapLimit
	mpool.CapLimit = 64
	t.Cleanup(func() { mpool.CapLimit = previous })
	mp := mpool.MustNewZero()
	v, err := StatementOption{}.WithParams([]string{"first", "second", "third"}).Params(mp)
	if v != nil {
		defer v.Free(mp)
	}
	require.Error(t, err)
	require.Nil(t, v)
	require.Zero(t, mp.OnHeapCurrNB())
}
