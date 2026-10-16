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
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
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

func TestOptionsTxnIsolation(t *testing.T) {
	var opts Options
	require.False(t, opts.HasTxnIsolation())

	opts = opts.WithTxnIsolation(txn.TxnIsolation_SI)
	require.True(t, opts.HasTxnIsolation())
	require.Equal(t, txn.TxnIsolation_SI, opts.TxnIsolation())
	require.Len(t, opts.ExtraTxnOptions(), 1)
}

func TestOptionsTxnMode(t *testing.T) {
	var opts Options
	require.False(t, opts.HasTxnMode())

	opts = opts.WithTxnMode(txn.TxnMode_Pessimistic)
	require.True(t, opts.HasTxnMode())
	require.Equal(t, txn.TxnMode_Pessimistic, opts.TxnMode())
	require.Len(t, opts.ExtraTxnOptions(), 1)
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

// WithOptimizerHints carries a per-statement optimizer_hints string (same format as the global
// variable). The internal SQL executor bridges it onto the execution context; the planner applies
// it. Value semantics: With* returns a new option and never mutates the receiver.
func TestStatementOptionOptimizerHints(t *testing.T) {
	var base StatementOption
	require.Equal(t, "", base.OptimizerHints(), "default is empty")

	set := base.WithOptimizerHints("applyIndices=1")
	require.Equal(t, "applyIndices=1", set.OptimizerHints())
	require.Equal(t, "", base.OptimizerHints(), "receiver must not be mutated")

	multi := base.WithOptimizerHints("applyIndices=1,aggPushDown=1")
	require.Equal(t, "applyIndices=1,aggPushDown=1", multi.OptimizerHints())

	// Overwriting replaces rather than appends.
	require.Equal(t, "x=1", set.WithOptimizerHints("x=1").OptimizerHints())
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
