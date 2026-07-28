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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

func TestStatementOptionParamsPreserveNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := StatementOption{}.
		WithParamsAndNulls([]string{"7", "", "value"}, []bool{false, true, false}).
		Params(mp)
	defer vec.Free(mp)

	require.Equal(t, 3, vec.Length())
	require.Equal(t, []byte("7"), vec.GetRawBytesAt(0))
	require.True(t, vec.IsNull(1))
	require.Equal(t, []byte("value"), vec.GetRawBytesAt(2))
}
