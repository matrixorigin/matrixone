// Copyright 2025 Matrix Origin
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

package frontend

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestAsyncSQLExecutorSuccess(t *testing.T) {
	exec, err := newAsyncSQLExecutor(2)
	require.NoError(t, err)
	defer exec.Close()

	var mu sync.Mutex
	executed := make([]int, 0, 3)

	for i := 0; i < 3; i++ {
		idx := i
		require.NoError(t, exec.Submit(func(context.Context) error {
			mu.Lock()
			executed = append(executed, idx)
			mu.Unlock()
			return nil
		}))
	}

	require.NoError(t, exec.Wait())
	require.Len(t, executed, 3)
}

func TestAsyncSQLExecutorPropagatesError(t *testing.T) {
	exec, err := newAsyncSQLExecutor(1)
	require.NoError(t, err)
	defer exec.Close()

	sentinel := moerr.NewInternalErrorNoCtx("boom")

	require.NoError(t, exec.Submit(func(context.Context) error {
		return sentinel
	}))

	require.ErrorIs(t, exec.Wait(), sentinel)
	require.ErrorIs(t, exec.Submit(func(context.Context) error { return nil }), sentinel)
}

func TestRowsCollectorSnapshot(t *testing.T) {
	rc := &rowsCollector{}
	rc.addRow([]any{1})
	rc.addRows([][]any{{2}, {3}})

	snapshot := rc.snapshot()
	require.Len(t, snapshot, 3)

	snapshot = append(snapshot, []any{4})
	require.Len(t, rc.rows, 3)

	snapshot[0] = []any{100}
	require.Equal(t, []any{1}, rc.rows[0])

	snapshot[1][0] = 200
	require.Equal(t, 200, rc.rows[1][0])
}

func TestBufferPool(t *testing.T) {
	buf := acquireBuffer()
	buf.WriteString("data")
	releaseBuffer(buf)

	buf2 := acquireBuffer()
	require.Equal(t, 0, buf2.Len())
	releaseBuffer(buf2)
}

func TestCompareRows(t *testing.T) {
	colTypes := []types.Type{
		{Oid: types.T_int32},
	}
	row1 := []any{nil, diffAddedLine, int32(1)}
	row2 := []any{nil, diffRemovedLine, int32(2)}

	expectedCompare := types.CompareValues(row1[2], row2[2], types.T_int32)
	require.Equal(t, expectedCompare, compareRows(row1, row2, []int{0}, colTypes, false))

	row3 := []any{nil, diffAddedLine, int32(1)}
	require.Equal(t, 0, compareRows(row1, row3, []int{0}, colTypes, true))
	rowEqual := []any{nil, diffRemovedLine, int32(1)}
	require.Equal(t, strings.Compare(rowEqual[1].(string), row1[1].(string)),
		compareRows(row1, rowEqual, []int{0}, colTypes, false))
}
