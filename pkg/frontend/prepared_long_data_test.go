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

package frontend

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestPreparedLongDataPoolFailureReleasesOwnedBuffers(t *testing.T) {
	pool, err := mpool.NewMPool("prepared-long-data-limit", 2<<20, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(pool)
	proc := testutil.NewProcessWithMPool(t, "", pool)
	stmt := &PrepareStmt{proc: proc}
	ctx := context.Background()

	require.NoError(t, stmt.appendLongData(ctx, proc, 0,
		bytes.Repeat([]byte{'a'}, 256<<10), 8<<20))
	require.Greater(t, pool.CurrNB(), int64(0))
	err = stmt.appendLongData(ctx, proc, 0,
		bytes.Repeat([]byte{'b'}, 2<<20), 8<<20)
	require.Error(t, err)
	require.Len(t, stmt.longDataBuffers[0], 256<<10,
		"failed growth must leave the old allocation owned by the statement")
	stmt.latchLongDataError(err)
	require.Zero(t, pool.CurrNB())
	require.True(t, stmt.hasPendingLongData())
	stmt.resetBinaryParamState()
	require.False(t, stmt.hasPendingLongData())
}
