// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCopyBackupResultRowsSurvivesResultClose(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	var value types.Varlena
	value[0] = 3
	copy(value[1:], "abc")
	data := types.EncodeSlice([]types.Varlena{value})
	releases := 0
	lease, err := vector.NewRefCountedBufferLease(data, int64(cap(data)), func() { releases++; clear(data) })
	require.NoError(t, err)
	releaseLease := sync.OnceFunc(lease.Release)
	t.Cleanup(releaseLease)
	vec := vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	t.Cleanup(func() { vec.Free(mp) })
	require.NoError(t, vec.InstallBorrowedData(data, lease))
	releaseLease()
	vec.SetLength(1)
	bat := batch.NewOffHeap([]string{"value"})
	bat.Vecs[0] = vec
	bat.SetRowCount(1)
	res := executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}
	rows := copyBackupResultRows(res)
	res.Close()
	require.Equal(t, 1, releases)
	require.Equal(t, []byte("abc"), rows[0][0])
}
