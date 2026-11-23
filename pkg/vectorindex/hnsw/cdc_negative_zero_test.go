// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hnsw

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func assertCdcNegativeZeroInHnsw[T types.ArrayElement](t *testing.T, values []T, typ int32) {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	oldSQL, oldStreaming, oldTxn := runSql, runSql_streaming, runTxn
	t.Cleanup(func() { runSql, runSql_streaming, runTxn = oldSQL, oldStreaming, oldTxn })
	runSql = mock_runSql_empty
	runSql_streaming = mock_runSql_streaming
	runTxn = mock_runTxn
	source := vectorindex.NewVectorIndexCdc[T](4)
	source.Insert(42, values, nil)
	encoded, err := source.ToJson()
	require.NoError(t, err)
	var decoded vectorindex.VectorIndexCdc[T]
	require.NoError(t, decoded.UnmarshalJSON([]byte(encoded)))
	sync, err := NewHnswSync[T](sqlproc, "db", "src", "idx", mockMoIndexes(), typ, 3, "", nil)
	require.NoError(t, err)
	require.NoError(t, sync.Update(sqlproc, &decoded))
	require.NotEmpty(t, sync.indexes)
	found := false
	for _, m := range sync.indexes {
		if m.Index == nil {
			continue
		}
		defer m.Destroy()
		v, err := m.Index.Get(42, 1)
		require.NoError(t, err)
		if len(v) > 0 {
			found = true
			require.Equal(t, uint32(0x80000000), math.Float32bits(v[0]))
			require.Equal(t, float32(1), v[1])
			require.Equal(t, float32(2), v[2])
		}
	}
	require.True(t, found, "CDC item must be present in a native index")
}

func TestCdcNegativeZeroReachesHnswFloat32(t *testing.T) {
	assertCdcNegativeZeroInHnsw(t, []float32{float32(math.Copysign(0, -1)), 1, 2}, int32(types.T_array_float32))
}
func TestCdcNegativeZeroReachesHnswFloat64(t *testing.T) {
	assertCdcNegativeZeroInHnsw(t, []float64{math.Copysign(0, -1), 1, 2}, int32(types.T_array_float64))
}
