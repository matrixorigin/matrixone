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

package fulltext2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// TestBuildIncludeBuffers pins the box-free covering side channel: Search builds one
// nullable ColumnBuffer per FULL index INCLUDE column (segment order) from each result's
// positional Include values, into rt.IncludeBuffers.Cols — so the TVF emits the projected
// INCLUDE columns (mapped by segment position) with no base-table JOIN, no map[string][]any,
// and no per-row reflection. NULLs round-trip to SQL NULL.
func TestBuildIncludeBuffers(t *testing.T) {
	mp := mpool.MustNewZero()
	idx := incIdx(t) // include cols: [0]=status(varchar), [1]=prio(int64)
	s := &Fulltext2Search{idx: idx, cfg: TableConfig{IncludeColumns: []string{"status", "prio"}}}
	results := []Result{
		{Pk: int64(1), Score: 1, Include: []any{[]byte("active"), int64(10)}},
		{Pk: int64(2), Score: 1, Include: []any{nil, int64(20)}}, // NULL status
	}

	fr := &vectorindex.IncludeResult{}
	// The requested set only gates the no-op; buffers are built in FULL include order.
	rt := vectorindex.RuntimeConfig{RequestedIncludeColumns: []string{"prio", "status"}, IncludeBuffers: fr}
	s.buildIncludeBuffers(rt, results)
	require.Len(t, fr.Cols, 2)

	// Col 0 = status (varchar): ["active", NULL].
	statusVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vectorindex.AppendColumnBuffer(fr.Cols[0], statusVec, mp))
	require.Equal(t, "active", statusVec.GetStringAt(0))
	require.False(t, statusVec.IsNull(0))
	require.True(t, statusVec.IsNull(1)) // pk2 status is NULL

	// Col 1 = prio (int64): [10, 20], no NULLs.
	prioVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vectorindex.AppendColumnBuffer(fr.Cols[1], prioVec, mp))
	require.Equal(t, []int64{10, 20}, vector.MustFixedColWithTypeCheck[int64](prioVec))
	require.False(t, prioVec.IsNull(0))
	require.False(t, prioVec.IsNull(1))

	// AppendColumnBufferRange pages a subset: row [1,2) of prio = just 20.
	pageVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vectorindex.AppendColumnBufferRange(fr.Cols[1], pageVec, 1, 1, mp))
	require.Equal(t, []int64{20}, vector.MustFixedColWithTypeCheck[int64](pageVec))

	// No requested columns → no-op (no panic, IncludeBuffers untouched).
	empty := &vectorindex.IncludeResult{}
	s.buildIncludeBuffers(vectorindex.RuntimeConfig{IncludeBuffers: empty}, results)
	require.Nil(t, empty.Cols)
}
