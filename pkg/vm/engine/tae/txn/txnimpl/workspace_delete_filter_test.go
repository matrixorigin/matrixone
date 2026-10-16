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

package txnimpl

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func workspaceDeleteFilterFixture(t testing.TB, probes, deletes int) (*txnTable, containers.Vector, containers.Vector) {
	t.Helper()
	keys := containers.MakeVector(objectio.RowidType, common.WorkspaceAllocator)
	t.Cleanup(keys.Close)
	workspace := containers.NewBatch()
	t.Cleanup(workspace.Close)
	deleted := containers.MakeVector(objectio.RowidType, common.WorkspaceAllocator)
	workspace.AddVector(objectio.TombstoneAttr_Rowid_Attr, deleted)
	for i := range probes {
		keys.Append(types.NewRowIDWithObjectIDBlkNumAndRowID(types.Objectid{}, 0, uint32(i)), false)
	}
	// Unsorted IDs with duplicates: workspace order is not a sort contract.
	for i := deletes - 1; i >= 0; i-- {
		var object types.Objectid
		object[0] = byte((i / 4) % 2)
		deleted.Append(types.NewRowIDWithObjectIDBlkNumAndRowID(object, uint16((i/2)%2), uint32(2*(i/2))), false)
	}
	table := &txnTable{tombstoneTable: &baseTable{tableSpace: &tableSpace{node: &anode{data: workspace}}}}
	return table, keys, deleted
}

func TestWorkspaceDeleteFilter(t *testing.T) {
	t.Run("all-null", func(t *testing.T) {
		table, keys, _ := workspaceDeleteFilterFixture(t, 65, 65)
		for i := range keys.Length() {
			keys.Update(i, nil, true)
		}
		require.NoError(t, table.contains(t.Context(), keys, index.ZM{}, common.WorkspaceAllocator))
		require.Equal(t, keys.Length(), keys.NullCount())
	})
	for _, probes := range []int{0, 1, 2, 32, 33, 34, 65} {
		for _, deletes := range []int{0, 1, 2, 32, 33, 34, 65} {
			t.Run(fmt.Sprintf("probes=%d/deletes=%d", probes, deletes), func(t *testing.T) {
				table, keys, workspace := workspaceDeleteFilterFixture(t, probes, deletes)
				original := append([]types.Rowid(nil), vector.MustFixedColWithTypeCheck[types.Rowid](keys.GetDownstreamVector())...)
				deleted := append([]types.Rowid(nil), vector.MustFixedColWithTypeCheck[types.Rowid](workspace.GetDownstreamVector())...)
				if probes > 3 {
					keys.Update(3, nil, true)
				}
				for range 2 { // A repeated filtering pass must be idempotent.
					require.NoError(t, table.contains(t.Context(), keys, index.ZM{}, common.WorkspaceAllocator))
					for i, rowid := range original {
						wantNull := i == 3
						for _, removed := range deleted {
							wantNull = wantNull || rowid == removed
						}
						require.Equal(t, wantNull, keys.IsNull(i), "candidate %d", i)
						if !wantNull {
							require.Equal(t, rowid, keys.Get(i))
						}
					}
					require.Equal(t, deleted, vector.MustFixedColWithTypeCheck[types.Rowid](workspace.GetDownstreamVector()))
				}
				if probes > 1 {
					// No index may survive a call: a later workspace append is visible.
					workspace.Append(original[1], false)
					require.NoError(t, table.contains(t.Context(), keys, index.ZM{}, common.WorkspaceAllocator))
					require.True(t, keys.IsNull(1))
				}
			})
		}
	}
}

func BenchmarkWorkspaceDeleteFilter(b *testing.B) {
	for _, size := range [][2]int{{1, 1}, {16, 16}, {32, 32}, {33, 33}, {128, 128}, {4096, 4096}, {1, 4096}, {4096, 1}, {33, 4096}, {4096, 33}} {
		b.Run(fmt.Sprintf("%dx%d", size[0], size[1]), func(b *testing.B) {
			table, keys, _ := workspaceDeleteFilterFixture(b, size[0], size[1])
			rowids := vector.MustFixedColWithTypeCheck[types.Rowid](keys.GetDownstreamVector())
			original := append([]types.Rowid(nil), rowids...)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				copy(rowids, original)
				keys.NullMask().Reset()
				if err := table.contains(context.Background(), keys, index.ZM{}, common.WorkspaceAllocator); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
