// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_scan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// poolRecordingReader records whether the scan handed it an accounted view.
type poolRecordingReader struct {
	*localDomainTestReader
	sawView bool
}

func (r *poolRecordingReader) Read(ctx context.Context, cols []string, expr *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	r.sawView = r.sawView || mp.IsView()
	return r.localDomainTestReader.Read(ctx, cols, expr, mp, out)
}

// TestTableScanOutputLease pins the charging contract: the reader allocates
// through proc's pool (its internal buffers are never charged), and the
// emitted batch is charged by one capacity lease that is released before the
// batch is refilled and on Reset/Free, including after a refused charge.
func TestTableScanOutputLease(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limit  uint64
		end    string // how the live lease is retired: "next", "reset", "free"
		refuse bool
	}{
		{name: "next read releases", limit: 1 << 10, end: "next"},
		{name: "reset releases", limit: 1 << 10, end: "reset"},
		{name: "free releases", limit: 1 << 10, end: "free"},
		{name: "over capacity is refused", limit: 8, refuse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			registry, err := mpool.NewAllocationAccountRegistry(1, 1<<10)
			require.NoError(t, err)
			account, err := registry.Open(tc.limit)
			require.NoError(t, err)
			allocation, err := proc.Mp().WithAccount(account, mpool.AllocationOwnerSelection, 1, mpool.AllocationCapacityClassDefault)
			require.NoError(t, err)

			scan := NewArgument().WithTypes([]plan.Type{{Id: int32(types.T_int64)}})
			scan.Attrs = []string{"id"}
			scan.SetAnalyzeControl(0, true)
			scan.OutputAllocation = allocation
			reader := &poolRecordingReader{localDomainTestReader: &localDomainTestReader{values: []int64{1, 2, 3, 4}, nullAt: -1}}
			scan.Reader = reader
			freed := false
			t.Cleanup(func() {
				if !freed {
					scan.Free(proc, false, nil)
				}
				scan.closeReader()
				scan.Release()
				require.Zero(t, account.Snapshot().Used, "every lease must be released")
				_, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
			})
			require.NoError(t, scan.Prepare(proc))

			result, err := scan.Call(proc)
			require.False(t, reader.sawView, "the reader must allocate through proc's pool")
			if tc.refuse {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.Nil(t, scan.ctr.outputLease)
				require.Zero(t, account.Snapshot().Used)
				return
			}
			require.NoError(t, err)
			require.Same(t, scan.ctr.buf, result.Batch)
			require.Equal(t, 4, result.Batch.RowCount())
			require.Positive(t, scan.ctr.buf.Allocated())
			require.EqualValues(t, scan.ctr.buf.Allocated(), account.Snapshot().Used,
				"the emitted batch is charged once, by its retained capacity")

			switch tc.end {
			case "next":
				result, err = scan.Call(proc)
				require.NoError(t, err)
				require.Nil(t, result.Batch)
			case "reset":
				scan.Reset(proc, false, nil)
			case "free":
				scan.Free(proc, false, nil)
				freed = true
			}
			require.Nil(t, scan.ctr.outputLease)
			require.Zero(t, account.Snapshot().Used)
		})
	}
}
