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

package readutil

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// A scan that reads many blocks into one reused output batch must hold cache
// pins for one block at a time: the batch is the lender of every borrowed
// column and the reader's reuse (CleanOnlyData) releases the previous block's
// pins before the next block is borrowed.
func TestReaderBorrowedPinsStayBoundedAcrossBlocks(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	writeMP := mpool.MustNewZero()
	t.Cleanup(func() { defer mpool.DeleteMPool(writeMP); require.Zero(t, writeMP.CurrNB()) })
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(writeMP) })
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType(), writeMP)
	input.Vecs[1] = vector.NewVec(types.T_text.ToType(), writeMP)
	payload := bytes.Repeat([]byte("borrowed-payload-"), 8)
	const rows = 64
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, writeMP))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], payload, false, writeMP))
	}
	input.SetRowCount(rows)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1}, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()

	tableDef := &plan.TableDef{
		Name:          "borrowed_reader_test",
		Name2ColIndex: map[string]int32{"id": 0, "payload": 1},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Cols: []*plan.ColDef{
			{Name: "id", Seqnum: 0, Primary: true, Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "payload", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_text)}},
		},
	}
	source := &singlePersistedBlockSource{info: stats.ConstructBlockInfo(0)}
	queryMP := mpool.MustNewZero()
	t.Cleanup(func() { defer mpool.DeleteMPool(queryMP); require.Zero(t, queryMP.CurrNB()) })
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.Open(1 << 30)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used, "every pin reservation is returned")
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := queryMP.WithAccount(account, mpool.AllocationOwnerSelection, 1, mpool.AllocationCapacityClassDefault)
	require.NoError(t, err)

	r, err := NewReader(ctx, queryMP, nil, fs, tableDef, timestamp.Timestamp{}, nil, source,
		GetThresholdForReader(1), engine.FilterHint{})
	require.NoError(t, err)
	mr := NewMergeReader([]engine.Reader{r})
	t.Cleanup(func() { require.NoError(t, mr.Close()) })

	output := batch.NewWithSize(2)
	output.Vecs[0] = vector.NewVec(types.T_int32.ToType(), allocation)
	output.Vecs[1] = vector.NewVec(types.T_text.ToType(), allocation)
	// The owned output column lives in allocation; the cached one is read-only.
	t.Cleanup(func() { output.Clean(allocation) })

	request := &engine.ReaderFilterRequest{
		OutputAllocation: allocation,
		// The scan is the lender of the retained column: borrowing is opt-in.
		ColumnUses: []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnRetain},
		Apply: func(*batch.Batch, []int) (engine.ReaderFilterResult, error) {
			t.Fatal("the selected read must not run Apply")
			return engine.ReaderFilterResult{}, nil
		},
		Select: func(bat *batch.Batch, _ []int) (engine.ReaderFilterResult, error) {
			return engine.ReaderFilterResult{All: true}, nil
		},
	}
	perBlockLeases, perBlockUsed := -1, uint64(0)
	const blocks = 6
	for block := 0; block < blocks; block++ {
		source.emitted = false
		isEnd, err := mr.ReadWithFilter(ctx, []string{"id", "payload"}, []int{0, 1}, request, queryMP, output)
		require.NoError(t, err)
		require.False(t, isEnd)
		require.Equal(t, rows, output.RowCount())
		require.True(t, output.Vecs[1].IsReadOnly(), "a retained column stays zero-copy from the cache")
		for row := 0; row < rows; row++ {
			require.Equal(t, int32(row), vector.GetFixedAtNoTypeCheck[int32](output.Vecs[0], row))
			require.Equal(t, payload, output.Vecs[1].GetBytesAt(row))
		}
		if perBlockLeases < 0 {
			perBlockLeases, perBlockUsed = heldPins(request), account.Snapshot().Used
			require.Positive(t, perBlockLeases)
			require.Positive(t, perBlockUsed, "the pinned capacity is charged to the scan's account")
			continue
		}
		require.Equal(t, perBlockLeases, heldPins(request), "block %d: pins must not accumulate", block)
		require.Equal(t, perBlockUsed, account.Snapshot().Used, "block %d: pin reservations must not accumulate", block)
	}
	// The scan, the lender, releases the pins once it is done with the batch.
	output.CleanOnlyData()
	request.ReleasePins()
	require.Zero(t, heldPins(request))
	require.Less(t, account.Snapshot().Used, perBlockUsed, "reuse returns the pin reservation; only owned scratch capacity stays")
}

func heldPins(request *engine.ReaderFilterRequest) int {
	return request.Pins().(*bufferlease.Holder).Len()
}
