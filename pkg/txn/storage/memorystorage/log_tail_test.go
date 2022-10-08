// Copyright 2022 Matrix Origin
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

package memorystorage

import (
	"context"
	"math"
	"testing"
	"time"

	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/stretchr/testify/assert"
)

func TestLogTail(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	clock := clock.NewHLCClock(func() int64 {
		return time.Now().Unix()
	}, math.MaxInt)
	storage, err := NewMemoryStorage(
		testutil.NewMheap(),
		SnapshotIsolation,
		clock,
		memoryengine.RandomIDGenerator,
	)
	assert.Nil(t, err)
	defer storage.Close(ctx)

	// txn
	txnMeta := txn.TxnMeta{
		ID:     []byte("1"),
		Status: txn.TxnStatus_Active,
		SnapshotTS: timestamp.Timestamp{
			PhysicalTime: 1,
			LogicalTime:  1,
		},
	}

	// test get log tail
	{
		for _, tableID := range []uint64{1, 2, 3} {
			resp, err := testRead[apipb.SyncLogTailResp](
				ctx, t, storage, txnMeta,
				memoryengine.OpGetLogTail,
				apipb.SyncLogTailReq{
					Table: &apipb.TableID{
						TbId: tableID,
					},
				},
			)
			assert.Nil(t, err)
			assert.Equal(t, 1, len(resp.Commands))
			cmd := resp.Commands[0]
			assert.Equal(t, apipb.Entry_Insert, cmd.EntryType)
			assert.Equal(t, tableID, cmd.TableId)
			assert.True(t, len(cmd.Bat.Attrs) > 0)
			assert.True(t, len(cmd.Bat.Vecs) > 0)
		}
	}
}
