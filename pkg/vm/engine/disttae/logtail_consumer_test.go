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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

// should ensure that subscribe and unsubscribe methods are effective.
func TestSubscribedTable(t *testing.T) {
	var subscribeRecord subscribedTable

	subscribeRecord.m = make(map[uint64]SubTableStatus)
	subscribeRecord.eng = &Engine{
		partitions: make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{
			logtailUpdate: newLogtailUpdate(),
		},
	}
	require.Equal(t, 0, len(subscribeRecord.m))

	tbls := []struct {
		db uint64
		tb uint64
	}{
		{db: 0, tb: 1},
		{db: 0, tb: 2},
		{db: 0, tb: 3},
		{db: 0, tb: 4},
		{db: 0, tb: 1},
	}
	for _, tbl := range tbls {
		subscribeRecord.setTableSubscribed(tbl.db, tbl.tb)
		_ = subscribeRecord.eng.GetOrCreateLatestPart(tbl.db, tbl.tb)
	}
	require.Equal(t, 4, len(subscribeRecord.m))
	require.Equal(t, true, subscribeRecord.isSubscribed(tbls[0].db, tbls[0].tb))
	for _, tbl := range tbls {
		subscribeRecord.setTableUnsubscribe(tbl.db, tbl.tb)
	}
	require.Equal(t, 0, len(subscribeRecord.m))
}

func TestBlockInfoSlice(t *testing.T) {
	var data []byte
	s := string(data)
	cnt := len(s)
	require.Equal(t, 0, cnt)

	data1 := data[:0]
	cnt = len(data1)
	require.Equal(t, 0, cnt)

	blkSlice := objectio.BlockInfoSlice(data)
	require.Equal(t, 0, len(blkSlice))
	cnt = blkSlice.Len()
	require.Equal(t, 0, cnt)

	data = []byte{1, 2, 3, 4, 5, 6, 7, 8}
	data1 = data[:0]
	require.Equal(t, 0, len(data1))

}
