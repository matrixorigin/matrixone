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
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
	"testing"
)

// the sort func should ensure that:
// the smaller the table id of item, the smaller the indexing.
// the smaller the timestamp of item, the smaller the indexing.
// if timestamps are equal, the smaller table id one will be in front (with a smaller index).
func TestLogList_Sort(t *testing.T) {
	var lists logList = []logtail.TableLogtail{
		{Ts: &timestamp.Timestamp{PhysicalTime: 1}, Table: &api.TableID{TbId: 2}},
		{Ts: &timestamp.Timestamp{PhysicalTime: 1}, Table: &api.TableID{TbId: 1}},
		{Ts: &timestamp.Timestamp{PhysicalTime: 1}, Table: &api.TableID{TbId: 3}},
		{Ts: &timestamp.Timestamp{PhysicalTime: 2}, Table: &api.TableID{TbId: 1}},
		{Ts: &timestamp.Timestamp{PhysicalTime: 3}, Table: &api.TableID{TbId: 1}},
	}
	lists.Sort()
	expectedT := []int64{1, 1, 1, 2, 3}
	expectedI := []uint64{1, 2, 3, 1, 1}
	for i := range lists {
		require.Equal(t, expectedT[i], lists[i].Ts.PhysicalTime)
		require.Equal(t, expectedI[i], lists[i].Table.TbId)
	}
}
