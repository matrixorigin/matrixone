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
	"context"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	log "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

// should ensure that subscribe and unsubscribe methods are effective.
func TestSubscribedTable(t *testing.T) {
	var subscribeRecord subscribedTable

	subscribeRecord.m = make(map[uint64]SubTableStatus)
	subscribeRecord.eng = &Engine{
		partitions: make(map[[2]uint64]*logtailreplay.Partition),
		globalStats: &GlobalStats{
			logtailUpdate: newLogtailUpdate(),
			waitKeeper:    newWaitKeeper(),
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

func TestDca(t *testing.T) {
	pClient := &PushClient{}

	signalCnt := 0
	require.True(t, pClient.dcaTryDelay(true, func() { signalCnt++ }))  // skip for sub response
	require.True(t, pClient.dcaTryDelay(false, func() { signalCnt++ })) // delay
	pClient.dcaConfirmAndApply()
	require.Equal(t, 1, signalCnt)
	require.False(t, pClient.dcaTryDelay(false, func() {})) // skip for finished replay

}

type testHAKeeperClient struct {
	sync.RWMutex
	value log.ClusterDetails
}

func (c *testHAKeeperClient) addTN(state log.NodeState, serviceID, logtailAddr string) {
	c.Lock()
	defer c.Unlock()
	c.value.TNStores = append(c.value.TNStores, log.TNStore{
		UUID:                 serviceID,
		State:                state,
		LogtailServerAddress: logtailAddr,
	})
}

func (c *testHAKeeperClient) GetClusterDetails(ctx context.Context) (log.ClusterDetails, error) {
	c.Lock()
	defer c.Unlock()
	return c.value, nil
}

func TestGetLogTailServiceAddr(t *testing.T) {
	e := &Engine{}

	t.Run("ok1", func(t *testing.T) {
		clusterClient := &testHAKeeperClient{}
		moc := clusterservice.NewMOCluster("", clusterClient, time.Hour)
		runtime.ServiceRuntime("").SetGlobalVariables(
			runtime.ClusterService,
			moc,
		)
		clusterClient.addTN(log.NormalState, "tn1", "a")
		moc.ForceRefresh(true)
		require.Equal(t, "a", e.getLogTailServiceAddr())
	})

	t.Run("ok2", func(t *testing.T) {
		clusterClient := &testHAKeeperClient{}
		moc := clusterservice.NewMOCluster("", clusterClient, time.Hour)
		runtime.ServiceRuntime("").SetGlobalVariables(
			runtime.ClusterService,
			moc,
		)
		go func() {
			time.Sleep(time.Second)
			clusterClient.addTN(log.NormalState, "tn1", "a")
			moc.ForceRefresh(true)
		}()
		require.Equal(t, "a", e.getLogTailServiceAddr())
	})

	t.Run("fail, empty addr", func(t *testing.T) {
		orig := defaultGetLogTailAddrTimeoutDuration
		defaultGetLogTailAddrTimeoutDuration = time.Second
		defer func() {
			defaultGetLogTailAddrTimeoutDuration = orig
		}()
		clusterClient := &testHAKeeperClient{}
		moc := clusterservice.NewMOCluster("", clusterClient, time.Hour)
		runtime.ServiceRuntime("").SetGlobalVariables(
			runtime.ClusterService,
			moc,
		)
		clusterClient.addTN(log.NormalState, "tn1", "")
		moc.ForceRefresh(true)
		require.Panics(t, func() {
			e.getLogTailServiceAddr()
		})
	})

	t.Run("fail, no tn", func(t *testing.T) {
		orig := defaultGetLogTailAddrTimeoutDuration
		defaultGetLogTailAddrTimeoutDuration = time.Second
		defer func() {
			defaultGetLogTailAddrTimeoutDuration = orig
		}()
		clusterClient := &testHAKeeperClient{}
		moc := clusterservice.NewMOCluster("", clusterClient, time.Hour)
		runtime.ServiceRuntime("").SetGlobalVariables(
			runtime.ClusterService,
			moc,
		)
		require.Panics(t, func() {
			e.getLogTailServiceAddr()
		})
	})
}
