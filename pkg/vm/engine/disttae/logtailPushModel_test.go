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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"
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

// should ensure syncLogTailTimestamp can get time or set time without any trace.
// and will not cause any deadlock.
func TestSyncLogTailTimestamp(t *testing.T) {
	var globalTime syncLogTailTimestamp
	globalTime.initLogTailTimestamp()
	require.Equal(t, int64(0), globalTime.getTimestamp().PhysicalTime)

	storeTime := timestamp.Timestamp{PhysicalTime: 100}
	compareTime := timestamp.Timestamp{PhysicalTime: 105}
	globalTime.updateTimestamp(storeTime)

	require.Equal(t, storeTime.GreaterEq(compareTime), globalTime.greatEq(compareTime))

	errChan := make(chan error, 2)
	// routine 1 to update the global timestamp.
	go func() {
		for i := 0; i < 10; i++ {
			compareTime.PhysicalTime++
			globalTime.updateTimestamp(compareTime)
		}
		errChan <- nil
	}()
	// routine 2 to block until global timestamp >= requireTime
	go func() {
		requireTime := timestamp.Timestamp{PhysicalTime: 115}
		errChan <- globalTime.blockUntilTxnTimeIsLegal(context.TODO(), requireTime)
	}()
	// global time will increase to 115 on routine 1, and the routine 2 can run succeed.
	for i := 0; i < 2; i++ {
		getErr := <-errChan
		require.NoError(t, getErr)
	}
}

// should ensure that subscribe and unsubscribe methods are effective.
func TestSubscribedTable(t *testing.T) {
	var subscribeRecord subscribedTable
	subscribeRecord.initTableSubscribeRecord()
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
		subscribeRecord.setTableSubscribe(tbl.db, tbl.tb)
	}
	require.Equal(t, 4, len(subscribeRecord.m))
	require.Equal(t, true, subscribeRecord.getTableSubscribe(tbls[0].db, tbls[0].tb))
	for _, tbl := range tbls {
		subscribeRecord.setTableUnsubscribe(tbl.db, tbl.tb)
	}
	require.Equal(t, 0, len(subscribeRecord.m))
}

// should ensure that subscriber can send request.
func TestLogTailSubscriber(t *testing.T) {
	receiveCount := 0
	logTailService, serviceAddr, err := mockLogTailServiceWithHandleFunc()
	require.NoError(t, err)
	require.NoError(t, logTailService.Start())
	defer logTailService.Close()
	logTailService.RegisterRequestHandler(func(ctx context.Context, request morpc.Message, sequence uint64, cs morpc.ClientSession) error {
		receiveCount++
		return nil
	})

	subscriber := new(logTailSubscriber)
	err = subscriber.init(serviceAddr)
	require.NoError(t, err)
	defer subscriber.logTailClient.Close()
	require.NoError(t, subscriber.subscribeTable(context.TODO(), api.TableID{}))
	require.NoError(t, subscriber.subscribeTable(context.TODO(), api.TableID{}))
	require.NoError(t, subscriber.unSubscribeTable(context.TODO(), api.TableID{}))
	time.Sleep(2 * time.Second)
	require.Equal(t, 3, receiveCount)
}

func mockLogTailServiceWithHandleFunc() (logTailService morpc.RPCServer, serviceAddr string, err error) {
	port := 0
	port, err = getAFreePort()
	if err != nil {
		return nil, serviceAddr, err
	}
	serviceAddr = fmt.Sprintf("0.0.0.0:%d", port)
	logTailService, err = morpc.NewRPCServer(
		"mock-log-tail-service", serviceAddr,
		morpc.NewMessageCodec(func() morpc.Message {
			return &service.LogtailRequest{}
		}))
	return logTailService, serviceAddr, err
}

// a method to get a free port.
func getAFreePort() (int, error) {
	addr, err1 := net.ResolveTCPAddr("tcp", "0.0.0.0:0")
	if err1 != nil {
		return 0, err1
	}
	listener, err2 := net.ListenTCP("tcp", addr)
	if err2 != nil {
		return 0, err2
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
