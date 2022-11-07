// Copyright 2021 - 2022 Matrix Origin
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

package service

import (
	"context"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

const (
	supportMultiDN = false
)

func TestClusterStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	// initialize cluster
	c, err := NewCluster(t, DefaultOptions())
	require.NoError(t, err)

	// start the cluster
	require.NoError(t, c.Start())
	// close the cluster
	require.NoError(t, c.Close())
}

func TestAllocateID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	// initialize cluster
	c, err := NewCluster(t, DefaultOptions())
	require.NoError(t, err)

	// start the cluster
	require.NoError(t, c.Start())
	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	c.WaitHAKeeperState(ctx, logpb.HAKeeperRunning)

	cfg := logservice.HAKeeperClientConfig{
		ServiceAddresses: []string{c.(*testCluster).network.addresses.logAddresses[0].listenAddr},
		AllocateIDBatch:  10,
	}
	hc, err := logservice.NewCNHAKeeperClient(ctx, cfg)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, hc.Close())
	}()

	last := uint64(0)
	for i := 0; i < int(cfg.AllocateIDBatch)-1; i++ {
		v, err := hc.AllocateID(ctx)
		require.NoError(t, err)
		assert.True(t, v > 0)
		if last != 0 {
			assert.Equal(t, v, last+1, i)
		}
		last = v
	}
}

func TestClusterAwareness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	if !supportMultiDN {
		t.Skip("skipping, multi db not support")
		return
	}

	dnSvcNum := 2
	logSvcNum := 3
	opt := DefaultOptions().
		WithDNServiceNum(dnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	require.NoError(t, c.Start())

	// close the cluster after all
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)

	// -------------------------------------------
	// the following would test `ClusterAwareness`
	// -------------------------------------------
	dsuuids := c.ListDNServices()
	require.Equal(t, dnSvcNum, len(dsuuids))

	lsuuids := c.ListLogServices()
	require.Equal(t, logSvcNum, len(lsuuids))

	hksvcs := c.ListHAKeeperServices()
	require.NotZero(t, len(hksvcs))

	dn, err := c.GetDNService(dsuuids[0])
	require.NoError(t, err)
	require.Equal(t, ServiceStarted, dn.Status())

	log, err := c.GetLogService(lsuuids[0])
	require.NoError(t, err)
	require.Equal(t, ServiceStarted, log.Status())

	ctx1, cancel1 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel1()
	leader := c.WaitHAKeeperLeader(ctx1)
	require.NotNil(t, leader)

	// we must wait for hakeeper's running state, or hakeeper wouldn't receive hearbeat.
	ctx2, cancel2 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel2()
	c.WaitHAKeeperState(ctx2, logpb.HAKeeperRunning)

	ctx3, cancel3 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel3()
	state, err := c.GetClusterState(ctx3)
	require.NoError(t, err)
	require.Equal(t, dnSvcNum, len(state.DNState.Stores))
	require.Equal(t, logSvcNum, len(state.LogState.Stores))
}

func TestClusterOperation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	if !supportMultiDN {
		t.Skip("skipping, multi db not support")
		return
	}

	dnSvcNum := 3
	logSvcNum := 3
	opt := DefaultOptions().
		WithDNServiceNum(dnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	require.NoError(t, c.Start())
	// close the cluster after all
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)

	// -------------------------------------------
	// the following would test `ClusterOperation`
	// -------------------------------------------

	// 1. start/close dn services via different ways
	dsuuids := c.ListDNServices()
	require.Equal(t, dnSvcNum, len(dsuuids))
	// 1.a start/close dn service by uuid
	{
		index := 0
		dsuuid := dsuuids[index]

		// get the instance of dn service
		ds, err := c.GetDNService(dsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// start it
		err = c.StartDNService(dsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// close it
		err = c.CloseDNService(dsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ds.Status())
	}

	// 1.b start/close dn service by index
	{
		index := 1

		// get the instance of dn service
		ds, err := c.GetDNServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// start it
		err = c.StartDNServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// close it
		err = c.CloseDNServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ds.Status())
	}

	// 1.c start/close dn service by instance
	{
		index := 2

		// get the instance of dn service
		ds, err := c.GetDNServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// start it
		err = ds.Start()
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// close it
		err = ds.Close()
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ds.Status())
	}

	// 2. start/close log services by different ways
	lsuuids := c.ListLogServices()
	require.Equal(t, logSvcNum, len(lsuuids))
	// 2.a start/close log service by uuid
	{
		index := 0
		lsuuid := lsuuids[index]

		// get the instance of log service
		ls, err := c.GetLogService(lsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// start it
		err = c.StartLogService(lsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// close it
		err = c.CloseLogService(lsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ls.Status())
	}

	// 2.b start/close log service by index
	{
		index := 1

		// get the instance of log service
		ls, err := c.GetLogServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// start it
		err = c.StartLogServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// close it
		err = c.CloseLogServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ls.Status())
	}

	// 2.c start/close log service by instance
	{
		index := 2

		// get the instance of log service
		ls, err := c.GetLogServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// start it
		err = ls.Start()
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// close it
		err = ls.Close()
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ls.Status())
	}
}

func TestClusterState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	if !supportMultiDN {
		t.Skip("skipping, multi db not support")
		return
	}

	dnSvcNum := 2
	logSvcNum := 3
	opt := DefaultOptions().
		WithDNServiceNum(dnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	require.NoError(t, c.Start())
	// close the cluster after all
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)

	// ----------------------------------------
	// the following would test `ClusterState`.
	// ----------------------------------------
	ctx1, cancel1 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel1()
	leader := c.WaitHAKeeperLeader(ctx1)
	require.NotNil(t, leader)

	dsuuids := c.ListDNServices()
	require.Equal(t, dnSvcNum, len(dsuuids))

	lsuuids := c.ListLogServices()
	require.Equal(t, logSvcNum, len(lsuuids))

	// we must wait for hakeeper's running state, or hakeeper wouldn't receive hearbeat.
	ctx2, cancel2 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel2()
	c.WaitHAKeeperState(ctx2, logpb.HAKeeperRunning)

	hkstate := c.GetHAKeeperState()
	require.Equal(t, logpb.HAKeeperRunning, hkstate)

	// cluster should be healthy
	require.True(t, c.IsClusterHealthy())

	ctx3, cancel3 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel3()
	state, err := c.GetClusterState(ctx3)
	require.NoError(t, err)
	require.Equal(t, dnSvcNum, len(state.DNState.Stores))
	require.Equal(t, logSvcNum, len(state.LogState.Stores))

	// FIXME: validate the result list of dn shards
	ctx4, cancel4 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel4()
	_, err = c.ListDNShards(ctx4)
	require.NoError(t, err)

	// FIXME: validate the result list of log shards
	ctx5, cancel5 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel5()
	_, err = c.ListLogShards(ctx5)
	require.NoError(t, err)

	// test for:
	//   - GetDNStoreInfo
	//   - GetDNStoreInfoIndexed
	//   - DNStoreExpired
	//   - DNStoreExpiredIndexed
	{
		dnIndex := 0
		dsuuid := dsuuids[dnIndex]

		ctx6, cancel6 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel6()
		dnStoreInfo1, err := c.GetDNStoreInfo(ctx6, dsuuid)
		require.NoError(t, err)

		ctx7, cancel7 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel7()
		dnStoreInfo2, err := c.GetDNStoreInfoIndexed(ctx7, dnIndex)
		require.NoError(t, err)
		require.Equal(t, dnStoreInfo1.Shards, dnStoreInfo2.Shards)

		expired1, err := c.DNStoreExpired(dsuuid)
		require.NoError(t, err)
		require.False(t, expired1)

		expired2, err := c.DNStoreExpiredIndexed(dnIndex)
		require.NoError(t, err)
		require.False(t, expired2)
	}

	// test for:
	//   - GetLogStoreInfo
	//   - GetLogStoreInfoIndexed
	//   - LogStoreExpired
	//   - LogStoreExpiredIndexed
	{
		logIndex := 1
		lsuuid := lsuuids[logIndex]

		ctx8, cancel8 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel8()
		logStoreInfo1, err := c.GetLogStoreInfo(ctx8, lsuuid)
		require.NoError(t, err)

		ctx9, cancel9 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel9()
		logStoreInfo2, err := c.GetLogStoreInfoIndexed(ctx9, logIndex)
		require.NoError(t, err)
		require.Equal(t, len(logStoreInfo1.Replicas), len(logStoreInfo2.Replicas)) // TODO: sort and compare detail.

		expired1, err := c.LogStoreExpired(lsuuid)
		require.NoError(t, err)
		require.False(t, expired1)

		expired2, err := c.LogStoreExpiredIndexed(logIndex)
		require.NoError(t, err)
		require.False(t, expired2)
	}
}

func TestClusterWaitState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	if !supportMultiDN {
		t.Skip("skipping, multi db not support")
		return
	}

	dnSvcNum := 2
	logSvcNum := 3
	opt := DefaultOptions().
		WithDNServiceNum(dnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	require.NoError(t, c.Start())
	// close the cluster after all
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)

	// we must wait for hakeeper's running state, or hakeeper wouldn't receive hearbeat.
	ctx1, cancel1 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel1()
	c.WaitHAKeeperState(ctx1, logpb.HAKeeperRunning)

	// --------------------------------------------
	// the following would test `ClusterWaitState`.
	// --------------------------------------------

	// test WaitDNShardsReported
	{
		ctx2, cancel2 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel2()
		c.WaitDNShardsReported(ctx2)
	}

	// test WaitLogShardsReported
	{
		ctx3, cancel3 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel3()
		c.WaitLogShardsReported(ctx3)
	}

	// test WaitDNReplicaReported
	{
		ctx4, cancel4 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel4()
		dnShards, err := c.ListDNShards(ctx4)
		require.NoError(t, err)
		require.NotZero(t, len(dnShards))

		dnShardID := dnShards[0].ShardID
		ctx5, cancel5 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel5()
		c.WaitDNReplicaReported(ctx5, dnShardID)
	}

	// test WaitLogReplicaReported
	{
		ctx6, cancel6 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel6()
		logShards, err := c.ListLogShards(ctx6)
		require.NotZero(t, len(logShards))
		require.NoError(t, err)

		logShardID := logShards[0].ShardID
		ctx7, cancel7 := context.WithTimeout(context.Background(), defaultTestTimeout)
		defer cancel7()
		c.WaitLogReplicaReported(ctx7, logShardID)
	}
}

func TestNetworkPartition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	if !supportMultiDN {
		t.Skip("skipping, multi db not support")
		return
	}

	dnSvcNum := 2
	logSvcNum := 4
	opt := DefaultOptions().
		WithDNServiceNum(dnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	require.NoError(t, c.Start())
	// close the cluster after all
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)

	// we must wait for hakeeper's running state, or hakeeper wouldn't receive hearbeat.
	ctx1, cancel1 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel1()
	c.WaitHAKeeperState(ctx1, logpb.HAKeeperRunning)

	// --------------------------------------------
	// the following would test network partition
	// --------------------------------------------

	// dn service index: 0, 1
	// log service index: 0, 1, 2, 3
	// seperate dn service 1 from other services
	partition1 := c.NewNetworkPartition([]uint32{1}, nil, nil)
	require.Equal(t, []uint32{1}, partition1.ListDNServiceIndex())
	require.Nil(t, partition1.ListLogServiceIndex())

	partition2 := c.RemainingNetworkPartition(partition1)
	require.Equal(t, []uint32{0}, partition2.ListDNServiceIndex())
	require.Equal(t, []uint32{0, 1, 2, 3}, partition2.ListLogServiceIndex())

	// enable network partition
	c.StartNetworkPartition(partition1, partition2)
	ctx2, cancel2 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel2()
	c.WaitDNStoreTimeoutIndexed(ctx2, 1)

	// disable network partition
	c.CloseNetworkPartition()
	ctx3, cancel3 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel3()
	c.WaitDNStoreReportedIndexed(ctx3, 1)

	// dn service index: 0, 1
	// log service index: 0, 1, 2, 3
	// seperate log service 3 from other services
	partition3 := c.NewNetworkPartition(nil, []uint32{3}, nil)
	require.Nil(t, partition3.ListDNServiceIndex())
	require.Equal(t, []uint32{3}, partition3.ListLogServiceIndex())

	partition4 := c.RemainingNetworkPartition(partition3)
	require.Equal(t, []uint32{0, 1}, partition4.ListDNServiceIndex())
	require.Equal(t, []uint32{0, 1, 2}, partition4.ListLogServiceIndex())

	// enable network partition
	c.StartNetworkPartition(partition3, partition4)
	ctx4, cancel4 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel4()
	c.WaitLogStoreTimeoutIndexed(ctx4, 3)

	// disable network partition
	c.CloseNetworkPartition()
	ctx5, cancel5 := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel5()
	c.WaitLogStoreReportedIndexed(ctx5, 3)
}
