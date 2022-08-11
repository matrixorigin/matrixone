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
	"time"

	"github.com/stretchr/testify/require"

	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestCluster(t *testing.T) {
	// initialize cluster
	c, err := NewCluster(t, DefaultOptions())
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	require.NoError(t, err)

	// close the cluster
	err = c.Close()
	require.NoError(t, err)

	// FIXME:
	// 	- check cluster state via `ClusterAssertState`
	// 	- wait cluster state via `ClusterWaitState`
}

func TestClusterAwareness(t *testing.T) {
	// FIXME: skip this test after issue #4334 solved:
	// https://github.com/matrixorigin/matrixone/issues/4334
	t.Skip()

	dnSvcNum := 2
	logSvcNum := 3
	opt := DefaultOptions().
		WithDNServiceNum(dnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	require.NoError(t, err)

	// close the cluster after all
	defer func() {
		err := c.Close()
		require.NoError(t, err)
	}()

	// -------------------------------------------
	// the following would test `ClusterAwareness`
	// -------------------------------------------
	dsuuids := c.ListDNServices()
	require.Equal(t, dnSvcNum, len(dsuuids))

	lsuuids := c.ListLogServices()
	require.Equal(t, logSvcNum, len(lsuuids))

	dn, err := c.GetDNService(dsuuids[0])
	require.NoError(t, err)
	require.Equal(t, ServiceStarted, dn.Status())

	log, err := c.GetLogService(lsuuids[0])
	require.NoError(t, err)
	require.Equal(t, ServiceStarted, log.Status())

	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	leader := c.WaitHAKeeperLeader(ctx1)
	require.NotNil(t, leader)

	// we must wait for hakeeper's running state,
	// or hakeeper wouldn't receive hearbeat.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	c.WaitHAKeeperState(ctx2, logpb.HAKeeperRunning)

	ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel3()
	state, err := c.GetClusterState(ctx3)
	require.NoError(t, err)
	require.Equal(t, dnSvcNum, len(state.DNState.Stores))
	require.Equal(t, logSvcNum, len(state.LogState.Stores))
}

func TestClusterOperation(t *testing.T) {
	dnSvcNum := 3
	logSvcNum := 3
	opt := DefaultOptions().
		WithDNServiceNum(dnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	require.NoError(t, err)

	// close the cluster after all
	defer func() {
		err := c.Close()
		require.NoError(t, err)
	}()

	// -------------------------------------------
	// the following would test `ClusterOperation`
	// -------------------------------------------
	// 1. close dn services by different ways
	dsuuids := c.ListDNServices()
	require.Equal(t, dnSvcNum, len(dsuuids))
	// 1.a close dn service by uuid
	{
		index := 0
		dsuuid := dsuuids[index]

		// get the instance of dn service
		ds, err := c.GetDNService(dsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// close it
		err = c.CloseDNService(dsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ds.Status())
	}

	// 1.b close dn service by index
	{
		index := 1

		// get the instance of dn service
		ds, err := c.GetDNServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// close it
		err = c.CloseDNServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ds.Status())
	}

	// 1.c close dn service by instance
	{
		index := 2

		// get the instance of dn service
		ds, err := c.GetDNServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ds.Status())

		// close it
		err = ds.Close()
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ds.Status())
	}

	// 2. close log services by different ways
	lsuuids := c.ListLogServices()
	require.Equal(t, logSvcNum, len(lsuuids))
	// 2.a close log service by uuid
	{
		index := 0
		lsuuid := lsuuids[index]

		// get the instance of log service
		ls, err := c.GetLogService(lsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// close it
		err = c.CloseLogService(lsuuid)
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ls.Status())
	}

	// 2.b close log service by index
	{
		index := 1

		// get the instance of log service
		ls, err := c.GetLogServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// close it
		err = c.CloseLogServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ls.Status())
	}

	// 2.c close log service by instance
	{
		index := 2

		// get the instance of log service
		ls, err := c.GetLogServiceIndexed(index)
		require.NoError(t, err)
		require.Equal(t, ServiceStarted, ls.Status())

		// close it
		err = ls.Close()
		require.NoError(t, err)
		require.Equal(t, ServiceClosed, ls.Status())
	}
}

func TestClusterState(t *testing.T) {
	// FIXME: skip this test after issue #4334 solved:
	// https://github.com/matrixorigin/matrixone/issues/4334
	t.Skip()

	dnSvcNum := 2
	logSvcNum := 3
	opt := DefaultOptions().
		WithDNServiceNum(dnSvcNum).
		WithLogServiceNum(logSvcNum)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	require.NoError(t, err)

	// close the cluster after all
	defer func() {
		err := c.Close()
		require.NoError(t, err)
	}()

	// -------------------------------------------
	// the following would test `ClusterState`
	// -------------------------------------------
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	leader := c.WaitHAKeeperLeader(ctx1)
	require.NotNil(t, leader)

	dsuuids := c.ListDNServices()
	require.Equal(t, dnSvcNum, len(dsuuids))

	lsuuids := c.ListLogServices()
	require.Equal(t, logSvcNum, len(lsuuids))

	// we must wait for hakeeper's running state,
	// or hakeeper wouldn't receive hearbeat.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	c.WaitHAKeeperState(ctx2, logpb.HAKeeperRunning)

	ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel3()
	state, err := c.GetClusterState(ctx3)
	require.NoError(t, err)
	require.Equal(t, dnSvcNum, len(state.DNState.Stores))
	require.Equal(t, logSvcNum, len(state.LogState.Stores))

	// FIXME: validate the result list of dn shards
	ctx4, cancel4 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel4()
	_, err = c.ListDNShards(ctx4)
	require.NoError(t, err)

	// FIXME: validate the result list of log shards
	ctx5, cancel5 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel5()
	_, err = c.ListLogShards(ctx5)
	require.NoError(t, err)

	// test GetDNStoreInfo and GetDNStoreInfoIndexed
	dnIndex := 0
	dsuuid := dsuuids[dnIndex]

	ctx6, cancel6 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel6()
	dnStoreInfo1, err := c.GetDNStoreInfo(ctx6, dsuuid)
	require.NoError(t, err)

	ctx7, cancel7 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel7()
	dnStoreInfo2, err := c.GetDNStoreInfoIndexed(ctx7, dnIndex)
	require.NoError(t, err)
	require.Equal(t, dnStoreInfo1, dnStoreInfo2)

	logIndex := 1
	lsuuid := lsuuids[logIndex]

	ctx8, cancel8 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel8()
	logStoreInfo1, err := c.GetLogStoreInfo(ctx8, lsuuid)
	require.NoError(t, err)

	ctx9, cancel9 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel9()
	logStoreInfo2, err := c.GetLogStoreInfoIndexed(ctx9, logIndex)
	require.NoError(t, err)
	require.Equal(t, logStoreInfo1, logStoreInfo2)
}
