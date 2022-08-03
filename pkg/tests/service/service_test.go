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
	// 	- do some operation via `ClusterOperation`
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

	leader := c.WaitHAKeeperLeader(2 * time.Second)
	require.NotNil(t, leader)

	// we must wait for hakeeper's running state,
	// or hakeeper wouldn't receive hearbeat.
	c.WaitHAKeeperState(10*time.Second, logpb.HAKeeperRunning)

	state, err := c.GetClusterState(2 * time.Second)
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
