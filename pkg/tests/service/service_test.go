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
	dnIDs := c.ListDNServices()
	require.Equal(t, dnSvcNum, len(dnIDs))

	logIDs := c.ListLogServices()
	require.Equal(t, logSvcNum, len(logIDs))

	dn, err := c.GetDNService(dnIDs[0])
	require.NoError(t, err)
	require.Equal(t, ServiceStarted, dn.Status())

	log, err := c.GetLogService(logIDs[0])
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
