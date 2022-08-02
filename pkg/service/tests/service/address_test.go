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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetAvailablePort(t *testing.T) {
	defer func() {
		curPort = 10000
	}()

	curPort = maxPort
	_, err := getAvailablePort("127.0.0.1")
	require.Error(t, err)

	curPort = maxPort - 1
	port, err := getAvailablePort("127.0.0.1")
	require.NoError(t, err)
	require.Equal(t, port, strconv.Itoa(maxPort))
}

func TestServiceAddress(t *testing.T) {
	address := newServiceAddress(t, 3, 3, "127.0.0.1")
	address.assertDNService()
	address.assertLogService()
}

func TestGetDnListenAddress(t *testing.T) {
	dnNum := 3
	address := newServiceAddress(t, 1, dnNum, "127.0.0.1")

	addr0 := address.getDnListenAddress(0)
	addr1 := address.getDnListenAddress(1)
	addr2 := address.getDnListenAddress(2)
	addr3 := address.getDnListenAddress(3)

	require.NotEqual(t, addr0, addr1)
	require.NotEqual(t, addr0, addr2)
	require.NotEqual(t, addr1, addr2)
	require.Equal(t, "", addr3)
}

func TestGetLogListenAddress(t *testing.T) {
	logNum := 3
	address := newServiceAddress(t, logNum, 1, "127.0.0.1")

	addr0 := address.getLogListenAddress(0)
	addr1 := address.getLogListenAddress(1)
	addr2 := address.getLogListenAddress(2)
	addr3 := address.getLogListenAddress(3)

	require.NotEqual(t, addr0, addr1)
	require.NotEqual(t, addr0, addr2)
	require.NotEqual(t, addr1, addr2)
	require.Equal(t, "", addr3)
}

func TestGetLogRaftAddress(t *testing.T) {
	logNum := 3
	address := newServiceAddress(t, logNum, 1, "127.0.0.1")

	addr0 := address.getLogRaftAddress(0)
	addr1 := address.getLogRaftAddress(1)
	addr2 := address.getLogRaftAddress(2)
	addr3 := address.getLogRaftAddress(3)

	require.NotEqual(t, addr0, addr1)
	require.NotEqual(t, addr0, addr2)
	require.NotEqual(t, addr1, addr2)
	require.Equal(t, "", addr3)
}

func TestGetLogGossipAddress(t *testing.T) {
	logNum := 3
	address := newServiceAddress(t, logNum, 1, "127.0.0.1")

	addr0 := address.getLogGossipAddress(0)
	addr1 := address.getLogGossipAddress(1)
	addr2 := address.getLogGossipAddress(2)
	addr3 := address.getLogGossipAddress(3)

	require.NotEqual(t, addr0, addr1)
	require.NotEqual(t, addr0, addr2)
	require.NotEqual(t, addr1, addr2)
	require.Equal(t, "", addr3)
}

func TestListHAKeeperListenAddresses(t *testing.T) {
	logNum := 3
	address := newServiceAddress(t, logNum, 1, "127.0.0.1")
	addrs := address.listHAKeeperListenAddresses()
	require.Equal(t, logNum, len(addrs))
	require.NotEqual(t, addrs[0], addrs[1])
	require.NotEqual(t, addrs[0], addrs[2])
	require.NotEqual(t, addrs[1], addrs[2])
}

func TestGetLogGossipSeedAddresses(t *testing.T) {
	logNum := 4
	address := newServiceAddress(t, logNum, 1, "127.0.0.1")
	addrs := address.getLogGossipSeedAddresses()
	require.Equal(t, defaultGossipSeedNum, len(addrs))
	require.NotEqual(t, addrs[0], addrs[1])
	require.NotEqual(t, addrs[0], addrs[2])
	require.NotEqual(t, addrs[1], addrs[2])
}
