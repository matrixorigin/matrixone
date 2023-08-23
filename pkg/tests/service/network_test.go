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

	"github.com/stretchr/testify/require"
)

func TestServiceAddress(t *testing.T) {
	logServiceNum := 3
	tnServiceNum := 2
	cnServiceNum := 2

	address := newServiceAddresses(t, logServiceNum, tnServiceNum, cnServiceNum, "127.0.0.1")
	address.assertTNService()
	address.assertLogService()
	address.assertCnService()

	for i := 0; i < tnServiceNum; i++ {
		addrList := address.listTnServiceAddresses(i)
		// 1 address for every tn service now
		require.Equal(t, 2, len(addrList))
	}
	// valid tn index: 0, 1
	// invalid tn index: 2
	addrList := address.listTnServiceAddresses(2)
	require.Equal(t, 0, len(addrList))

	for i := 0; i < logServiceNum; i++ {
		addrList := address.listLogServiceAddresses(i)
		// 3 addresses for every log service now
		require.Equal(t, 3, len(addrList))
	}
	// valid tn index: 0, 1, 2
	// invalid tn index: 3
	addrList = address.listLogServiceAddresses(3)
	require.Equal(t, 0, len(addrList))

	for i := 0; i < cnServiceNum; i++ {
		addrList := address.listCnServiceAddresses(i)
		// 1 address for every cn service now
		require.Equal(t, 1, len(addrList))
	}
	// valid tn index: 0, 1
	// invalid tn index: 2
	addrList = address.listCnServiceAddresses(2)
	require.Equal(t, 0, len(addrList))

	// ------------------------------
	// integrate with NetworkPartition
	// ------------------------------
	tnIndex := uint32(1)
	logIndex := uint32(2)
	cnIndex := uint32(1)
	partition1 := newNetworkPartition(
		logServiceNum, []uint32{logIndex},
		tnServiceNum, []uint32{tnIndex},
		cnServiceNum, []uint32{cnIndex},
	)

	partition2 := remainingNetworkPartition(logServiceNum, tnServiceNum, cnServiceNum, partition1)

	addrSets := address.buildPartitionAddressSets(partition1, partition2)
	// there are 2 address sets corresponding with 2 partitions
	require.Equal(t, 2, len(addrSets))
	// in partition 1, there are 1 tn service, 1 log service and 1 cn service.
	require.Equal(t, 3+2+1, len(addrSets[0]))
	// in partition 2, there are 1 tn service, 1 cn service and 2 log service.
	require.Equal(t, 3*2+2+1, len(addrSets[1]))

	// the first address set should contain the following addresses.
	tnListenAddr := address.getTnListenAddress(int(tnIndex))
	require.True(t, addrSets[0].contains(tnListenAddr))
	tnServiceAddr := address.getTnLogtailAddress(int(tnIndex))
	require.True(t, addrSets[0].contains(tnServiceAddr))
	logListenAddr := address.getLogListenAddress(int(logIndex))
	require.True(t, addrSets[0].contains(logListenAddr))
	logRaftAddr := address.getLogListenAddress(int(logIndex))
	require.True(t, addrSets[0].contains(logRaftAddr))
	logGossipAddr := address.getLogListenAddress(int(logIndex))
	require.True(t, addrSets[0].contains(logGossipAddr))
}

func TestGetTnListenAddress(t *testing.T) {
	tnNum := 3
	address := newServiceAddresses(t, 1, tnNum, 0, "127.0.0.1")

	addr0 := address.getTnListenAddress(0)
	addr1 := address.getTnListenAddress(1)
	addr2 := address.getTnListenAddress(2)
	addr3 := address.getTnListenAddress(3)

	require.NotEqual(t, addr0, addr1)
	require.NotEqual(t, addr0, addr2)
	require.NotEqual(t, addr1, addr2)
	require.Equal(t, "", addr3)
}

func TestGetTnServiceAddress(t *testing.T) {
	tnNum := 3
	address := newServiceAddresses(t, 1, tnNum, 0, "127.0.0.1")

	addr0 := address.getTnLogtailAddress(0)
	addr1 := address.getTnLogtailAddress(1)
	addr2 := address.getTnLogtailAddress(2)
	addr3 := address.getTnLogtailAddress(3)

	require.NotEqual(t, addr0, addr1)
	require.NotEqual(t, addr0, addr2)
	require.NotEqual(t, addr1, addr2)
	require.Equal(t, "", addr3)
}

func TestGetLogListenAddress(t *testing.T) {
	logNum := 3
	address := newServiceAddresses(t, logNum, 1, 0, "127.0.0.1")

	addr0 := address.getLogListenAddress(0)
	addr1 := address.getLogListenAddress(1)
	addr2 := address.getLogListenAddress(2)
	addr3 := address.getLogListenAddress(3)

	require.NotEqual(t, addr0, addr1)
	require.NotEqual(t, addr0, addr2)
	require.NotEqual(t, addr1, addr2)
	require.Equal(t, "", addr3)
}

func TestGetCnListenAddress(t *testing.T) {
	cnNum := 3
	address := newServiceAddresses(t, 1, 1, cnNum, "127.0.0.1")

	addr0 := address.getCNListenAddress(0)
	addr1 := address.getCNListenAddress(1)
	addr2 := address.getCNListenAddress(2)
	addr3 := address.getCNListenAddress(3)

	require.NotEqual(t, addr0, addr1)
	require.NotEqual(t, addr0, addr2)
	require.NotEqual(t, addr1, addr2)
	require.Equal(t, "", addr3)
}

func TestGetLogRaftAddress(t *testing.T) {
	logNum := 3
	address := newServiceAddresses(t, logNum, 1, 0, "127.0.0.1")

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
	address := newServiceAddresses(t, logNum, 1, 0, "127.0.0.1")

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
	logNum := 1
	address := newServiceAddresses(t, logNum, 1, 0, "127.0.0.1")
	addrs := address.listHAKeeperListenAddresses()
	require.Equal(t, logNum, len(addrs))
}

func TestGetLogGossipSeedAddresses(t *testing.T) {
	logNum := 1
	address := newServiceAddresses(t, logNum, 1, 0, "127.0.0.1")
	addrs := address.getLogGossipSeedAddresses()
	require.Equal(t, defaultGossipSeedNum, len(addrs))
}

func TestPartition(t *testing.T) {
	logServiceNum := 3
	tnServiceNum := 2
	cnServiceNum := 1

	// normal condition
	{
		partition := newNetworkPartition(
			logServiceNum, []uint32{1},
			tnServiceNum, []uint32{0, 1},
			cnServiceNum, []uint32{1},
		)
		require.Equal(t, []uint32{0, 1}, partition.ListTNServiceIndex())
		require.Equal(t, []uint32{1}, partition.ListLogServiceIndex())

		remaining := remainingNetworkPartition(logServiceNum, tnServiceNum, cnServiceNum, partition)
		require.Nil(t, remaining.ListTNServiceIndex())
		require.Equal(t, []uint32{0, 2}, remaining.ListLogServiceIndex())

		require.Equal(t, uint64(0), remaining.tnIndexSet.GetCardinality())
		require.Equal(t, uint64(2), remaining.logIndexSet.GetCardinality())
		require.True(t, remaining.logIndexSet.Contains(0))
		require.True(t, remaining.logIndexSet.Contains(2))
	}

	// valid tn index should be: 0, 1
	// invoker specifies invalid tn index: 2, 3
	{
		partition := newNetworkPartition(
			logServiceNum, nil,
			tnServiceNum, []uint32{0, 2, 3},
			cnServiceNum, nil,
		)
		require.Equal(t, []uint32{0}, partition.ListTNServiceIndex())
		require.Nil(t, partition.ListLogServiceIndex())

		remaining := remainingNetworkPartition(logServiceNum, tnServiceNum, cnServiceNum, partition)
		require.Equal(t, []uint32{1}, remaining.ListTNServiceIndex())
		require.Equal(t, []uint32{0, 1, 2}, remaining.ListLogServiceIndex())

		require.Equal(t, uint64(1), remaining.tnIndexSet.GetCardinality())
		require.Equal(t, uint64(3), remaining.logIndexSet.GetCardinality())
		require.True(t, remaining.tnIndexSet.Contains(1))
		require.True(t, remaining.logIndexSet.Contains(0))
		require.True(t, remaining.logIndexSet.Contains(1))
		require.True(t, remaining.logIndexSet.Contains(2))
	}
}
