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
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	maxPort   = 65535
	curPort   = 10000 // curPort indicates allocated port.
	curPortMu sync.Mutex
)

// serviceAddresses contains addresses of all services.
type serviceAddresses struct {
	t *testing.T

	// Construct service addresses according to service number
	logServiceNum int
	dnServiceNum  int

	logAddresses []logServiceAddress
	dnAddresses  []dnServiceAddress
}

// newServiceAddresses constructs addresses for all services.
func newServiceAddresses(
	t *testing.T, logServiceNum, dnServiceNum int, hostAddr string,
) serviceAddresses {
	address := serviceAddresses{
		t:             t,
		logServiceNum: logServiceNum,
		dnServiceNum:  dnServiceNum,
	}

	// build log service addresses
	logBatch := address.logServiceNum
	logAddrs := make([]logServiceAddress, logBatch)
	for i := 0; i < logBatch; i++ {
		logAddr, err := newLogServiceAddress(hostAddr)
		require.NoError(t, err)
		logAddrs[i] = logAddr
	}
	address.logAddresses = logAddrs

	// build dn service addresses
	dnBatch := address.dnServiceNum
	dnAddrs := make([]dnServiceAddress, dnBatch)
	for i := 0; i < dnBatch; i++ {
		dnAddr, err := newDNServiceAddress(hostAddr)
		require.NoError(t, err)
		dnAddrs[i] = dnAddr
	}
	address.dnAddresses = dnAddrs

	return address
}

// assertDNService asserts constructed address for dn service.
func (a serviceAddresses) assertDNService() {
	assert.Equal(a.t, a.dnServiceNum, len(a.dnAddresses))
}

// assertLogService asserts constructed address for log service.
func (a serviceAddresses) assertLogService() {
	assert.Equal(a.t, a.logServiceNum, len(a.logAddresses))
}

// getDnListenAddress gets dn service address by its index.
func (a serviceAddresses) getDnListenAddress(index int) string {
	a.assertDNService()

	if index >= len(a.dnAddresses) || index < 0 {
		return ""
	}
	return a.dnAddresses[index].listenAddr
}

// getLogListenAddress gets log service address by its index.
func (a serviceAddresses) getLogListenAddress(index int) string {
	a.assertLogService()

	if index >= len(a.logAddresses) || index < 0 {
		return ""
	}
	return a.logAddresses[index].listenAddr
}

// getLogRaftAddress gets log raft address by its index.
func (a serviceAddresses) getLogRaftAddress(index int) string {
	a.assertLogService()

	if index >= len(a.logAddresses) || index < 0 {
		return ""
	}
	return a.logAddresses[index].raftAddr
}

// getLogGossipAddress gets log gossip address by its index.
func (a serviceAddresses) getLogGossipAddress(index int) string {
	a.assertLogService()

	if index >= len(a.logAddresses) || index < 0 {
		return ""
	}
	return a.logAddresses[index].gossipAddr
}

// getLogGossipSeedAddresses gets all gossip seed addresses.
//
// Select gossip addresses of the first 3 log services.
// If the number of log services was less than 3,
// then select all of them.
func (a serviceAddresses) getLogGossipSeedAddresses() []string {
	a.assertLogService()

	n := gossipSeedNum(len(a.logAddresses))
	seedAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		seedAddrs[i] = a.logAddresses[i].gossipAddr
	}
	return seedAddrs
}

// listHAKeeperListenAddresses gets addresses of all hakeeper servers.
//
// Select the first 3 log services to start hakeeper replica.
// If the number of log services was less than 3,
// then select the first of them.
func (a serviceAddresses) listHAKeeperListenAddresses() []string {
	a.assertLogService()

	n := haKeeperNum(len(a.logAddresses))
	listenAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		listenAddrs[i] = a.logAddresses[i].listenAddr
	}
	return listenAddrs
}

// buildPartitionAddressSets returns service addresses by every partition.
func (a serviceAddresses) buildPartitionAddressSets(partitions ...NetworkPartition) []addressSet {
	sets := make([]addressSet, 0, len(partitions))
	for _, part := range partitions {
		sets = append(sets, a.listPartitionAddresses(part))
	}
	return sets
}

// listPartitionAddresses returns all service addresses within the same partition.
func (a serviceAddresses) listPartitionAddresses(partition NetworkPartition) addressSet {
	addrSet := newAddressSet()
	for _, dnIndex := range partition.ListDNServiceIndex() {
		addrs := a.listDnServiceAddresses(int(dnIndex))
		addrSet.addAddresses(addrs...)
	}
	for _, logIndex := range partition.ListLogServiceIndex() {
		addrs := a.listLogServiceAddresses(int(logIndex))
		addrSet.addAddresses(addrs...)
	}
	return addrSet
}

// listDnServiceAddresses lists all addresses of dn service by its index.
func (a serviceAddresses) listDnServiceAddresses(index int) []string {
	a.assertDNService()

	if index >= len(a.dnAddresses) || index < 0 {
		return nil
	}
	return a.dnAddresses[index].listAddresses()
}

// listLogServiceAddresses lists all addresses of log service by its index.
func (a serviceAddresses) listLogServiceAddresses(index int) []string {
	a.assertLogService()

	if index >= len(a.logAddresses) || index < 0 {
		return nil
	}
	return a.logAddresses[index].listAddresses()
}

// logServiceAddress contains addresses for log service.
type logServiceAddress struct {
	listenAddr string
	raftAddr   string
	gossipAddr string
}

func newLogServiceAddress(host string) (logServiceAddress, error) {
	addrs, err := getAddressBatch(host, 3)
	if err != nil {
		return logServiceAddress{}, err
	}

	return logServiceAddress{
		listenAddr: addrs[0],
		raftAddr:   addrs[1],
		gossipAddr: addrs[2],
	}, nil
}

// listAddresses returns all addresses for single log service.
func (la logServiceAddress) listAddresses() []string {
	return []string{la.listenAddr, la.raftAddr, la.gossipAddr}
}

// dnServiceAddress contains address for dn service.
type dnServiceAddress struct {
	listenAddr string
}

func newDNServiceAddress(host string) (dnServiceAddress, error) {
	addrs, err := getAddressBatch(host, 1)
	if err != nil {
		return dnServiceAddress{}, err
	}
	return dnServiceAddress{
		listenAddr: addrs[0],
	}, nil
}

// listAddresses returns all addresses for single dn service.
func (da dnServiceAddress) listAddresses() []string {
	return []string{da.listenAddr}
}

// getAddressBatch generates service addresses by batch.
func getAddressBatch(host string, batch int) ([]string, error) {
	addrs := make([]string, batch)
	for i := 0; i < batch; i++ {
		port, err := getAvailablePort(host)
		if err != nil {
			return nil, err
		}
		addrs[i] = net.JoinHostPort(host, port)
	}
	return addrs, nil
}

// getAvailablePort gets available port on host address.
func getAvailablePort(host string) (string, error) {
	curPortMu.Lock()
	defer curPortMu.Unlock()

	port := 0
	for curPort < maxPort {
		curPort++

		ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP(host), Port: curPort})
		if err != nil {
			continue
		}
		ln.Close()

		port = curPort
		break
	}

	if port == 0 {
		return "", ErrFailAllocatePort
	}
	return strconv.Itoa(port), nil
}

// addressSet records addresses for services within the same partition.
type addressSet map[string]struct{}

func newAddressSet() addressSet {
	return make(map[string]struct{})
}

// addAddresses registers a list of addresses.
func (s addressSet) addAddresses(addrs ...string) {
	for _, addr := range addrs {
		s[addr] = struct{}{}
	}
}

// contains checks address exist or not.
func (s addressSet) contains(addr string) bool {
	_, ok := s[addr]
	return ok
}

// NetworkPartition records index of services from the same network partition.
type NetworkPartition struct {
	logIndexSet *roaring.Bitmap
	dnIndexSet  *roaring.Bitmap
}

// newNetworkPartition returns an instance of NetworkPartition.
//
// The returned instance only contains valid index according to service number.
func newNetworkPartition(
	logServiceNum int, logIndexes []uint32,
	dnServiceNum int, dnIndexes []uint32,
) NetworkPartition {
	logTotal := roaring.FlipInt(roaring.NewBitmap(), 0, logServiceNum)
	dnTotal := roaring.FlipInt(roaring.NewBitmap(), 0, dnServiceNum)

	rawLogSet := roaring.BitmapOf(logIndexes...)
	rawDnSet := roaring.BitmapOf(dnIndexes...)

	return NetworkPartition{
		logIndexSet: roaring.And(logTotal, rawLogSet),
		dnIndexSet:  roaring.And(dnTotal, rawDnSet),
	}
}

// remainingNetworkPartition returns partition for the remaining services.
func remainingNetworkPartition(
	logServiceNum, dnServiceNum int, partitions ...NetworkPartition,
) NetworkPartition {
	logTotal := roaring.FlipInt(roaring.NewBitmap(), 0, logServiceNum)
	dnTotal := roaring.FlipInt(roaring.NewBitmap(), 0, dnServiceNum)

	logUsed := roaring.NewBitmap()
	dnUsed := roaring.NewBitmap()
	for _, p := range partitions {
		dnUsed.Or(p.dnIndexSet)
		logUsed.Or(p.logIndexSet)
	}

	return NetworkPartition{
		logIndexSet: roaring.AndNot(logTotal, logUsed),
		dnIndexSet:  roaring.AndNot(dnTotal, dnUsed),
	}
}

// ListDNServiceIndex lists index of all dn services in the partition.
func (p NetworkPartition) ListDNServiceIndex() []uint32 {
	set := p.dnIndexSet

	if set.GetCardinality() == 0 {
		return nil
	}

	indexes := make([]uint32, 0, set.GetCardinality())
	iter := set.Iterator()
	for iter.HasNext() {
		indexes = append(indexes, iter.Next())
	}
	return indexes
}

// ListLogServiceIndex lists index of all log services in the partition.
func (p NetworkPartition) ListLogServiceIndex() []uint32 {
	set := p.logIndexSet

	if set.GetCardinality() == 0 {
		return nil
	}

	indexes := make([]uint32, 0, set.GetCardinality())
	iter := set.Iterator()
	for iter.HasNext() {
		indexes = append(indexes, iter.Next())
	}
	return indexes
}
