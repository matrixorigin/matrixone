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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	maxPort   = 65535
	curPort   = 10000 // curPort indicates allocated port.
	curPortMu sync.Mutex
)

// serviceAddress contains addresses of all services.
type serviceAddress struct {
	t *testing.T

	// Construct service addresses according to service number
	logServiceNum int
	dnServiceNum  int

	logAddresses []logServiceAddress
	dnAddresses  []dnServiceAddress
}

// newServiceAddress constructs addresses for all services.
func newServiceAddress(
	t *testing.T, logServiceNum, dnServiceNum int, hostAddr string,
) serviceAddress {
	address := serviceAddress{
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
func (a serviceAddress) assertDNService() {
	assert.Equal(a.t, a.dnServiceNum, len(a.dnAddresses))
}

// assertLogService asserts constructed address for log service.
func (a serviceAddress) assertLogService() {
	assert.Equal(a.t, a.logServiceNum, len(a.logAddresses))
}

// getDnListenAddress gets dn service address by its index.
func (a serviceAddress) getDnListenAddress(index int) string {
	a.assertDNService()

	if index >= len(a.dnAddresses) {
		return ""
	}
	return a.dnAddresses[index].listenAddr
}

// getLogListenAddress gets log service address by its index.
func (a serviceAddress) getLogListenAddress(index int) string {
	a.assertLogService()

	if index >= len(a.logAddresses) {
		return ""
	}
	return a.logAddresses[index].listenAddr
}

// getLogRaftAddress gets log raft address by its index.
func (a serviceAddress) getLogRaftAddress(index int) string {
	a.assertLogService()

	if index >= len(a.logAddresses) {
		return ""
	}
	return a.logAddresses[index].raftAddr
}

// getLogGossipAddress gets log gossip address by its index.
func (a serviceAddress) getLogGossipAddress(index int) string {
	a.assertLogService()

	if index >= len(a.logAddresses) {
		return ""
	}
	return a.logAddresses[index].gossipAddr
}

// getLogGossipSeedAddresses gets all gossip seed addresses.
//
// Select gossip addresses of the first 3 log services.
// If the number of log services was less than 3,
// then select all of them.
func (a serviceAddress) getLogGossipSeedAddresses() []string {
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
func (a serviceAddress) listHAKeeperListenAddresses() []string {
	a.assertLogService()

	n := haKeeperNum(len(a.logAddresses))
	listenAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		listenAddrs[i] = a.logAddresses[i].listenAddr
	}
	return listenAddrs
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

// getAvailablePort gets avwailable port on host address.
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
