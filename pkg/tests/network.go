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

package tests

import (
	"net"
	"strconv"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/testutil/portlease"
)

// Returned ports retain their range reservation until process exit, including
// while callers wait for admission and between service listener restarts.
var processPorts portAllocator

type portAllocator struct {
	sync.Mutex
	ranges    portlease.Allocator
	next, end uint64
	leases    []*portlease.Lease
}

// GetAddressBatch generates service addresses by batch.
func GetAddressBatch(host string, batch int) ([]string, error) {
	addrs := make([]string, batch)
	for i := 0; i < batch; i++ {
		port, err := GetAvailablePort(host)
		if err != nil {
			return nil, err
		}
		addrs[i] = net.JoinHostPort(host, port)
	}
	return addrs, nil
}

// GetAvailablePort gets an available port and reserves its range against other
// cooperating test allocators until process exit. The probe skips listeners
// already owned by unrelated applications, which do not honor these leases.
func GetAvailablePort(host string) (string, error) { return processPorts.allocate(host) }

func (a *portAllocator) allocate(host string) (string, error) {
	a.Lock()
	defer a.Unlock()
	for {
		if a.next == a.end {
			lease, err := a.ranges.Acquire()
			if err != nil {
				return "", err
			}
			a.leases = append(a.leases, lease)
			a.next = lease.Base()
			a.end = a.next + portlease.Span
		}
		port := a.next
		a.next++
		ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP(host), Port: int(port)})
		if err != nil {
			continue
		}
		if err = ln.Close(); err != nil {
			return "", err
		}
		return strconv.FormatUint(port, 10), nil
	}
}
