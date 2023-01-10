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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	maxPort   = 65535
	curPort   = 10000 // curPort indicates allocated port.
	curPortMu sync.Mutex
)

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

// GetAvailablePort gets available port on host address.
func GetAvailablePort(host string) (string, error) {
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
		return "", moerr.NewInternalErrorNoCtx("failed to allocate")
	}
	return strconv.Itoa(port), nil
}
