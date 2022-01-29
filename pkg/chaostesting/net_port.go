// Copyright 2021 Matrix Origin
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

package fz

import (
	"strconv"
	"sync"
	"sync/atomic"
)

type PortRange [2]uint16

func (_ Def) PortRange() PortRange {
	return PortRange{20000, 50000}
}

type GetPort func(nodeID NodeID) uint16

type GetPortStr func(nodeID NodeID) string

var nextPort uint32

func (_ Def) GetPort(
	portRange PortRange,
	infos PortInfos,
) (
	get GetPort,
	getStr GetPortStr,
) {

	lower := uint16(portRange[0])
	mod := uint16(portRange[1] - portRange[0])

	get = func(nodeID NodeID) uint16 {
		port := lower + uint16(atomic.AddUint32(&nextPort, 1))%mod
		info := &PortInfo{
			Port:   port,
			NodeID: nodeID,
		}
		infos.Store(port, info)
		return port
	}

	getStr = func(nodeID NodeID) string {
		return strconv.Itoa(int(get(nodeID)))
	}

	return
}

type PortInfo struct {
	Port   uint16
	NodeID NodeID
}

type PortInfos struct {
	*sync.Map
}

func (_ Def) PortInfos() PortInfos {
	return PortInfos{
		Map: new(sync.Map),
	}
}
