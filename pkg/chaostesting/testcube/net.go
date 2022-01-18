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

package main

import (
	"encoding/binary"
	"math/rand"
	"net"
	"runtime"
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"github.com/vishvananda/netlink"
)

type PortRange [2]int64

func (_ Def) PortRange() PortRange {
	return PortRange{20000, 50000}
}

type RandPort func() string

var nextPort int64

func (_ Def) RandPort(
	portRange PortRange,
) RandPort {
	lower := int64(portRange[0])
	mod := int64(portRange[1] - portRange[0])
	return func() (ret string) {
		return strconv.FormatInt(
			lower+atomic.AddInt64(&nextPort, 1)%mod,
			10,
		)
	}
}

type UseDummyInterface bool

func (_ Def) UseDummyInterface() UseDummyInterface {
	return false
}

type ListenHost string

func (_ Def) ListenHost(
	id uuid.UUID,
	use UseDummyInterface,
) (
	host ListenHost,
	cleanup fz.Cleanup,
) {

	if runtime.GOOS != "linux" || !use {
		host = "localhost"
		return
	}

	// setup dummy device on linux

	linkAttrs := netlink.LinkAttrs{
		MTU:    1234,
		TxQLen: 256,
		Name:   id.String()[:8],
	}
	link := &netlink.Dummy{
		LinkAttrs: linkAttrs,
	}
	ce(netlink.LinkAdd(link))
	ce(netlink.LinkSetUp(link))

	ip := make(net.IP, 4)
	binary.LittleEndian.PutUint32(ip, uint32(rand.Int31()))
	addr, err := netlink.ParseAddr(ip.String() + "/24")
	ce(err)
	ce(netlink.AddrAdd(link, addr))

	host = ListenHost(ip.String())

	cleanup = func() {
		ce(netlink.LinkDel(link))
	}

	return
}
