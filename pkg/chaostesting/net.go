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
	"encoding/binary"
	"math/rand"
	"net"
	"runtime"
	"strings"

	"github.com/google/uuid"
	"github.com/songgao/water"
	"github.com/vishvananda/netlink"
)

type NetworkModel string

func (_ Def) NetworkModel() NetworkModel {
	return "localhost"
}

type NetworkHost string

func (_ Def) NetworkHost(
	id uuid.UUID,
	model NetworkModel,
	wt RootWaitTree,
) (
	host NetworkHost,
	cleanup Cleanup,
) {

	switch model {

	case "localhost":
		host = "127.0.0.1"

	case "dummy":
		// linux only
		if runtime.GOOS != "linux" {
			host = "localhost"
			break
		}

		linkAttrs := netlink.LinkAttrs{
			MTU:    1234,
			TxQLen: 256,
			Name:   id.String()[:8],
		}
		link := &netlink.Dummy{
			LinkAttrs: linkAttrs,
		}
		err := netlink.LinkAdd(link)
		if err != nil {
			if strings.Contains(err.Error(), "file exists") {
				err = nil
			}
		}
		ce(err)
		ce(netlink.LinkSetUp(link))

		ip := make(net.IP, 4)
		binary.LittleEndian.PutUint32(ip, uint32(rand.Int31()))
		addr, err := netlink.ParseAddr(ip.String() + "/24")
		ce(err)
		ce(netlink.AddrAdd(link, addr))

		host = NetworkHost(ip.String())

		cleanup = func() {
			ce(netlink.LinkDel(link))
		}

	case "tun":
		dev, err := water.New(water.Config{
			DeviceType: water.TUN,
			PlatformSpecificParams: water.PlatformSpecificParams{
				Name: id.String(),
			},
		})
		ce(err)

		link, err := netlink.LinkByName(dev.Name())
		ce(err)
		err = netlink.LinkSetUp(link)
		ce(err)

		ip := make(net.IP, 4)
		binary.LittleEndian.PutUint32(ip, uint32(rand.Int31()))
		addr, err := netlink.ParseAddr(ip.String() + "/24")
		ce(err)
		ce(netlink.AddrAdd(link, addr))

		const mtu = 1234
		err = netlink.LinkSetMTU(link, mtu)
		ce(err)
		err = netlink.SetPromiscOn(link)
		ce(err)

		wt.Go(func() {
			buf := make([]byte, mtu+123)
			for {
				_, err := dev.Read(buf)
				if err != nil {
					return
				}

				//TODO packet manipulation

				_, err = dev.Write(buf)
				if err != nil {
					return
				}
			}
		})

		host = NetworkHost(ip.String())

	default:
		panic("unknown network model")

	}

	return
}
