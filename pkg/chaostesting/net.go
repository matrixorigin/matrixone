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
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/songgao/water"
	"github.com/vishvananda/netlink"
)

type NetworkModel string

func (_ Def) NetworkModel() NetworkModel {
	return "localhost"
}

type GetNetworkHost func() string

func (_ Def) NetworkHost(
	id uuid.UUID,
	model NetworkModel,
	wt RootWaitTree,
	filter FilterPacket,
) (
	get GetNetworkHost,
	cleanup Cleanup,
) {

	var once sync.Once
	var host string
	var cleanupFuncs []func()
	cleanup = func() {
		for _, fn := range cleanupFuncs {
			fn()
		}
	}

	switch model {

	case "localhost":
		get = func() string {
			return "127.0.0.1"
		}

	case "dummy":

		get = func() string {
			// linux only
			if runtime.GOOS != "linux" {
				return "localhost"
			}

			once.Do(func() {
				linkAttrs := netlink.LinkAttrs{
					MTU:    1234,
					TxQLen: 256,
					Name:   "fz-" + id.String()[:8],
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

				ip := randomIP()
				addr, err := netlink.ParseAddr(ip.String() + "/24")
				ce(err)
				ce(netlink.AddrAdd(link, addr))

				host = ip.String()

				cleanupFuncs = append(cleanupFuncs, func() {
					ce(netlink.LinkSetDown(link))
					ce(netlink.LinkDel(link))
				})
			})

			return host
		}

	case "tun":

		get = func() string {

			once.Do(func() {
				name := id.String()
				name = name[len(name)-15:]
				dev, err := water.New(water.Config{
					DeviceType: water.TUN,
					PlatformSpecificParams: water.PlatformSpecificParams{
						Name: name,
					},
				})
				ce(err)
				wt.Go(func() {
					<-wt.Ctx.Done()
					ce(dev.Close())
				})

				link, err := netlink.LinkByName(dev.Name())
				ce(err)
				err = netlink.LinkSetUp(link)
				ce(err)

				ip := randomIP()
				addr, err := netlink.ParseAddr(ip.String() + "/24")
				ce(err)
				ce(netlink.AddrAdd(link, addr))

				const mtu = 1234
				err = netlink.LinkSetMTU(link, mtu)
				ce(err)

				wt.Go(func() {
					buf := make([]byte, mtu+123)
					for {
						n, err := dev.Read(buf)
						if err != nil {
							return
						}

						packet := filter(buf[:n])

						if len(packet) > 0 {
							_, err = dev.Write(packet)
							if err != nil {
								return
							}
						}
					}
				})

				host = ip.String()
			})

			return host
		}

	default:
		panic("unknown network model")

	}

	return
}

func randomIP() net.IP {
	ip := make(net.IP, 4)
	ip[0] = 192
	ip[1] = 168
	ip[2] = 100 + byte(rand.Intn(123))
	ip[3] = 100 + byte(rand.Intn(123))
	return ip
}
