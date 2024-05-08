// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package address

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAdjust(t *testing.T) {
	defaultListenAddress := "127.0.0.1:8080"
	cases := []struct {
		address       Address
		exceptAddress Address
		machineHost   string
	}{
		{
			address: Address{},
			exceptAddress: Address{
				ListenAddress:  defaultListenAddress,
				ServiceAddress: defaultListenAddress,
			},
		},
		{
			address: Address{
				ListenAddress:  "127.0.0.1:8081",
				ServiceAddress: "",
			},
			exceptAddress: Address{
				ListenAddress:  "127.0.0.1:8081",
				ServiceAddress: "127.0.0.1:8081",
			},
		},
		{
			address: Address{
				ListenAddress:  "127.0.0.1:8081",
				ServiceAddress: "abc:8081",
			},
			exceptAddress: Address{
				ListenAddress:  "127.0.0.1:8081",
				ServiceAddress: "abc:8081",
			},
		},
		{
			address: Address{
				ListenAddress:  "127.0.0.1:8081",
				ServiceAddress: "abc:8081",
			},
			exceptAddress: Address{
				ListenAddress:  "127.0.0.1:8081",
				ServiceAddress: "bcd:8081",
			},
			machineHost: "bcd",
		},
		{
			address: Address{},
			exceptAddress: Address{
				ListenAddress:  defaultListenAddress,
				ServiceAddress: "abc:8080",
			},
			machineHost: "abc",
		},
	}

	for _, c := range cases {
		c.address.Adjust(c.machineHost, defaultListenAddress)
		assert.Equal(t, c.exceptAddress, c.address)
	}
}

func TestAddressManager(t *testing.T) {
	serviceAddr := "127.0.0.1"
	portBase := 39000
	m := NewAddressManager(serviceAddr, portBase)
	assert.NotNil(t, m)
	m.Register(2)
	m.Register(1)
	assert.Equal(t, "0.0.0.0:39000", m.ListenAddress(2))
	assert.Equal(t, "127.0.0.1:39000", m.ServiceAddress(2))
	assert.Equal(t, "0.0.0.0:39001", m.ListenAddress(1))
	assert.Equal(t, "127.0.0.1:39001", m.ServiceAddress(1))

	l, err := net.Listen("tcp4", "0.0.0.0:39002")
	assert.NoError(t, err)
	defer func() {
		err = l.Close()
		assert.NoError(t, err)
	}()
	m.Register(3)
	assert.Equal(t, "0.0.0.0:39003", m.ListenAddress(3))
	assert.Equal(t, "127.0.0.1:39003", m.ServiceAddress(3))
}

func TestRemoteAddressAvail(t *testing.T) {
	remoteAddr := "127.0.0.1:17001"
	timeout := time.Millisecond * 500
	assert.False(t, RemoteAddressAvail(remoteAddr, timeout))

	l, err := net.Listen("tcp", remoteAddr)
	assert.NoError(t, err)
	assert.NotNil(t, l)
	defer l.Close()
	assert.True(t, RemoteAddressAvail(remoteAddr, timeout))
}
