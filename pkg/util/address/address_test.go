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
	"testing"

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
