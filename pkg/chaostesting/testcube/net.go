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
	"strconv"
	"sync/atomic"
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

type ListenHost string

func (_ Def) ListenHost() ListenHost {
	return "localhost"
}
