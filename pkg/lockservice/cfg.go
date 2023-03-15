// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

// Config lock service config
type Config struct {
	ServiceID              string
	ServiceAddress         string
	MaxFixedSliceSize      toml.ByteSize
	KeepBindDuration       toml.Duration
	KeepRemoteLockDuration toml.Duration
	RemoteLockTimeout      toml.Duration
	RPC                    morpc.Config
}

func (c *Config) adjust() {
	if c.ServiceID == "" {
		panic("missing service id")
	}
	if c.ServiceAddress == "" {
		panic("missing service address")
	}
	if c.MaxFixedSliceSize == 0 {
		c.MaxFixedSliceSize = toml.ByteSize(1024 * 1024)
	}
	if c.KeepBindDuration.Duration == 0 {
		c.KeepBindDuration.Duration = time.Second
	}
	if c.KeepRemoteLockDuration.Duration == 0 {
		c.KeepRemoteLockDuration.Duration = time.Second
	}
	if c.RemoteLockTimeout.Duration == 0 {
		c.RemoteLockTimeout.Duration = c.KeepRemoteLockDuration.Duration * 10
	}
	c.RPC.Adjust()
}
