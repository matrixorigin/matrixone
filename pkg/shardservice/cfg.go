// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

var (
	defaultLockListenAddress    = "127.0.0.1:6004"
	defaultMaxScheduleTables    = 1
	defaultFreezeCNTimeout      = time.Minute
	defaultScheduleDuration     = time.Second
	defaultHeartbeatDuration    = time.Second * 2
	defaultCheckChangedDuration = time.Minute
)

// Shard config
type Config struct {
	// ServiceID service id
	ServiceID string `toml:"-"`
	// RPC rpc config
	RPC morpc.Config `toml:"-"`

	// Enable enable shard service
	Enable bool `toml:"enable"`
	// ListenAddress shard service listen address for receiving lock requests
	ListenAddress string `toml:"listen-address"`
	// ServiceAddress service address for communication, if this address is not set, use
	// ListenAddress as the communication address.
	ServiceAddress string `toml:"service-address"`
	// MaxScheduleTables how many tables's shards can be scheduled by balancer at once.
	MaxScheduleTables int `toml:"max-schedule-tables"`
	// FreezeCNTimeout max freeze time for CN can be scheduled by balancer again.
	FreezeCNTimeout toml.Duration `toml:"max-cn-freeze-time"`
	// ScheduleDuration how often to schedule shards on CNs.
	ScheduleDuration toml.Duration `toml:"schedule-duration"`
	// SelectCNLabel label name for tenant to select CNs.
	SelectCNLabel string `toml:"cn-select-label-name"`
	// HeartbeatDuration how often to send heartbeat to shard server.
	HeartbeatDuration toml.Duration `toml:"heartbeat-duration"`
	// CheckChangedDuration how often to check deleted tables or sharding changed in CN.
	CheckChangedDuration toml.Duration `toml:"check-changed-duration"`
}

// Validate validate
func (c *Config) Validate() {
	if c.ServiceID == "" {
		panic("missing service id")
	}
	if c.ListenAddress == "" {
		c.ListenAddress = defaultLockListenAddress
	}
	if c.ServiceAddress == "" {
		c.ServiceAddress = c.ListenAddress
	}
	if c.MaxScheduleTables == 0 {
		c.MaxScheduleTables = defaultMaxScheduleTables
	}
	if c.FreezeCNTimeout.Duration == 0 {
		c.FreezeCNTimeout.Duration = defaultFreezeCNTimeout
	}
	if c.ScheduleDuration.Duration == 0 {
		c.ScheduleDuration.Duration = defaultScheduleDuration
	}
	if c.HeartbeatDuration.Duration == 0 {
		c.HeartbeatDuration.Duration = defaultHeartbeatDuration
	}
	if c.CheckChangedDuration.Duration == 0 {
		c.CheckChangedDuration.Duration = defaultCheckChangedDuration
	}
	c.RPC.Adjust()
}
