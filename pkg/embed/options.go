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

package embed

import "time"

const (
	testHAKeeperStoreTimeout = 60 * time.Second
)

func WithConfigs(
	configs []string,
) Option {
	return func(c *cluster) {
		c.files = configs
	}
}

func WithPreStart(
	f func(ServiceOperator),
) Option {
	return func(c *cluster) {
		c.options.preStart = f
	}
}

func WithCNCount(
	cn int,
) Option {
	return func(c *cluster) {
		c.options.cn = cn
	}
}

func WithTesting() Option {
	return func(c *cluster) {
		c.options.testing = true
		if c.options.storeTimeout == 0 {
			c.options.storeTimeout = testHAKeeperStoreTimeout
		}
	}
}

// WithConcurrentTestClusters is only for a test whose assertion requires two
// complete embedded clusters to remain live together. Ordinary tests must use
// the default exclusive admission so an accidental second cluster fails fast.
func WithConcurrentTestClusters() Option {
	return func(c *cluster) {
		c.options.allowConcurrentTestClusters = true
	}
}

// WithHAKeeperHeartbeatTimeout overrides the CN and TN HAKeeper heartbeat RPC
// deadline for this embedded cluster. Heartbeats are issued serially, so a
// larger deadline also delays retries and command delivery after a failed RPC.
// Use it only when the RPC response itself requires a longer deadline.
func WithHAKeeperHeartbeatTimeout(timeout time.Duration) Option {
	return func(c *cluster) {
		c.options.heartbeatTimeout = timeout
	}
}
