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
	// Test clusters run under the race detector and may lose several seconds to
	// scheduler instrumentation while a catalog-heavy transaction is active.
	// Keep the heartbeat request alive through a transient scheduler stall, and
	// keep the shared transport alive longer than that request. Both remain
	// bounded well inside the store-liveness window so real failures are retried.
	testHAKeeperHeartbeatTimeout   = 15 * time.Second
	testHAKeeperBackendReadTimeout = 20 * time.Second
	testHAKeeperStoreTimeout       = 60 * time.Second
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
		if c.options.heartbeatTimeout == 0 {
			c.options.heartbeatTimeout = testHAKeeperHeartbeatTimeout
		}
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
// deadline for this embedded cluster. Heartbeat retries remain serial, but
// schedule-command polling uses an independent transport and progress budget.
// Use this only when a test needs a deadline different from its mode default.
func WithHAKeeperHeartbeatTimeout(timeout time.Duration) Option {
	return func(c *cluster) {
		c.options.heartbeatTimeout = timeout
	}
}
