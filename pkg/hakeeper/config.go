// Copyright 2021 - 2022 Matrix Origin
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

package hakeeper

import (
	"time"
)

const (
	DefaultTickPerSecond   = 10
	DefaultLogStoreTimeout = 5 * time.Minute
	DefaultDNStoreTimeout  = 10 * time.Second
)

type Config struct {
	// TickPerSecond indicates how many ticks every second.
	// In HAKeeper, we do not use actual time to measure time elapse.
	// Instead, we use ticks.
	TickPerSecond int

	// LogStoreTimeout is the actual time limit between a log store's heartbeat.
	// If HAKeeper does not receive two heartbeat within LogStoreTimeout,
	// it regards the log store as down.
	LogStoreTimeout time.Duration

	// DNStoreTimeout is the actual time limit between a dn store's heartbeat.
	// If HAKeeper does not receive two heartbeat within DNStoreTimeout,
	// it regards the dn store as down.
	DNStoreTimeout time.Duration
}

func (cfg Config) Validate() error {
	return nil
}

func (cfg *Config) Fill() {
	if cfg.TickPerSecond == 0 {
		cfg.TickPerSecond = DefaultTickPerSecond
	}
	if cfg.LogStoreTimeout == 0 {
		cfg.LogStoreTimeout = DefaultLogStoreTimeout
	}
	if cfg.DNStoreTimeout == 0 {
		cfg.DNStoreTimeout = DefaultDNStoreTimeout
	}
}

func (cfg Config) LogStoreExpired(start, current uint64) bool {
	return uint64(int(cfg.LogStoreTimeout/time.Second)*cfg.TickPerSecond)+start < current
}

func (cfg Config) DnStoreExpired(start, current uint64) bool {
	return uint64(int(cfg.DNStoreTimeout/time.Second)*cfg.TickPerSecond)+start < current
}

func (cfg Config) ExpiredTick(start uint64, timeout time.Duration) uint64 {
	return uint64(timeout/time.Second)*uint64(cfg.TickPerSecond) + start
}
