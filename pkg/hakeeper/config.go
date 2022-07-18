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

import "time"

const (
	DefaultTickPerSecond   = 10
	DefaultLogStoreTimeout = 5 * time.Minute
	DefaultDnStoreTimeout  = 10 * time.Second
)

type HAConfig struct {
	TickPerSecond int

	LogStoreTimeout time.Duration

	DnStoreTimeout time.Duration
}

func DefaultTimeoutConfig() *HAConfig {
	return &HAConfig{
		TickPerSecond:   DefaultTickPerSecond,
		LogStoreTimeout: DefaultLogStoreTimeout,
		DnStoreTimeout:  DefaultDnStoreTimeout,
	}
}

func (config *HAConfig) SetTickPerSecond(tickPerSecond int) *HAConfig {
	config.TickPerSecond = tickPerSecond
	return config
}

func (config *HAConfig) SetLogStoreTimeout(timeout time.Duration) *HAConfig {
	config.LogStoreTimeout = timeout
	return config
}

func (config *HAConfig) SetDnStoreTimeout(timeout time.Duration) *HAConfig {
	config.DnStoreTimeout = timeout
	return config
}

func (config *HAConfig) LogStoreExpired(start, current uint64) bool {
	return uint64(int(config.LogStoreTimeout/time.Second)*config.TickPerSecond)+start < current
}

func (config *HAConfig) DnStoreExpired(start, current uint64) bool {
	return uint64(int(config.DnStoreTimeout/time.Second)*config.TickPerSecond)+start < current
}

func (config *HAConfig) ExpiredTick(start uint64, timeout time.Duration) uint64 {
	return uint64(timeout/time.Second)*uint64(config.TickPerSecond) + start
}
