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

package config

import "time"

const (
	DefaultTickPerSecond   = 10
	DefaultLogStoreTimeout = 5 * time.Minute
	DefaultDnStoreTimeout  = 10 * time.Second
)

type TimeoutConfig struct {
	TickPerSecond int

	LogStoreTimeout time.Duration

	DnStoreTimeout time.Duration
}

func DefaultTimeoutConfig() *TimeoutConfig {
	return &TimeoutConfig{
		TickPerSecond:   DefaultTickPerSecond,
		LogStoreTimeout: DefaultLogStoreTimeout,
		DnStoreTimeout:  DefaultDnStoreTimeout,
	}
}

func (config *TimeoutConfig) SetTickPerSecond(tickPerSecond int) *TimeoutConfig {
	config.TickPerSecond = tickPerSecond
	return config
}

func (config *TimeoutConfig) SetLogStoreTimeout(timeout time.Duration) *TimeoutConfig {
	config.LogStoreTimeout = timeout
	return config
}

func (config *TimeoutConfig) SetDnStoreTimeout(timeout time.Duration) *TimeoutConfig {
	config.DnStoreTimeout = timeout
	return config
}

func (config *TimeoutConfig) LogStoreExpired(start, current uint64) bool {
	return uint64(int(config.LogStoreTimeout/time.Second)*config.TickPerSecond)+start < current
}

func (config *TimeoutConfig) DnStoreExpired(start, current uint64) bool {
	return uint64(int(config.DnStoreTimeout/time.Second)*config.TickPerSecond)+start < current
}
