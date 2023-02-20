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

package checkpoint

import "time"

type Option func(*runner)

func WithObserver(o Observer) Option {
	return func(r *runner) {
		r.observers.add(o)
	}
}

func WithCollectInterval(interval time.Duration) Option {
	return func(r *runner) {
		r.options.collectInterval = interval
	}
}

func WithFlushInterval(interval time.Duration) Option {
	return func(r *runner) {
		r.options.maxFlushInterval = interval
	}
}

func WithMinCount(count int) Option {
	return func(r *runner) {
		r.options.minCount = count
	}
}

func WithMinIncrementalInterval(interval time.Duration) Option {
	return func(r *runner) {
		r.options.minIncrementalInterval = interval
	}
}

func WithGlobalMinCount(count int) Option {
	return func(r *runner) {
		r.options.globalMinCount = count
	}
}

func WithForceFlushTimeout(to time.Duration) Option {
	return func(r *runner) {
		r.options.forceFlushTimeout = to
	}
}

func WithForceFlushCheckInterval(interval time.Duration) Option {
	return func(r *runner) {
		r.options.forceFlushCheckInterval = interval
	}
}

func WithGlobalVersionInterval(interval time.Duration) Option {
	return func(r *runner) {
		r.options.globalVersionInterval = interval
	}
}
