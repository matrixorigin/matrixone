// Copyright 2022 Matrix Origin
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

// this file contains test utils. Name this file "*_test.go" to make
// compiler ignore it

package mometric

import (
	"context"
	"sync"
	"sync/atomic"
)

// some tests will modify global variables, and something weird
// may happen if they run concurrently.
// use this mutex to make those tests execute sequentially
var configMu *sync.Mutex = new(sync.Mutex)

func withModifiedConfig(f func()) {
	configMu.Lock()
	defer configMu.Unlock()
	f()
}

func makeDummyClock(startOffset int64) func() int64 {
	var tick = startOffset - 1
	return func() int64 {
		return atomic.AddInt64(&tick, 1)
	}
}

type dummySwitch struct{}

func (dummySwitch) Start(ctx context.Context) bool             { return true }
func (dummySwitch) Stop(graceful bool) (<-chan struct{}, bool) { return nil, false }
