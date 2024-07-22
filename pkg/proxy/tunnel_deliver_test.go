// Copyright 2021 - 2024 Matrix Origin
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

package proxy

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/assert"
)

func TestTunnelDeliver_Deliver(t *testing.T) {
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()

	queue := make(chan *tunnel, 10)
	deliver := newTunnelDeliver(queue, logger)

	// a nil tunnel
	deliver.Deliver(nil, transferByRebalance)
	assert.Equal(t, 0, deliver.Count())

	tun1 := &tunnel{cc: &clientConn{connID: 12}}
	deliver.Deliver(tun1, transferByRebalance)
	assert.Equal(t, 0, deliver.Count())

	startTunnel := func(tun *tunnel) {
		tun.mu.Lock()
		defer tun.mu.Unlock()
		tun.mu.started = true
	}

	// start the tunnel
	startTunnel(tun1)

	deliver.Deliver(tun1, transferByRebalance)
	assert.Equal(t, 1, deliver.Count())

	// deliver again, but cannot deliver it.
	deliver.Deliver(tun1, transferByRebalance)
	assert.Equal(t, 1, deliver.Count())

	for i := 0; i < 15; i++ {
		tun := &tunnel{cc: &clientConn{connID: uint32(20 + i)}}
		startTunnel(tun)
		deliver.Deliver(tun, transferByRebalance)
		if i < 9 {
			assert.Equal(t, i+2, deliver.Count())
		} else {
			assert.Equal(t, 10, deliver.Count())
		}
	}
}
