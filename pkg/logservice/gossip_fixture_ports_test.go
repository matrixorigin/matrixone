// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"net"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/require"
)

func TestGossipFixtureConfigPortsUseListenerRange(t *testing.T) {
	first, limit, err := testPortRange()
	require.NoError(t, err)
	rt := runtime.DefaultRuntime()
	configs, err := makeGossipFixtureConfigs(t, rt, 3)
	t.Cleanup(func() { require.NoError(t, closeGossipFixture(rt, configs, nil)) })
	require.NoError(t, err)
	require.Len(t, configs, 3)
	ports := make(map[int]struct{})
	for _, cfg := range configs {
		for _, addr := range []string{cfg.GossipListenAddr(), cfg.RaftListenAddr(), cfg.LogServiceListenAddr()} {
			_, value, err := net.SplitHostPort(addr)
			require.NoError(t, err)
			port, err := strconv.Atoi(value)
			require.NoError(t, err)
			require.GreaterOrEqual(t, port, first)
			require.Less(t, port, limit)
			require.NotContains(t, ports, port)
			ports[port] = struct{}{}
		}
	}
	require.Len(t, ports, 9)
}
