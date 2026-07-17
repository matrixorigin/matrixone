// Copyright 2021 - 2023 Matrix Origin
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestFillDefault(t *testing.T) {
	c := Config{}
	c.FillDefault()
	require.NotEqual(t, "", c.ListenAddress)
	require.NotEqual(t, 0, c.RebalanceInterval.Duration)
	require.NotEqual(t, true, c.RebalanceDisabled)
	require.NotEqual(t, 0, c.RebalanceTolerance)
	require.Less(t, c.RebalanceTolerance, float64(1))
	require.NotEqual(t, 0, c.Cluster.RefreshInterval.Duration)
	require.Equal(t, defaultCNHealthCheckBaseCooldown, c.CNHealthCheckBaseCooldown.Duration)
	require.Equal(t, defaultCNHealthCheckMaxCooldown, c.CNHealthCheckMaxCooldown.Duration)
	require.Equal(t, defaultCNHealthFailThreshold, c.CNHealthCheckFailThreshold)
	require.Equal(t, defaultClientHandshakeTimeout, c.ClientHandshakeTimeout.Duration)
	require.Equal(t, defaultMaxConnections, c.MaxConnections)
	require.Equal(t, defaultMaxConnectionsPerTenant, c.MaxConnectionsPerTenant)
	require.Equal(t, defaultProtocolMemoryLimit, c.ProtocolMemoryLimit)
	require.Equal(t, defaultClientHandshakePacketLimit, c.ClientHandshakePacketLimit)

	c = Config{MaxConnections: 1000}
	c.FillDefault()
	require.Equal(t, 1000, c.MaxConnections)
	require.Equal(t, 1000, c.MaxConnectionsPerTenant)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{{
		name: "empty",
		cfg:  Config{},
	}, {
		name: "negative client handshake timeout",
		cfg: Config{
			ClientHandshakeTimeout: toml.Duration{Duration: -time.Second},
		},
		wantErr: true,
	}, {
		name: "negative global connection limit",
		cfg: Config{
			MaxConnections: -1,
		},
		wantErr: true,
	}, {
		name: "negative tenant connection limit",
		cfg: Config{
			MaxConnectionsPerTenant: -1,
		},
		wantErr: true,
	}, {
		name: "tenant limit exceeds global limit",
		cfg: Config{
			MaxConnections:          10,
			MaxConnectionsPerTenant: 11,
		},
		wantErr: true,
	}, {
		name: "protocol memory below fixed buffers",
		cfg: Config{
			MaxConnections:          10,
			MaxConnectionsPerTenant: 10,
			ProtocolMemoryLimit:     1,
		},
		wantErr: true,
	}, {
		name: "handshake packet limit below protocol minimum",
		cfg: Config{
			ClientHandshakePacketLimit: minimumClientHandshakePacketLimit - 1,
		},
		wantErr: true,
	}, {
		name: "handshake packet limit above protocol maximum",
		cfg: Config{
			ClientHandshakePacketLimit: maximumClientHandshakePacketLimit + 1,
		},
		wantErr: true,
	}, {
		name: "plugin enabled but no backend",
		cfg: Config{
			Plugin: &PluginConfig{},
		},
		wantErr: true,
	}, {
		name: "plugin enabled but no timeout",
		cfg: Config{
			Plugin: &PluginConfig{
				Backend: "test",
			},
		},
		wantErr: true,
	}, {
		name: "plugin valid",
		cfg: Config{
			Plugin: &PluginConfig{
				Backend: "test",
				Timeout: time.Second,
			},
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
