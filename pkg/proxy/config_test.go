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
