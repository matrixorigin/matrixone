// Copyright 2026 Matrix Origin
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

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestArrowLoadDefaultsAndProgrammaticOptOut(t *testing.T) {
	var frontend FrontendParameters
	frontend.SetDefaultValues()
	require.True(t, frontend.ArrowLoad.Enabled)
	require.True(t, frontend.ArrowLoad.S3Enabled)
	require.True(t, frontend.ArrowLoad.DistributedEnabled)
	require.False(t, frontend.ArrowLoad.ForceMaterialize)

	parameters := NewArrowLoadParameters()
	parameters.Enabled = false
	parameters.S3Enabled = false
	parameters.DistributedEnabled = false
	parameters.SetDefaultValues()
	require.False(t, parameters.Enabled)
	require.False(t, parameters.S3Enabled)
	require.False(t, parameters.DistributedEnabled)
}

func TestLaunchTAEComposeProfileKeepsArrowLoadDefaultOn(t *testing.T) {
	for _, name := range []string{"cn-0.toml", "cn-1.toml"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", "..", "etc", "launch-tae-compose", "config", name))
			require.NoError(t, err)

			var decoded struct {
				CN struct {
					Frontend FrontendParameters `toml:"frontend"`
				} `toml:"cn"`
			}
			_, err = toml.Decode(string(data), &decoded)
			require.NoError(t, err)
			decoded.CN.Frontend.SetDefaultValues()

			require.True(t, decoded.CN.Frontend.ArrowLoad.Enabled)
			require.True(t, decoded.CN.Frontend.ArrowLoad.S3Enabled)
			require.True(t, decoded.CN.Frontend.ArrowLoad.DistributedEnabled)
		})
	}
}

func TestArrowLoadTOMLDefaultsAndExplicitOptOut(t *testing.T) {
	for _, test := range []struct {
		name               string
		input              string
		enabled            bool
		s3Enabled          bool
		distributedEnabled bool
		forceMaterialize   bool
	}{
		{name: "section omitted", enabled: true, s3Enabled: true, distributedEnabled: true},
		{
			name: "enable fields omitted", input: "[arrow-load]\nforce-materialize = true\n",
			enabled: true, s3Enabled: true, distributedEnabled: true, forceMaterialize: true,
		},
		{
			name: "explicit opt out", input: `[arrow-load]
enabled = false
s3-enabled = false
distributed-enabled = false
`,
		},
		{
			name: "one case-insensitive opt out", input: "[arrow-load]\nENABLED = false\nS3-ENABLED = false\n",
			distributedEnabled: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			decoded := struct {
				ArrowLoad ArrowLoadParameters `toml:"arrow-load"`
			}{ArrowLoad: *NewArrowLoadParameters()}
			_, err := toml.Decode(test.input, &decoded)
			require.NoError(t, err)
			decoded.ArrowLoad.SetDefaultValues()

			require.Equal(t, test.enabled, decoded.ArrowLoad.Enabled)
			require.Equal(t, test.s3Enabled, decoded.ArrowLoad.S3Enabled)
			require.Equal(t, test.distributedEnabled, decoded.ArrowLoad.DistributedEnabled)
			require.Equal(t, test.forceMaterialize, decoded.ArrowLoad.ForceMaterialize)

			// Service validation may apply defaults more than once. An explicit
			// false must remain an opt-out on every later pass.
			decoded.ArrowLoad.SetDefaultValues()
			require.Equal(t, test.enabled, decoded.ArrowLoad.Enabled)
			require.Equal(t, test.s3Enabled, decoded.ArrowLoad.S3Enabled)
			require.Equal(t, test.distributedEnabled, decoded.ArrowLoad.DistributedEnabled)
		})
	}
}

func TestArrowLoadRejectsConflictingGateKeys(t *testing.T) {
	var decoded struct {
		ArrowLoad ArrowLoadParameters `toml:"arrow-load"`
	}
	_, err := toml.Decode("[arrow-load]\nenabled = false\nENABLED = true\n", &decoded)
	require.ErrorContains(t, err, "conflicting enabled keys")
}
