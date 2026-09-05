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
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestArrowLoadDefaultsAndProgrammaticOptIn(t *testing.T) {
	var frontend FrontendParameters
	frontend.SetDefaultValues()
	require.True(t, frontend.ArrowLoad.Enabled)
	require.False(t, frontend.ArrowLoad.S3Enabled)
	require.False(t, frontend.ArrowLoad.DistributedEnabled)
	require.False(t, frontend.ArrowLoad.ForceMaterialize)

	parameters := NewArrowLoadParameters()
	parameters.S3Enabled = true
	parameters.DistributedEnabled = true
	parameters.SetDefaultValues()
	require.True(t, parameters.Enabled)
	require.True(t, parameters.S3Enabled)
	require.True(t, parameters.DistributedEnabled)
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
		{name: "section omitted", enabled: true},
		{
			name: "enable fields omitted", input: "[arrow-load]\nforce-materialize = true\n",
			enabled: true, forceMaterialize: true,
		},
		{
			name: "explicit opt in", input: `[arrow-load]
s3-enabled = true
distributed-enabled = true
`,
			enabled: true, s3Enabled: true, distributedEnabled: true,
		},
		{
			name: "one case-insensitive opt in", input: "[arrow-load]\nS3-ENABLED = true\n",
			enabled: true, s3Enabled: true,
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
