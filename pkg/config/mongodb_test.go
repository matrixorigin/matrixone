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
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestMongoDBParametersDefaultsAndValidation(t *testing.T) {
	var parameters MongoDBParameters
	parameters.SetDefaultValues()
	require.NoError(t, parameters.Validate(t.Context()))
	require.True(t, parameters.Enable)
	require.False(t, parameters.EnablePerAccount)
	require.False(t, parameters.AllowLoopback)
	require.Empty(t, parameters.AllowedHostSuffixes)
	require.Empty(t, parameters.AllowedCIDRs)
	require.Positive(t, parameters.MaxConversionErrors)
	require.InDelta(t, 0.10, parameters.MaxConversionErrorRate, 0)
	parameters.MaxValueBytes = parameters.MaxBatchBytes + 1
	require.Error(t, parameters.Validate(t.Context()))
	parameters.MaxValueBytes = 1
	parameters.MaxConversionErrorRate = 1.1
	require.Error(t, parameters.Validate(t.Context()))
}

func TestMongoDBEnableDefaultPreservesExplicitDisable(t *testing.T) {
	var omitted FrontendParameters
	_, err := toml.Decode("", &omitted)
	require.NoError(t, err)
	omitted.SetDefaultValues()
	require.True(t, omitted.MongoDB.Enable)
	require.False(t, omitted.MongoDB.EnablePerAccount)

	var disabled FrontendParameters
	_, err = toml.Decode(`[mongodb]
enable = false
enable-per-account = true
allowed-accounts = [7, 8]
connect-timeout = "3s"
max-pool-size = 20
max-conversion-error-rate = 0.2
`, &disabled)
	require.NoError(t, err)
	disabled.SetDefaultValues()
	require.False(t, disabled.MongoDB.Enable)
	require.True(t, disabled.MongoDB.EnablePerAccount)
	require.Equal(t, []uint32{7, 8}, disabled.MongoDB.AllowedAccounts)
	require.Equal(t, 3*time.Second, disabled.MongoDB.ConnectTimeout.Duration)
	require.Equal(t, uint64(20), disabled.MongoDB.MaxPoolSize)
	require.InDelta(t, 0.2, disabled.MongoDB.MaxConversionErrorRate, 0)

	// A reload that keeps the MongoDB section but omits enable restores the
	// default and must not retain the previous explicit opt-out.
	_, err = toml.Decode("[mongodb]\nallowed-cidrs = [\"10.0.0.0/8\"]\n", &disabled)
	require.NoError(t, err)
	disabled.SetDefaultValues()
	require.True(t, disabled.MongoDB.Enable)
	require.False(t, disabled.MongoDB.EnablePerAccount)
	require.Empty(t, disabled.MongoDB.AllowedAccounts)
	require.Equal(t, []string{"10.0.0.0/8"}, disabled.MongoDB.AllowedCIDRs)
}

func TestMongoDBParametersCaseInsensitiveEnablement(t *testing.T) {
	for _, input := range []string{
		"[mongodb]\\nEnable = false\\n",
		"[mongodb]\\nENABLE = false\\n",
		"[mongodb]\\neNaBlE = false\\n",
	} {
		var parameters FrontendParameters
		_, err := toml.Decode(input, &parameters)
		require.NoError(t, err)
		parameters.SetDefaultValues()
		require.False(t, parameters.MongoDB.Enable)
	}
}

func TestMongoDBParametersRejectConflictingEnableKeys(t *testing.T) {
	var parameters FrontendParameters
	_, err := toml.Decode("[mongodb]\\nenable = false\\nEnable = true\\n", &parameters)
	require.ErrorContains(t, err, "conflicting enable keys")
}

func TestMongoDBParametersRejectMalformedEndpointPolicy(t *testing.T) {
	var parameters MongoDBParameters
	parameters.SetDefaultValues()
	parameters.AllowedHostSuffixes = []string{"mongo.example"}
	parameters.AllowedCIDRs = []string{"10.0.0.0/8"}
	require.NoError(t, parameters.Validate(t.Context()))

	parameters.AllowedHostSuffixes = []string{"*.mongo.example"}
	require.ErrorContains(t, parameters.Validate(t.Context()), "host suffix")
	parameters.AllowedHostSuffixes = []string{"mongo.example"}
	parameters.AllowedCIDRs = []string{"not-a-cidr"}
	require.ErrorContains(t, parameters.Validate(t.Context()), "CIDR")
}
