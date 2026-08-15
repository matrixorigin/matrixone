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

func TestMongoDBParametersProgrammaticOptOutSurvivesRepeatedDefaulting(t *testing.T) {
	parameters := NewMongoDBParameters()
	parameters.SetDefaultValues()
	parameters.Enable = false

	parameters.SetDefaultValues()
	require.False(t, parameters.Enable)
}

func TestMongoDBParametersEnablementTOMLDefaults(t *testing.T) {
	for _, tc := range []struct {
		name        string
		input       string
		wantEnabled bool
	}{
		{name: "mongodb section omitted", input: "", wantEnabled: true},
		{name: "enable omitted", input: "[mongodb]\nallow-loopback = false\n", wantEnabled: true},
		{name: "explicit disable", input: "[mongodb]\nenable = false\n", wantEnabled: false},
		{name: "field case explicit disable", input: "[mongodb]\nEnable = false\n", wantEnabled: false},
		{name: "upper case explicit disable", input: "[mongodb]\nENABLE = false\n", wantEnabled: false},
		{name: "mixed case explicit disable", input: "[mongodb]\neNaBlE = false\n", wantEnabled: false},
		{name: "explicit enable", input: "[mongodb]\nenable = true\n", wantEnabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parameters := struct {
				MongoDB MongoDBParameters `toml:"mongodb"`
			}{MongoDB: *NewMongoDBParameters()}
			_, err := toml.Decode(tc.input, &parameters)
			require.NoError(t, err)
			parameters.MongoDB.SetDefaultValues()
			require.Equal(t, tc.wantEnabled, parameters.MongoDB.Enable)
			require.False(t, parameters.MongoDB.EnablePerAccount)
			require.False(t, parameters.MongoDB.AllowLoopback)
			require.Empty(t, parameters.MongoDB.AllowedHostSuffixes)
			require.Empty(t, parameters.MongoDB.AllowedCIDRs)
			require.NoError(t, parameters.MongoDB.Validate(t.Context()))

			// Re-running defaulting models validation/reload paths and must not
			// overwrite an explicit false value.
			parameters.MongoDB.SetDefaultValues()
			require.Equal(t, tc.wantEnabled, parameters.MongoDB.Enable)
		})
	}
}

func TestMongoDBParametersRejectConflictingEnableKeys(t *testing.T) {
	var parameters struct {
		MongoDB MongoDBParameters `toml:"mongodb"`
	}
	_, err := toml.Decode("[mongodb]\nenable = false\nEnable = true\n", &parameters)
	require.ErrorContains(t, err, "conflicting enable keys")
}

func TestMongoDBParametersUnmarshalPreservesOtherSettings(t *testing.T) {
	var parameters struct {
		MongoDB MongoDBParameters `toml:"mongodb"`
	}
	_, err := toml.Decode(`[mongodb]
enable = false
enable-per-account = true
allowed-accounts = [7, 8]
allow-loopback = true
allowed-host-suffixes = ["mongo.example"]
allowed-cidrs = ["10.0.0.0/8"]
connect-timeout = "3s"
max-pool-size = 17
`, &parameters)
	require.NoError(t, err)
	parameters.MongoDB.SetDefaultValues()
	require.False(t, parameters.MongoDB.Enable)
	require.True(t, parameters.MongoDB.EnablePerAccount)
	require.Equal(t, []uint32{7, 8}, parameters.MongoDB.AllowedAccounts)
	require.True(t, parameters.MongoDB.AllowLoopback)
	require.Equal(t, []string{"mongo.example"}, parameters.MongoDB.AllowedHostSuffixes)
	require.Equal(t, []string{"10.0.0.0/8"}, parameters.MongoDB.AllowedCIDRs)
	require.Equal(t, 3*time.Second, parameters.MongoDB.ConnectTimeout.Duration)
	require.Equal(t, uint64(17), parameters.MongoDB.MaxPoolSize)
	require.NoError(t, parameters.MongoDB.Validate(t.Context()))
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
