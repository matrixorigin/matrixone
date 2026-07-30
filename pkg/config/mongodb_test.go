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

	"github.com/stretchr/testify/require"
)

func TestMongoDBParametersDefaultsAndValidation(t *testing.T) {
	var parameters MongoDBParameters
	parameters.SetDefaultValues()
	require.NoError(t, parameters.Validate(t.Context()))
	require.Positive(t, parameters.MaxConversionErrors)
	require.InDelta(t, 0.10, parameters.MaxConversionErrorRate, 0)
	parameters.MaxValueBytes = parameters.MaxBatchBytes + 1
	require.Error(t, parameters.Validate(t.Context()))
	parameters.MaxValueBytes = 1
	parameters.MaxConversionErrorRate = 1.1
	require.Error(t, parameters.Validate(t.Context()))
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
