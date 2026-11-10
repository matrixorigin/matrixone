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

package cnservice

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSiriusConfigIsOptInAndFailClosed(t *testing.T) {
	disabled := SiriusConfig{}
	require.NoError(t, disabled.validate())
	require.False(t, disabled.Enabled)
	enabled := SiriusConfig{Enabled: true, NativeConfigPath: "sirius.yaml"}
	if err := validateSiriusEmbeddedBuild(); err != nil {
		require.ErrorContains(t, enabled.validate(), "MO_SIRIUS=1")
		return
	}
	require.NoError(t, enabled.validate())
	require.Equal(t, uint32(2), enabled.GPUStreams)
	require.Equal(t, uint32(16), enabled.MaxWaitingQueries)
	require.Equal(t, 15*time.Minute, enabled.RequestTimeout.Duration)
	require.Equal(t, 30*time.Second, enabled.CleanupTimeout.Duration)
	for _, change := range []func(*SiriusConfig){
		func(c *SiriusConfig) { c.RequestTimeout.Duration = -time.Second },
		func(c *SiriusConfig) { c.CleanupTimeout.Duration = -time.Second },
		func(c *SiriusConfig) { c.RequestTimeout.Duration = time.Duration(1<<63 - 1) },
	} {
		invalid := enabled
		change(&invalid)
		require.ErrorContains(t, invalid.validate(), "invalid Sirius request or cleanup timeout")
	}
}
