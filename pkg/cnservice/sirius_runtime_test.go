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
	var disabled SiriusConfig
	require.NoError(t, disabled.validate())

	enabled := SiriusConfig{Enabled: true}
	err := enabled.validate()
	require.ErrorContains(t, err, "missing Sirius flight-address")
	require.Equal(t, uint64(64<<20), enabled.MaxBatchBytes)
	require.Equal(t, 15*time.Minute, enabled.RequestTimeout.Duration)
	require.Equal(t, 30*time.Second, enabled.CleanupTimeout.Duration)
	require.Equal(t, 15*time.Minute+30*time.Second, enabled.LeaseTTL.Duration)

	enabled.FlightAddress = "sidecar:32010"
	enabled.FlightServerName = "sidecar.internal"
	enabled.FlightClientCertPath = "client.crt"
	enabled.FlightClientKeyPath = "client.key"
	enabled.FlightServerCAPath = "sidecar-ca.crt"
	enabled.ResolverAddress = "127.0.0.1:32011"
	enabled.ResolverServerCertPath = "resolver.crt"
	enabled.ResolverServerKeyPath = "resolver.key"
	enabled.ResolverClientCAPath = "resolver-client-ca.crt"
	enabled.ResolverClientCertPath = "sidecar-client.crt"
	enabled.DataDir = "/var/lib/matrixone"
	require.NoError(t, enabled.validate())

	enabled.LeaseTTL.Duration = enabled.RequestTimeout.Duration
	require.ErrorContains(t, enabled.validate(), "invalid Sirius transport limits")
}
