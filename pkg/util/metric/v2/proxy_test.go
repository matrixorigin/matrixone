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

package v2

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestProxyConnectionMetricTypes(t *testing.T) {
	require.NoError(t, testutil.CollectAndCompare(
		ProxyConnectClosedCounter,
		strings.NewReader(`# HELP mo_proxy_connect_counter Total number of proxy connection events.
# TYPE mo_proxy_connect_counter counter
mo_proxy_connect_counter{type="closed"} 0
`),
		"mo_proxy_connect_counter",
	))
	require.NoError(t, testutil.GatherAndCompare(
		GetPrometheusGatherer(),
		strings.NewReader(`# HELP mo_proxy_connections_current Current number of active proxy client connections.
# TYPE mo_proxy_connections_current gauge
mo_proxy_connections_current 0
`),
		"mo_proxy_connections_current",
	))
}
