// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package python

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientConfigRequiresExplicitUnisolatedOptIn(t *testing.T) {
	require.NoError(t, (&ClientConfig{}).Validate())
	require.NoError(t, (&ClientConfig{ServerAddress: "127.0.0.1:50051"}).Validate())

	config := &ClientConfig{Enabled: true, ServerAddress: "127.0.0.1:50051"}
	require.ErrorContains(t, config.Validate(), "allow-unisolated")

	config.AllowUnisolated = true
	config.RequestTimeout = time.Second
	require.NoError(t, config.Validate())
}

func TestClientConfigRejectsUnboundedInvocationAdmission(t *testing.T) {
	config := &ClientConfig{
		Enabled:              true,
		AllowUnisolated:      true,
		ServerAddress:        "127.0.0.1:50051",
		MaxActiveInvocations: -1,
	}
	require.ErrorContains(t, config.Validate(), "max active invocations")
	config.MaxActiveInvocations = 1 << 20
	require.NoError(t, config.Validate())
	config.MaxActiveInvocations++
	require.ErrorContains(t, config.Validate(), "max active invocations")
}

func TestClientConfigRejectsInvalidInvocationBudgets(t *testing.T) {
	config := &ClientConfig{
		Enabled: true, AllowUnisolated: true, ServerAddress: "127.0.0.1:50051",
		MaxInvocationRows: -1,
	}
	require.ErrorContains(t, config.Validate(), "max invocation rows")
	config.MaxInvocationRows = 1
	config.MaxInvocationResultBytes = -1
	require.ErrorContains(t, config.Validate(), "max invocation result bytes")
}

func TestClientConfigMatchesWorkerHandlerTimeoutContract(t *testing.T) {
	config := &ClientConfig{
		Enabled: true, AllowUnisolated: true, ServerAddress: "127.0.0.1:50051",
		RequestTimeout: maxHandlerTimeout,
	}
	require.NoError(t, config.Validate())

	config.RequestTimeout = maxHandlerTimeout + time.Nanosecond
	require.ErrorContains(t, config.Validate(), "request timeout")
}
