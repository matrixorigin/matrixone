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

func TestClientConfigRejectsEveryOutOfRangeBudget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ClientConfig)
		want   string
	}{
		{name: "missing worker address", mutate: func(c *ClientConfig) { c.ServerAddress = "" }, want: "missing python udf address"},
		{name: "negative input bytes", mutate: func(c *ClientConfig) { c.MaxBatchBytes = -1 }, want: "max batch bytes"},
		{name: "input bytes exceed wire bound", mutate: func(c *ClientConfig) { c.MaxBatchBytes = 1<<30 + 1 }, want: "max batch bytes"},
		{name: "negative input rows", mutate: func(c *ClientConfig) { c.MaxBatchRows = -1 }, want: "max batch rows"},
		{name: "input rows exceed bound", mutate: func(c *ClientConfig) { c.MaxBatchRows = 1<<30 + 1 }, want: "max batch rows"},
		{name: "invocation rows exceed bound", mutate: func(c *ClientConfig) { c.MaxInvocationRows = 1<<32 + 1 }, want: "max invocation rows"},
		{name: "result bytes exceed bound", mutate: func(c *ClientConfig) { c.MaxInvocationResultBytes = 1<<40 + 1 }, want: "max invocation result bytes"},
		{name: "active slots exceed bound", mutate: func(c *ClientConfig) { c.MaxActiveInvocations = 1<<20 + 1 }, want: "max active invocations"},
		{name: "negative request timeout", mutate: func(c *ClientConfig) { c.RequestTimeout = -time.Nanosecond }, want: "request timeout"},
		{name: "terminal entries exceed bound", mutate: func(c *ClientConfig) { c.MaxTerminalEntries = 1<<30 + 1 }, want: "max terminal entries"},
		{name: "negative terminal bytes", mutate: func(c *ClientConfig) { c.MaxTerminalBytes = -1 }, want: "max terminal bytes"},
		{name: "terminal bytes exceed bound", mutate: func(c *ClientConfig) { c.MaxTerminalBytes = 1<<40 + 1 }, want: "max terminal bytes"},
		{name: "negative tombstone lifetime", mutate: func(c *ClientConfig) { c.TerminalRecordTTL = -time.Nanosecond }, want: "terminal record ttl"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := &ClientConfig{
				Enabled: true, AllowUnisolated: true, ServerAddress: "127.0.0.1:50051",
			}
			tc.mutate(config)
			require.ErrorContains(t, config.Validate(), tc.want)
		})
	}
}

func TestWorkerConfigRequiresAddressAndPath(t *testing.T) {
	config := &Config{Path: "/opt/matrixone/pkg/udf/python/worker/worker.py"}
	require.ErrorContains(t, config.Validate(), "missing python udf address")
	config.Address = "127.0.0.1:50051"
	config.Path = ""
	require.ErrorContains(t, config.Validate(), "missing python udf path")
	config.Path = "/opt/matrixone/pkg/udf/python/worker/worker.py"
	require.NoError(t, config.Validate())
}
