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

package cnservice

import (
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestValidateRejectsRemovedMemoryEngines(t *testing.T) {
	for _, engineType := range []EngineType{"memory", "non-distributed-memory"} {
		t.Run(string(engineType), func(t *testing.T) {
			cfg := Config{UUID: "cn1"}
			cfg.Engine.Type = engineType
			require.ErrorContains(t, cfg.Validate(), "unsupported CN engine")
		})
	}
}

func TestValidateHeartbeatDurations(t *testing.T) {
	for name, interval := range map[string]time.Duration{
		"negative interval":       -time.Nanosecond,
		"exceeds progress budget": logservice.ScheduleCommandPollInterval + time.Nanosecond,
	} {
		t.Run(name, func(t *testing.T) {
			cfg := Config{UUID: "cn1"}
			cfg.HAKeeper.HeatbeatInterval.Duration = interval
			require.ErrorContains(t, cfg.Validate(), "hakeeper heartbeat interval")
		})
	}
	cfg := Config{UUID: "cn1"}
	cfg.HAKeeper.HeatbeatTimeout.Duration = -time.Nanosecond
	require.ErrorContains(t, cfg.Validate(), "hakeeper heartbeat timeout")
}

func TestValidatePythonUdfClientContract(t *testing.T) {
	service := "python-udf-config-" + t.Name()
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
	moruntime.SetupServiceBasedRuntime(service, rt)

	disabled := Config{UUID: service}
	require.NoError(t, disabled.Validate())

	requiresOptIn := Config{UUID: service}
	requiresOptIn.PythonUdfClient.Enabled = true
	require.ErrorContains(t, requiresOptIn.Validate(), "allow-unisolated")

	requiresAddress := Config{UUID: service}
	requiresAddress.PythonUdfClient.Enabled = true
	requiresAddress.PythonUdfClient.AllowUnisolated = true
	require.ErrorContains(t, requiresAddress.Validate(), "missing python udf address")

	valid := Config{UUID: service}
	valid.PythonUdfClient.Enabled = true
	valid.PythonUdfClient.AllowUnisolated = true
	valid.PythonUdfClient.ServerAddress = "127.0.0.1:50051"
	require.NoError(t, valid.Validate())
}
