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
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/stretchr/testify/require"
)

func TestCatalogUpgradeObjectLimitConfig(t *testing.T) {
	const serviceID = "catalog-upgrade-limit-config"
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	t.Cleanup(func() { moruntime.CompareAndDeleteServiceBasedRuntime(serviceID, rt) })
	for _, tc := range []struct {
		name  string
		value uint64
		want  uint64
		bad   bool
	}{
		{name: "default", want: bootstrap.DefaultCatalogUpgradeObjectBytes},
		{name: "minimum", value: bootstrap.CatalogUpgradePageBytes, want: bootstrap.CatalogUpgradePageBytes},
		{name: "larger object", value: 128 << 20, want: 128 << 20},
		{name: "below page budget", value: bootstrap.CatalogUpgradePageBytes - 1, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{UUID: serviceID, UpgradeCatalogObjectLimit: tc.value}
			err := cfg.Validate()
			if tc.bad {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig), "%v", err)
				require.ErrorContains(t, err, "cn.upgrade-catalog-object-limit")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.UpgradeCatalogObjectLimit)
		})
	}
	var cfg Config
	_, err := toml.Decode("upgrade-catalog-object-limit = 134217728", &cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(128<<20), cfg.UpgradeCatalogObjectLimit)
}

func TestISCPConfigurationCadences(t *testing.T) {
	const serviceID = "iscp-cadence-config"
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	t.Cleanup(func() { moruntime.CompareAndDeleteServiceBasedRuntime(serviceID, rt) })
	for _, tc := range []struct {
		name, input   string
		sync, persist time.Duration
		wantErr       string
	}{
		{name: "inherit", input: `uuid = "iscp-cadence-config"`},
		{name: "configured", input: `uuid = "iscp-cadence-config"
[iscp]
sync-task-interval = "11ms"
index-flush-watermark-interval = "23ms"`, sync: 11 * time.Millisecond, persist: 23 * time.Millisecond},
		{name: "negative discovery", input: `uuid = "iscp-cadence-config"
[iscp]
sync-task-interval = "-1ns"`, wantErr: "cn.iscp.sync-task-interval"},
		{name: "negative persistence", input: `uuid = "iscp-cadence-config"
[iscp]
index-flush-watermark-interval = "-1ns"`, wantErr: "cn.iscp.index-flush-watermark-interval"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config
			_, err := toml.Decode(tc.input, &cfg)
			require.NoError(t, err)
			err = cfg.Validate()
			if tc.wantErr != "" {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig), "%v", err)
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.sync, cfg.ISCP.SyncTaskInterval.Duration)
			require.Equal(t, tc.persist, cfg.ISCP.IndexFlushWatermarkInterval.Duration)
		})
	}
}

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

func TestValidateServiceUUID(t *testing.T) {
	require.Panics(t, func() { _ = (&Config{}).Validate() })
	for _, id := range []string{".", "..", "../trace", "../shared2", "./cn", "cn/child", `cn\child`} {
		t.Run(id, func(t *testing.T) {
			err := (&Config{UUID: id}).Validate()
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig), "%v", err)
		})
	}
	require.NoError(t, validateCNServiceUUID(strings.Repeat("é", 63)+"x"))
}

func TestTaskAndTransactionDefaults(t *testing.T) {
	const serviceID = "task-transaction-defaults"
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	t.Cleanup(func() { moruntime.CompareAndDeleteServiceBasedRuntime(serviceID, rt) })
	for _, mode := range []struct {
		name        string
		consistency int64
	}{
		{name: "optimistic", consistency: 1},
		{name: "pessimistic", consistency: -1},
	} {
		for _, defaultsOnly := range []bool{false, true} {
			cfg := Config{UUID: serviceID}
			cfg.Txn.Mode = mode.name
			cfg.TaskRunner.Parallelism = 7
			if defaultsOnly {
				cfg.SetDefaultValue()
			} else {
				require.NoError(t, cfg.Validate())
			}
			require.Equal(t, 7, cfg.TaskRunner.QueryLimit)
			require.Equal(t, 256, cfg.TaskRunner.MaxWaitTasks)
			require.Equal(t, 10*time.Second, cfg.TaskRunner.FetchTimeout.Duration)
			require.EqualValues(t, mode.consistency, cfg.Txn.EnableCNBasedConsistency)
			require.EqualValues(t, -1, cfg.Txn.EnableRefreshExpression)
			require.Equal(t, 2*time.Minute, cfg.Txn.MaxActiveAges.Duration)
		}
	}
	cfg := Config{UUID: serviceID}
	cfg.HAKeeper.HeatbeatInterval.Duration = -time.Second
	require.Error(t, cfg.Validate())
	require.Zero(t, cfg.TaskRunner.Parallelism)
	require.Zero(t, cfg.Txn.MaxActiveAges.Duration)
}

func TestServiceAddressAndHAKeeperDefaults(t *testing.T) {
	const serviceID = "address-defaults"
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	t.Cleanup(func() { moruntime.CompareAndDeleteServiceBasedRuntime(serviceID, rt) })
	for _, defaultsOnly := range []bool{false, true} {
		cfg := Config{UUID: serviceID, ServiceAddress: "cn.example:6002"}
		if defaultsOnly {
			cfg.SetDefaultValue()
		} else {
			require.NoError(t, cfg.Validate())
		}
		require.Equal(t, defaultListenAddress, cfg.ListenAddress)
		require.Equal(t, "cn.example:6002", cfg.ServiceAddress)
		require.Equal(t, "cn.example:19998", cfg.QueryServiceConfig.Address.ServiceAddress)
		require.Equal(t, time.Second, cfg.HAKeeper.HeatbeatInterval.Duration)
		require.Equal(t, 3*time.Second, cfg.HAKeeper.HeatbeatTimeout.Duration)
		require.Equal(t, 30*time.Second, cfg.HAKeeper.DiscoveryTimeout.Duration)
	}
	cfg := Config{UUID: serviceID}
	cfg.HAKeeper.HeatbeatInterval.Duration = -time.Second
	require.Error(t, cfg.Validate())
	require.Equal(t, defaultListenAddress, cfg.ListenAddress)
	require.Equal(t, 3*time.Second, cfg.HAKeeper.HeatbeatTimeout.Duration)
	require.Empty(t, cfg.QueryServiceConfig.Address.ServiceAddress)
}
