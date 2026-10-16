// Copyright 2021-2024 Matrix Origin
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

package embed

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTNConfig(t *testing.T) {
	data := `
	# service node type, [TN|CN|LOG]
	service-type = "TN"

	[log]
	level = "debug"
	format = "json"
	max-size = 512

	[hakeeper-client]
	backend-read-timeout = "20s"
	service-addresses = [
		"1",
		"2"
	]

	[[fileservice]]
	# local fileservice instance, used to store TAE Data and TNStore metadata.
	name = "local"
	# use disk as fileservice backend
	backend = "DISK"
	# set the directory used by DISK backend. There must has a file named "thisisalocalfileservicedir"
	# in the data dir
	data-dir = "data dir"

	[[fileservice]]
	# s3 fileservice instance, used to store data.
	name = "SHARED"
	# use disk as fileservice backend.
	backend = "DISK"
	# set the directory used by DISK backend. There must has a file named "thisisalocalfileservicedir"
	# in the data dir
	data-dir = "data dir"

	[tn.Txn.Storage]
	# txn storage backend implementation. [TAE|MEMKV]
	backend = "MEMKV"
	`
	cfg := &ServiceConfig{}
	err := parseFromString(data, cfg)
	assert.NoError(t, err)
	assert.Equal(t, tnservice.StorageMEMKV, cfg.getTNServiceConfig().Txn.Storage.Backend)
	assert.Equal(t, 2, len(cfg.FileServices))
	assert.Equal(t, 20*time.Second, cfg.HAKeeperClient.BackendReadTimeout.Duration)
	assert.Equal(t, "local", cfg.FileServices[0].Name)
	assert.Equal(t, defines.SharedFileServiceName, cfg.FileServices[1].Name)
	assert.Equal(t, 2, len(cfg.getTNServiceConfig().HAKeeper.ClientConfig.ServiceAddresses))
}

func TestFileServiceFactory(t *testing.T) {
	ctx := context.Background()

	c := &ServiceConfig{}
	c.FileServices = append(c.FileServices, fileservice.Config{
		Name:    "a",
		Backend: "MEM",
	})
	c.FileServices = append(c.FileServices, fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "MEM",
	})
	c.FileServices = append(c.FileServices, fileservice.Config{
		Name:    defines.SharedFileServiceName,
		Backend: "MEM",
	})
	c.FileServices = append(c.FileServices, fileservice.Config{
		Name:    defines.ETLFileServiceName,
		Backend: "DISK-ETL",
	})
	c.FileServices = append(c.FileServices, fileservice.Config{
		Name:    defines.TmpFileServiceName,
		Backend: "DISK-TMP",
	})

	fs, err := c.createFileService(ctx, metadata.ServiceType_CN, "")
	assert.NoError(t, err)
	assert.NotNil(t, fs)
}

func TestFileServiceFactoryScopesMemoryCacheMetrics(t *testing.T) {
	ctx := context.Background()

	newConfig := func(capacity toml.ByteSize) *ServiceConfig {
		return &ServiceConfig{FileServices: []fileservice.Config{
			{
				Name:    defines.LocalFileServiceName,
				Backend: "DISK",
				DataDir: t.TempDir(),
				Cache: fileservice.CacheConfig{
					MemoryCapacity: &capacity,
				},
			},
			{
				Name:    defines.SharedFileServiceName,
				Backend: "DISK",
				DataDir: t.TempDir(),
				Cache: fileservice.CacheConfig{
					MemoryCapacity: &capacity,
				},
			},
			{
				Name:    defines.ETLFileServiceName,
				Backend: "DISK-ETL",
			},
			{
				Name:    defines.TmpFileServiceName,
				Backend: "DISK-TMP",
			},
		}}
	}

	firstCapacity := toml.ByteSize(1 << 20)
	first, err := newConfig(firstCapacity).createFileService(ctx, metadata.ServiceType_CN, "scope-node-a")
	require.NoError(t, err)
	t.Cleanup(func() { first.Close(ctx) })

	secondCapacity := toml.ByteSize(2 << 20)
	second, err := newConfig(secondCapacity).createFileService(ctx, metadata.ServiceType_CN, "scope-node-b")
	require.NoError(t, err)
	t.Cleanup(func() { second.Close(ctx) })

	_, firstGauge := metric.GetFsCacheBytesGaugeWithScope(
		fileservice.ServiceMetricScope(metadata.ServiceType_CN.String(), "scope-node-a"),
		defines.SharedFileServiceName,
		"mem",
	)
	_, secondGauge := metric.GetFsCacheBytesGaugeWithScope(
		fileservice.ServiceMetricScope(metadata.ServiceType_CN.String(), "scope-node-b"),
		defines.SharedFileServiceName,
		"mem",
	)
	require.Equal(t, float64(firstCapacity), testutil.ToFloat64(firstGauge))
	require.Equal(t, float64(secondCapacity), testutil.ToFloat64(secondGauge))
}

func TestDefaultTmpFileServiceUsesServiceDataDir(t *testing.T) {
	c := newServiceConfig()
	c.DataDir = t.TempDir()
	assert.NoError(t, c.setDefaultValue())

	for _, fs := range c.FileServices {
		if fs.Name == defines.TmpFileServiceName {
			assert.Equal(t, c.defaultFileServiceDataDir(defines.TmpFileServiceName), fs.DataDir)
			return
		}
	}
	t.Fatal("default TMP file service was not configured")
}

func TestResolveGossipSeedAddresses(t *testing.T) {
	for _, tt := range []struct {
		name    string
		addrs   []string
		results []string
		lookups []string
		wantErr bool
	}{
		{
			name:    "one IPv4 per host, preserving ports",
			addrs:   []string{"seed.test:32001", "seed.test:32011", "127.0.0.1:32021"},
			results: []string{"192.0.2.1:32001", "192.0.2.1:32011", "127.0.0.1:32021"},
			lookups: []string{"seed.test", "seed.test", "127.0.0.1"},
		},
		{
			name:    "lookup failure retains original hostname",
			addrs:   []string{"seed.test:32001", "missing.test:32011"},
			results: []string{"192.0.2.1:32001", "missing.test:32011"},
			lookups: []string{"seed.test", "missing.test"},
		},
		{
			name:  "IPv6 is filtered from mixed results",
			addrs: []string{"dual.test:32001"}, results: []string{"192.0.2.2:32001"},
			lookups: []string{"dual.test"},
		},
		{
			name:    "empty successful result is not a lookup failure",
			addrs:   []string{"seed.test:32001", "empty.test:32011"},
			lookups: []string{"seed.test", "empty.test"}, wantErr: true,
		},
		{
			name:  "IPv6 only is rejected",
			addrs: []string{"v6.test:32001"}, lookups: []string{"v6.test"}, wantErr: true,
		},
		{
			name:  "multiple IPv4 addresses are rejected",
			addrs: []string{"many.test:32001"}, lookups: []string{"many.test"}, wantErr: true,
		},
		{
			name:  "malformed address is rejected before lookup",
			addrs: []string{"missing-port.test"}, wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := ServiceConfig{LogService: logservice.Config{
				GossipSeedAddresses: append([]string(nil), tt.addrs...),
			}}
			var lookups []string
			lookup := func(host string) ([]net.IP, error) {
				lookups = append(lookups, host)
				switch host {
				case "seed.test":
					return []net.IP{net.ParseIP("192.0.2.1")}, nil
				case "127.0.0.1":
					return []net.IP{net.ParseIP(host)}, nil
				case "dual.test":
					return []net.IP{net.ParseIP("::1"), net.ParseIP("192.0.2.2")}, nil
				case "missing.test":
					return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
				case "empty.test":
					return nil, nil
				case "v6.test":
					return []net.IP{net.ParseIP("::1")}, nil
				case "many.test":
					return []net.IP{net.ParseIP("192.0.2.1"), net.ParseIP("192.0.2.2")}, nil
				default:
					t.Fatalf("unexpected lookup: %s", host)
					return nil, nil
				}
			}
			err := cfg.resolveGossipSeedAddressesWithLookup(lookup)
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, tt.addrs, cfg.LogService.GossipSeedAddresses, "failed resolution must not partially rewrite config")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.results, cfg.LogService.GossipSeedAddresses)
			}
			require.Equal(t, tt.lookups, lookups)
		})
	}
}

func TestGossipSeedAddressesAreResolved(t *testing.T) {
	data := `
	service-type = "LOG"

[log]
level = "debug"
format = "json"
max-size = 512

[logservice]
deployment-id = 1
uuid = "9c4dccb4-4d3c-41f8-b482-5251dc7a41bf"
gossip-seed-addresses = [
  "127.0.0.1:32002",
]

[logservice.BootstrapConfig]
bootstrap-cluster = true
num-of-log-shards = 1
num-of-tn-shards = 1
num-of-log-shard-replicas = 1
init-hakeeper-members = [
  "131072:9c4dccb4-4d3c-41f8-b482-5251dc7a41bf",
]

[hakeeper-client]
service-addresses = [
  "127.0.0.1:32000",
]
	`
	cfg := &ServiceConfig{}
	err := parseFromString(data, cfg)
	assert.NoError(t, err)
	assert.NoError(t, cfg.validate())
	assert.NoError(t, cfg.resolveGossipSeedAddresses())
	assert.Equal(t, 1, len(cfg.LogService.GossipSeedAddresses))
	assert.Equal(t, "127.0.0.1:32002", cfg.LogService.GossipSeedAddresses[0])
}

func TestDumpCommonConfig(t *testing.T) {
	cfg1 := newServiceConfig()
	_, err := dumpCommonConfig(cfg1)
	assert.NoError(t, err)
}

func TestMongoDBEnablementConfigDefaults(t *testing.T) {
	for _, tc := range []struct {
		name        string
		input       string
		wantEnabled bool
	}{
		{name: "omitted", input: "", wantEnabled: true},
		{name: "explicit disable", input: "[cn.frontend.mongodb]\nenable = false\n", wantEnabled: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newServiceConfig()
			assert.NoError(t, parseFromString(tc.input, &cfg))
			assert.NoError(t, cfg.setDefaultValue())
			assert.Equal(t, tc.wantEnabled, cfg.CN.Frontend.MongoDB.Enable)
			assert.False(t, cfg.CN.Frontend.MongoDB.EnablePerAccount)
			assert.False(t, cfg.CN.Frontend.MongoDB.AllowLoopback)
			assert.Empty(t, cfg.CN.Frontend.MongoDB.AllowedHostSuffixes)
			assert.Empty(t, cfg.CN.Frontend.MongoDB.AllowedCIDRs)

			cfg.CN.SetDefaultValue()
			assert.Equal(t, tc.wantEnabled, cfg.CN.Frontend.MongoDB.Enable)
		})
	}
}

func TestMongoDBProgrammaticOptOutSurvivesCNDefaulting(t *testing.T) {
	op := &operator{cfg: newServiceConfig()}
	require.True(t, op.cfg.CN.Frontend.MongoDB.Enable)

	// Model the public WithPreStart callback path.
	op.Adjust(func(cfg *ServiceConfig) {
		cfg.CN.Frontend.MongoDB.Enable = false
	})

	serviceCfg := op.GetServiceConfig()
	cfg := serviceCfg.getCNServiceConfig()
	cfg.SetDefaultValue()
	cfg.Frontend.SetDefaultValues()
	require.False(t, cfg.Frontend.MongoDB.Enable)
}

func TestClockOffsetBoundRetainedWhenMonitoringDisabled(t *testing.T) {
	validators := []struct {
		name string
		call func(*ServiceConfig) error
	}{
		{name: "validate loaded config", call: (*ServiceConfig).validate},
		{name: "apply programmatic defaults", call: (*ServiceConfig).setDefaultValue},
	}
	for _, validator := range validators {
		t.Run(validator.name, func(t *testing.T) {
			cfg := newServiceConfig()
			cfg.ServiceType = metadata.ServiceType_CN.String()
			require.False(t, cfg.Clock.EnableCheckMaxClockOffset)
			require.NoError(t, validator.call(&cfg))
			require.Equal(t, defaultMaxClockOffset, cfg.Clock.MaxClockOffset.Duration)

			cfg = newServiceConfig()
			cfg.ServiceType = metadata.ServiceType_CN.String()
			cfg.Clock.MaxClockOffset.Duration = -time.Nanosecond
			require.ErrorContains(t, validator.call(&cfg), "max-clock-offset must be positive")

			cfg = newServiceConfig()
			cfg.ServiceType = metadata.ServiceType_CN.String()
			cfg.Clock.MaxClockOffset.Duration = time.Second
			cfg.CN.Frontend.ConnectTimeout.Duration = 2*time.Second + time.Nanosecond
			require.ErrorContains(t, validator.call(&cfg), "authentication freshness clock budget 2.000000001s")

			cfg.CN.Frontend.ConnectTimeout.Duration = 2*time.Second + 2*time.Nanosecond
			cfg.CN.Frontend.CreateTxnOpTimeout.Duration = time.Nanosecond
			require.NoError(t, validator.call(&cfg))

			// Catalog authentication is unreachable when user checks are skipped,
			// so its connection-timeout budget must not reject startup.
			cfg = newServiceConfig()
			cfg.ServiceType = metadata.ServiceType_CN.String()
			cfg.Clock.MaxClockOffset.Duration = time.Second
			cfg.CN.Frontend.ConnectTimeout.Duration = time.Nanosecond
			cfg.CN.Frontend.SkipCheckUser = true
			require.NoError(t, validator.call(&cfg))

			// Skipping authentication does not bypass the general clock contract.
			cfg.Clock.MaxClockOffset.Duration = -time.Nanosecond
			require.ErrorContains(t, validator.call(&cfg), "max-clock-offset must be positive")
		})
	}
}

func TestStartupRetryIntervalsDefaultAndConfigurable(t *testing.T) {
	cfg := newServiceConfig()
	assert.Equal(t, time.Second, cfg.HAKeeperRunningRetryInterval.Duration)
	assert.Equal(t, time.Second, cfg.TNShardReadyRetryInterval.Duration)

	cfg.HAKeeperRunningRetryInterval.Duration = 100 * time.Millisecond
	cfg.TNShardReadyRetryInterval.Duration = 200 * time.Millisecond
	assert.NoError(t, cfg.setStartupRetryIntervalsDefault())
	assert.Equal(t, 100*time.Millisecond, cfg.HAKeeperRunningRetryInterval.Duration)
	assert.Equal(t, 200*time.Millisecond, cfg.TNShardReadyRetryInterval.Duration)
}

func TestSiriusConfigurationUsesOnlyCurrentSchema(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		invalid     bool
	}{
		{"minimal", "[cn.sirius]\nenabled=true\nnative-config-path='sirius.yaml'", false},
		{"disabled", "[cn.sirius]\nenabled=false", false},
		{"backend", "[cn.sirius]\nbackend='embedded'", true},
		{"input mode", "[cn.sirius]\ninput-mode='mo'", true},
		{"empty address", "[cn.sirius]\nflight-address=''", true},
		{"zero limit", "[cn.sirius]\nmax-batch-bytes=0", true},
		{"false benchmark", "[cn.sirius]\nbenchmark-no-gc=false", true},
		{"unknown", "[cn.sirius]\nunused=1", true},
		{"inline", "[cn]\nsirius={enabled=false, backend='flight'}", true},
		{"empty table", "[cn.sirius.backend]", true},
		{"case folded", "[CN.SIRIUS]\nbackend='flight'", true},
		{"unrelated", "[unrelated]\nunknown=1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg ServiceConfig
			err := parseFromString(tc.input, &cfg)
			if tc.invalid {
				require.ErrorContains(t, err, "unknown Sirius configuration key")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
