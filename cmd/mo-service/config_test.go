// Copyright 2022 Matrix Origin
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

package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/stretchr/testify/assert"
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
	# txn storage backend implementation. [TAE|MEM]
	backend = "MEM"
	`
	cfg := &Config{}
	err := parseFromString(data, cfg)
	assert.NoError(t, err)
	assert.Equal(t, tnservice.StorageMEM, cfg.getTNServiceConfig().Txn.Storage.Backend)
	assert.Equal(t, 2, len(cfg.FileServices))
	assert.Equal(t, "local", cfg.FileServices[0].Name)
	assert.Equal(t, defines.SharedFileServiceName, cfg.FileServices[1].Name)
	assert.Equal(t, 2, len(cfg.getTNServiceConfig().HAKeeper.ClientConfig.ServiceAddresses))
}

func TestFileServiceFactory(t *testing.T) {
	ctx := context.Background()

	c := &Config{}
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

	fs, err := c.createFileService(ctx, "A", globalCounterSet, 0, "")
	assert.NoError(t, err)
	assert.NotNil(t, fs)
}

func TestResolveGossipSeedAddresses(t *testing.T) {
	tests := []struct {
		addrs   []string
		results []string
		err     error
	}{
		{
			[]string{"localhost:32001", "localhost:32011"},
			[]string{"127.0.0.1:32001", "127.0.0.1:32011"},
			nil,
		},
		{
			[]string{"localhost:32001", "localhost:32011", "127.0.0.1:32021"},
			[]string{"127.0.0.1:32001", "127.0.0.1:32011", "127.0.0.1:32021"},
			nil,
		},
		{
			[]string{"127.0.0.1:32001"},
			[]string{"127.0.0.1:32001"},
			nil,
		},
		{
			[]string{"localhost:32001", "of-course-no-such-address42033.io:32001"},
			[]string{"127.0.0.1:32001", "of-course-no-such-address42033.io:32001"},
			nil,
		},
	}

	for _, tt := range tests {
		cfg := Config{
			LogService: logservice.Config{
				GossipSeedAddresses: tt.addrs,
			},
		}
		err := cfg.resolveGossipSeedAddresses()
		if err != tt.err {
			t.Errorf("expected %v, got %v", tt.err, err)
		}
		if got := cfg.LogService.GossipSeedAddresses; !reflect.DeepEqual(got, tt.results) {
			t.Errorf("expected %v, got %v", tt.results, got)
		}
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
  "localhost:32002",
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
	cfg := &Config{}
	err := parseFromString(data, cfg)
	assert.NoError(t, err)
	assert.NoError(t, cfg.validate())
	assert.NoError(t, cfg.resolveGossipSeedAddresses())
	assert.Equal(t, 1, len(cfg.LogService.GossipSeedAddresses))
	assert.Equal(t, "127.0.0.1:32002", cfg.LogService.GossipSeedAddresses[0])
}

func TestDumpCommonConfig(t *testing.T) {
	cfg1 := *NewConfig()
	_, err := dumpCommonConfig(cfg1)
	assert.NoError(t, err)
}
