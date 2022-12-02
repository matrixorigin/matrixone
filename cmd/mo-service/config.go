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
	"fmt"
	"hash/fnv"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/dnservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	tomlutil "github.com/matrixorigin/matrixone/pkg/util/toml"
)

var (
	defaultMaxClockOffset = time.Millisecond * 500
	defaultMemoryLimit    = 1 << 40

	supportServiceTypes = map[string]metadata.ServiceType{
		metadata.ServiceType_CN.String():  metadata.ServiceType_CN,
		metadata.ServiceType_DN.String():  metadata.ServiceType_DN,
		metadata.ServiceType_LOG.String(): metadata.ServiceType_LOG,
	}
)

// LaunchConfig Start a MO cluster with launch
type LaunchConfig struct {
	// LogServiceConfigFiles log service config files
	LogServiceConfigFiles []string `toml:"logservices"`
	// DNServiceConfigsFiles log service config files
	DNServiceConfigsFiles []string `toml:"dnservices"`
	// CNServiceConfigsFiles log service config files
	CNServiceConfigsFiles []string `toml:"cnservices"`
}

// Config mo-service configuration
type Config struct {
	// DataDir data dir
	DataDir string `toml:"data-dir"`
	// Log log config
	Log logutil.LogConfig `toml:"log"`
	// ServiceType service type, select the corresponding configuration to start the
	// service according to the service type. [CN|DN|LOG]
	ServiceType string `toml:"service-type"`
	// FileServices the config for file services
	FileServices []fileservice.Config `toml:"fileservice"`
	// HAKeeperClient hakeeper client config
	HAKeeperClient logservice.HAKeeperClientConfig `toml:"hakeeper-client"`
	// DN dn service config
	DN dnservice.Config `toml:"dn"`
	// LogService is the config for log service
	LogService logservice.Config `toml:"logservice"`
	// CN cn service config
	CN cnservice.Config `toml:"cn"`
	// Observability parameters for the metric/trace
	Observability config.ObservabilityParameters `toml:"observability"`

	// Clock txn clock type. [LOCAL|HLC]. Default is LOCAL.
	Clock struct {
		// Backend clock backend implementation. [LOCAL|HLC], default LOCAL.
		Backend string `toml:"source"`
		// MaxClockOffset max clock offset between two nodes. Default is 500ms.
		// Only valid when enable-check-clock-offset is true
		MaxClockOffset tomlutil.Duration `toml:"max-clock-offset"`
		// EnableCheckMaxClockOffset enable local clock offset checker
		EnableCheckMaxClockOffset bool `toml:"enable-check-clock-offset"`
	}

	// Limit limit configuration
	Limit struct {
		// Memory memory usage limit, see mpool for details
		Memory tomlutil.ByteSize `toml:"memory"`
	}
}

func parseConfigFromFile(file string, cfg any) error {
	if file == "" {
		return moerr.NewInternalError(context.Background(), "toml config file not set")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	return parseFromString(string(data), cfg)
}

func parseFromString(data string, cfg any) error {
	if _, err := toml.Decode(data, cfg); err != nil {
		return err
	}
	return nil
}

func (c *Config) validate() error {
	if c.DataDir == "" {
		c.DataDir = "./mo-data"
	}
	if _, err := c.getServiceType(); err != nil {
		return err
	}
	if c.Clock.MaxClockOffset.Duration == 0 {
		c.Clock.MaxClockOffset.Duration = defaultMaxClockOffset
	}
	if c.Clock.Backend == "" {
		c.Clock.Backend = localClockBackend
	}
	if _, ok := supportTxnClockBackends[strings.ToUpper(c.Clock.Backend)]; !ok {
		return moerr.NewInternalError(context.Background(), "%s clock backend not support", c.Clock.Backend)
	}
	if !c.Clock.EnableCheckMaxClockOffset {
		c.Clock.MaxClockOffset.Duration = 0
	}
	for idx := range c.FileServices {
		switch c.FileServices[idx].Name {
		case defines.LocalFileServiceName, defines.ETLFileServiceName:
			if c.FileServices[idx].DataDir == "" {
				c.FileServices[idx].DataDir = filepath.Join(c.DataDir, strings.ToLower(c.FileServices[idx].Name))
			}
		}
	}
	if c.Limit.Memory == 0 {
		c.Limit.Memory = tomlutil.ByteSize(defaultMemoryLimit)
	}
	return nil
}

func (c *Config) createFileService(defaultName string) (*fileservice.FileServices, error) {
	// create all services
	services := make([]fileservice.FileService, 0, len(c.FileServices))
	for _, config := range c.FileServices {
		service, err := fileservice.NewFileService(config)
		if err != nil {
			return nil, err
		}
		services = append(services, service)
	}

	// create FileServices
	fs, err := fileservice.NewFileServices(
		defaultName,
		services...,
	)
	if err != nil {
		return nil, err
	}

	// validate default name
	_, err = fileservice.Get[fileservice.FileService](fs, defaultName)
	if err != nil {
		return nil, err
	}

	// ensure local exists
	_, err = fileservice.Get[fileservice.FileService](fs, defines.LocalFileServiceName)
	if err != nil {
		return nil, err
	}

	// ensure s3 exists
	_, err = fileservice.Get[fileservice.FileService](fs, defines.S3FileServiceName)
	if err != nil {
		return nil, err
	}

	// ensure etl exists, for trace & metric
	if !c.Observability.DisableMetric || !c.Observability.DisableTrace {
		_, err = fileservice.Get[fileservice.FileService](fs, defines.ETLFileServiceName)
		if err != nil {
			return nil, moerr.ConvertPanicError(context.Background(), err)
		}
	}

	return fs, nil
}

func (c *Config) getLogServiceConfig() logservice.Config {
	cfg := c.LogService
	logutil.Infof("hakeeper client cfg: %v", c.HAKeeperClient)
	cfg.HAKeeperClientConfig = c.HAKeeperClient
	cfg.DataDir = filepath.Join(c.DataDir, "logservice-data", cfg.UUID)
	// Should sync directory structure with dragonboat.
	hostname, err := os.Hostname()
	if err != nil {
		panic(fmt.Sprintf("cannot get hostname: %s", err))
	}
	cfg.SnapshotExportDir = filepath.Join(cfg.DataDir, hostname,
		fmt.Sprintf("%020d", cfg.DeploymentID), "exported-snapshot")
	return cfg
}

func (c *Config) getDNServiceConfig() dnservice.Config {
	cfg := c.DN
	cfg.HAKeeper.ClientConfig = c.HAKeeperClient
	cfg.DataDir = filepath.Join(c.DataDir, "dn-data", cfg.UUID)
	return cfg
}

func (c *Config) getCNServiceConfig() cnservice.Config {
	cfg := c.CN
	cfg.HAKeeper.ClientConfig = c.HAKeeperClient
	cfg.Frontend.SetLogAndVersion(&c.Log, Version)
	cfg.Frontend.StorePath = filepath.Join(c.DataDir, "cn-data", cfg.UUID)
	return cfg
}

func (c *Config) getObservabilityConfig() config.ObservabilityParameters {
	cfg := c.Observability
	cfg.SetDefaultValues(Version)
	return cfg
}

// memberlist requires all gossip seed addresses to be provided as IP:PORT
func (c *Config) resolveGossipSeedAddresses() error {
	result := make([]string, 0)
	for _, addr := range c.LogService.GossipSeedAddresses {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return err
		}
		ips, err := net.LookupIP(host)
		if err != nil {
			// the configured member may be failed currently, keep the host name anyway since
			// memberlist would try to resolve it again
			result = append(result, addr)
			continue
		}
		// only keep IPv4 addresses
		filtered := make([]string, 0)
		for _, ip := range ips {
			if ip.To4() != nil {
				filtered = append(filtered, ip.String())
			}
		}
		if len(filtered) != 1 {
			return moerr.NewBadConfig(context.Background(), "GossipSeedAddress %s", addr)
		}
		result = append(result, net.JoinHostPort(filtered[0], port))
	}
	c.LogService.GossipSeedAddresses = result
	return nil
}

func (c *Config) hashNodeID() uint16 {
	st, err := c.getServiceType()
	if err != nil {
		panic(err)
	}

	uuid := ""
	switch st {
	case metadata.ServiceType_CN:
		uuid = c.CN.UUID
	case metadata.ServiceType_DN:
		uuid = c.DN.UUID
	case metadata.ServiceType_LOG:
		uuid = c.LogService.UUID
	}
	if uuid == "" {
		return 0
	}

	h := fnv.New32()
	if _, err := h.Write([]byte(uuid)); err != nil {
		panic(err)
	}
	v := h.Sum32()
	return uint16(v % math.MaxUint16)
}

func (c *Config) getServiceType() (metadata.ServiceType, error) {
	if v, ok := supportServiceTypes[strings.ToUpper(c.ServiceType)]; ok {
		return v, nil
	}
	return metadata.ServiceType(0), moerr.NewInternalError(context.Background(), "service type %s not support", c.ServiceType)
}

func (c *Config) mustGetServiceType() metadata.ServiceType {
	v, err := c.getServiceType()
	if err != nil {
		panic(err)
	}
	return v
}

func (c *Config) mustGetServiceUUID() string {
	switch c.mustGetServiceType() {
	case metadata.ServiceType_CN:
		return c.CN.UUID
	case metadata.ServiceType_DN:
		return c.DN.UUID
	case metadata.ServiceType_LOG:
		return c.LogService.UUID
	}
	panic("impossible")
}
