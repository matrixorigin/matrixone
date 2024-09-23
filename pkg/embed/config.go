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
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/proxy"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/matrixorigin/matrixone/pkg/udf/pythonservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/debug/goroutine"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	tomlutil "github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/version"
	"go.uber.org/zap"
)

var (
	defaultMaxClockOffset = time.Millisecond * 500
	defaultMemoryLimit    = 1 << 40

	supportServiceTypes = map[string]metadata.ServiceType{
		metadata.ServiceType_CN.String():         metadata.ServiceType_CN,
		metadata.ServiceType_TN.String():         metadata.ServiceType_TN,
		metadata.ServiceType_LOG.String():        metadata.ServiceType_LOG,
		metadata.ServiceType_PROXY.String():      metadata.ServiceType_PROXY,
		metadata.ServiceType_PYTHON_UDF.String(): metadata.ServiceType_PYTHON_UDF,
	}

	supportTxnClockBackends = map[string]struct{}{
		localClockBackend: {},
		hlcClockBackend:   {},
	}
)

// ServiceConfig mo-service configuration
type ServiceConfig struct {
	// DataDir data dir
	DataDir string `toml:"data-dir"`
	// Log log config
	Log logutil.LogConfig `toml:"log"`
	// ServiceType service type, select the corresponding configuration to start the
	// service according to the service type. [CN|TN|LOG|PROXY]
	ServiceType string `toml:"service-type"`
	// FileServices the config for file services
	FileServices []fileservice.Config `toml:"fileservice"`
	// HAKeeperClient hakeeper client config
	HAKeeperClient logservice.HAKeeperClientConfig `toml:"hakeeper-client"`
	// TN tn service config
	TN_please_use_getTNServiceConfig *tnservice.Config `toml:"tn"`
	TNCompatible                     *tnservice.Config `toml:"dn"` // for old config files compatibility
	// LogService is the config for log service
	LogService logservice.Config `toml:"logservice"`
	// CN cn service config
	CN cnservice.Config `toml:"cn"`
	// ProxyConfig is the config of proxy.
	ProxyConfig proxy.Config `toml:"proxy"`
	// PythonUdfServerConfig is the config of python udf server
	PythonUdfServerConfig pythonservice.Config `toml:"python-udf-server"`
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

	// MetaCache the config for objectio metacache
	MetaCache objectio.CacheConfig `toml:"metacache"`

	// IsStandalone denotes the matrixone is running in standalone mode
	// For the tn does not boost an independent queryservice.
	// cn,tn shares the same queryservice in standalone mode.
	// Under distributed deploy mode, cn,tn are independent os process.
	// they have their own queryservice.
	IsStandalone bool

	// Goroutine goroutine config
	Goroutine goroutine.Config `toml:"goroutine"`

	// Malloc default config
	Malloc malloc.Config `toml:"malloc"`
}

func newServiceConfig() ServiceConfig {
	return ServiceConfig{
		HAKeeperClient: logservice.HAKeeperClientConfig{
			DiscoveryAddress: "",
			ServiceAddresses: []string{logservice.DefaultLogServiceServiceAddress},
			AllocateIDBatch:  100,
			EnableCompress:   false,
		},
		Observability: *config.NewObservabilityParameters(),
		LogService:    logservice.DefaultConfig(),
		CN: cnservice.Config{
			AutomaticUpgrade: true,
			Frontend: config.FrontendParameters{
				KeyEncryptionKey: "JlxRbXjFGnCsvbsFQSJFvhMhDLaAXq5y",
			},
		},
	}
}

func parseConfigFromFile(
	file string,
	cfg any,
) error {
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

func (c *ServiceConfig) validate() error {
	// data dir
	if c.DataDir == "" ||
		c.DataDir == "./mo-data" ||
		c.DataDir == "mo-data" {
		path, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		c.DataDir = filepath.Join(path, "mo-data")
	}

	// service type
	if _, err := c.getServiceType(); err != nil {
		return err
	}

	// clock
	if c.Clock.MaxClockOffset.Duration == 0 {
		c.Clock.MaxClockOffset.Duration = defaultMaxClockOffset
	}
	if c.Clock.Backend == "" {
		c.Clock.Backend = localClockBackend
	}
	if _, ok := supportTxnClockBackends[strings.ToUpper(c.Clock.Backend)]; !ok {
		return moerr.NewInternalErrorf(context.Background(), "%s clock backend not support", c.Clock.Backend)
	}
	if !c.Clock.EnableCheckMaxClockOffset {
		c.Clock.MaxClockOffset.Duration = 0
	}

	// file service
	c.setFileserviceDefaultValues()

	// limit
	if c.Limit.Memory == 0 {
		c.Limit.Memory = tomlutil.ByteSize(defaultMemoryLimit)
	}

	// log
	if c.Log.StacktraceLevel == "" {
		c.Log.StacktraceLevel = zap.PanicLevel.String()
	}

	if err := c.resolveGossipSeedAddresses(); err != nil {
		return err
	}
	return nil
}

func (c *ServiceConfig) setDefaultValue() error {
	// data dir
	if c.DataDir == "" {
		c.DataDir = "./mo-data"
	}

	// clock
	if c.Clock.MaxClockOffset.Duration == 0 {
		c.Clock.MaxClockOffset.Duration = defaultMaxClockOffset
	}
	if c.Clock.Backend == "" {
		c.Clock.Backend = localClockBackend
	}
	if _, ok := supportTxnClockBackends[strings.ToUpper(c.Clock.Backend)]; !ok {
		return moerr.NewInternalErrorf(context.Background(), "%s clock backend not support", c.Clock.Backend)
	}
	if !c.Clock.EnableCheckMaxClockOffset {
		c.Clock.MaxClockOffset.Duration = 0
	}

	// file service
	c.setFileserviceDefaultValues()

	// limit
	if c.Limit.Memory == 0 {
		c.Limit.Memory = tomlutil.ByteSize(defaultMemoryLimit)
	}

	// log
	if c.Log.StacktraceLevel == "" {
		c.Log.StacktraceLevel = zap.PanicLevel.String()
	}
	c.Log = logutil.GetDefaultConfig()

	// HAKeeperClient has been set in NewConfig

	// tn
	if c.TN_please_use_getTNServiceConfig != nil {
		c.TN_please_use_getTNServiceConfig.SetDefaultValue()
	}
	if c.TNCompatible != nil {
		c.TNCompatible.SetDefaultValue()
	}

	// LogService has been set in NewConfig

	// cn
	c.CN.SetDefaultValue()

	//no default proxy config

	// Observability has been set in NewConfig

	// meta cache
	c.initMetaCache()

	return nil
}

func (c *ServiceConfig) initMetaCache() {
	if c.MetaCache.MemoryCapacity > 0 {
		objectio.InitMetaCache(int64(c.MetaCache.MemoryCapacity))
	}
}

func (c *ServiceConfig) defaultFileServiceDataDir(name string) string {
	return filepath.Join(c.DataDir, strings.ToLower(name))
}

func (c *ServiceConfig) createFileService(
	ctx context.Context,
	serviceType metadata.ServiceType,
	nodeUUID string,
) (*fileservice.FileServices, error) {

	// set distributed cache callbacks
	for i := range c.FileServices {
		c.setCacheCallbacks(&c.FileServices[i])
	}

	services := make([]fileservice.FileService, 0, len(c.FileServices))
	for _, config := range c.FileServices {
		counterSet := new(perfcounter.CounterSet)
		service, err := fileservice.NewFileService(
			ctx,
			config,
			[]*perfcounter.CounterSet{
				counterSet,
			},
		)
		if err != nil {
			return nil, err
		}
		services = append(services, service)

		// perf counter
		counterSetName := perfcounter.NameForFileService(
			serviceType.String(),
			nodeUUID,
			service.Name(),
		)
		perfcounter.Named.Store(counterSetName, counterSet)

		// set shared fs perf counter as node perf counter
		if service.Name() == defines.SharedFileServiceName {
			perfcounter.Named.Store(
				perfcounter.NameForNode(serviceType.String(), nodeUUID),
				counterSet,
			)
		}

		// Create "Log Exporter" for this PerfCounter
		counterLogExporter := perfcounter.NewCounterLogExporter(counterSet)
		// Register this PerfCounter's "Log Exporter" to global stats registry.
		stats.Register(counterSetName, stats.WithLogExporter(counterLogExporter))
	}

	// create FileServices
	fs, err := fileservice.NewFileServices(
		"",
		services...,
	)
	if err != nil {
		return nil, err
	}

	// ensure local exists
	_, err = fileservice.Get[fileservice.FileService](fs, defines.LocalFileServiceName)
	if err != nil {
		return nil, err
	}

	// ensure shared exists
	_, err = fileservice.Get[fileservice.FileService](fs, defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}

	// ensure etl exists and is ETL
	if !c.Observability.DisableMetric || !c.Observability.DisableTrace {
		_, err = fileservice.Get[fileservice.ETLFileService](fs, defines.ETLFileServiceName)
		if err != nil {
			return nil, moerr.ConvertPanicError(context.Background(), err)
		}
	}

	return fs, nil
}

func (c *ServiceConfig) getLogServiceConfig() logservice.Config {
	cfg := c.LogService
	logutil.Infof("hakeeper client cfg: %v", c.HAKeeperClient)
	cfg.HAKeeperClientConfig = c.HAKeeperClient
	cfg.DataDir = filepath.Join(c.DataDir, "logservice-data", cfg.UUID)
	var hostname string
	var err error
	hostname = cfg.ExplicitHostname
	if len(hostname) == 0 {
		// Should sync directory structure with dragonboat.
		hostname, err = os.Hostname()
		if err != nil {
			panic(fmt.Sprintf("cannot get hostname: %s", err))
		}
	}
	cfg.SnapshotExportDir = filepath.Join(cfg.DataDir, hostname,
		fmt.Sprintf("%020d", cfg.DeploymentID), "exported-snapshot")
	return cfg
}

func (c *ServiceConfig) getTNServiceConfig() tnservice.Config {
	if c.TN_please_use_getTNServiceConfig == nil && c.TNCompatible != nil {
		c.TN_please_use_getTNServiceConfig = c.TNCompatible
	}
	var cfg tnservice.Config
	if c.TN_please_use_getTNServiceConfig != nil {
		cfg = *c.TN_please_use_getTNServiceConfig
	}
	cfg.HAKeeper.ClientConfig = c.HAKeeperClient
	cfg.DataDir = filepath.Join(c.DataDir, "dn-data", cfg.UUID)
	return cfg
}

func (c *ServiceConfig) getCNServiceConfig() cnservice.Config {
	cfg := c.CN
	cfg.HAKeeper.ClientConfig = c.HAKeeperClient
	cfg.Frontend.SetLogAndVersion(&c.Log, version.Version)
	if cfg.Txn.Trace.Dir == "" {
		cfg.Txn.Trace.Dir = "trace"
	}
	return cfg
}

func (c *ServiceConfig) getProxyConfig() proxy.Config {
	cfg := c.ProxyConfig
	cfg.HAKeeper.ClientConfig = c.HAKeeperClient
	return cfg
}

// memberlist requires all gossip seed addresses to be provided as IP:PORT
func (c *ServiceConfig) resolveGossipSeedAddresses() error {
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
			return moerr.NewBadConfigf(context.Background(), "GossipSeedAddress %s", addr)
		}
		result = append(result, net.JoinHostPort(filtered[0], port))
	}
	c.LogService.GossipSeedAddresses = result
	return nil
}

func (c *ServiceConfig) hashNodeID() uint16 {
	st, err := c.getServiceType()
	if err != nil {
		panic(err)
	}

	uuid := ""
	switch st {
	case metadata.ServiceType_CN:
		uuid = c.CN.UUID
	case metadata.ServiceType_TN:
		uuid = c.getTNServiceConfig().UUID
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

func (c *ServiceConfig) getServiceType() (metadata.ServiceType, error) {
	if c.ServiceType == "DN" { // for old config files compatibility
		c.ServiceType = metadata.ServiceType_TN.String()
	}
	if v, ok := supportServiceTypes[strings.ToUpper(c.ServiceType)]; ok {
		return v, nil
	}
	return metadata.ServiceType(0), moerr.NewInternalErrorf(context.Background(), "service type %s not support", c.ServiceType)
}

func (c *ServiceConfig) mustGetServiceType() metadata.ServiceType {
	v, err := c.getServiceType()
	if err != nil {
		panic(err)
	}
	return v
}

func (c *ServiceConfig) mustGetServiceUUID() string {
	switch c.mustGetServiceType() {
	case metadata.ServiceType_CN:
		return c.CN.UUID
	case metadata.ServiceType_TN:
		return c.getTNServiceConfig().UUID
	case metadata.ServiceType_LOG:
		return c.LogService.UUID
	case metadata.ServiceType_PROXY:
		return c.ProxyConfig.UUID
	}
	panic("impossible")
}

func (c *ServiceConfig) setCacheCallbacks(fsConfig *fileservice.Config) {
	fsConfig.Cache.SetRemoteCacheCallback()
}

func (c *ServiceConfig) setFileserviceDefaultValues() {
	for i := 0; i < len(c.FileServices); i++ {
		config := &c.FileServices[i]

		// rename 's3' to 'shared'
		oldName := config.Name
		if strings.EqualFold(config.Name, "s3") {
			config.Name = defines.SharedFileServiceName
		}

		// set default data dir
		if config.DataDir == "" {
			// compatibility check
			// if 's3' was renamed to 'shared' but 's3' is not empty, use it
			oldDir := c.defaultFileServiceDataDir(oldName)
			_, err := os.Stat(oldDir)
			if err == nil {
				config.DataDir = oldDir
			} else {
				config.DataDir = c.defaultFileServiceDataDir(config.Name)
			}
		}

		// set default disk cache dir
		if config.Cache.DiskPath == nil {
			path := config.DataDir + "-cache"
			config.Cache.DiskPath = &path
		}

	}

	// default LOCAL fs
	ok := false
	for _, config := range c.FileServices {
		if strings.EqualFold(config.Name, defines.LocalFileServiceName) {
			ok = true
			break
		}
	}
	// default to local disk
	if !ok {
		c.FileServices = append(c.FileServices, fileservice.Config{
			Name:    defines.LocalFileServiceName,
			Backend: "DISK",
			DataDir: c.defaultFileServiceDataDir(defines.LocalFileServiceName),
		})
	}

	// default SHARED fs
	ok = false
	for _, config := range c.FileServices {
		if strings.EqualFold(config.Name, defines.SharedFileServiceName) {
			ok = true
			break
		}
	}
	// default to local disk
	if !ok {
		c.FileServices = append(c.FileServices, fileservice.Config{
			Name:    defines.SharedFileServiceName,
			Backend: "DISK",
			DataDir: c.defaultFileServiceDataDir(defines.SharedFileServiceName),
		})
	}

	// default ETL fs
	ok = false
	for _, config := range c.FileServices {
		if strings.EqualFold(config.Name, defines.ETLFileServiceName) {
			ok = true
			break
		}
	}
	// default to local disk
	if !ok {
		c.FileServices = append(c.FileServices, fileservice.Config{
			Name:    defines.ETLFileServiceName,
			Backend: "DISK-ETL", // must be ETL
			DataDir: c.defaultFileServiceDataDir(defines.ETLFileServiceName),
		})
	}

}

func dumpCommonConfig(cfg ServiceConfig) (map[string]*logservicepb.ConfigItem, error) {
	defCfg := newServiceConfig()
	err := defCfg.setDefaultValue()
	if err != nil {
		return nil, err
	}
	ret, err := util.DumpConfig(cfg, defCfg)
	if err != nil {
		return nil, err
	}

	//specific config items should be remoted
	filters := []string{
		"config.tn_please_use_gettnserviceconfig",
		"config.tncompatible",
		"config.logservice",
		"config.cn",
		"config.proxyconfig",
	}

	//denote the common for cn,tn,log or proxy
	prefix := "common"

	newMap := make(map[string]*logservicepb.ConfigItem)
	for s, item := range ret {
		needDrop := false
		for _, filter := range filters {
			if strings.HasPrefix(strings.ToLower(s), strings.ToLower(filter)) {
				needDrop = true
				break
			}
		}
		if needDrop {
			continue
		}

		s = prefix + s
		item.Name = s
		newMap[s] = item
	}

	return newMap, err
}
