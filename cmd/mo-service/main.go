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
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	struntime "runtime"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/datasync"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/gossip"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/proxy"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/matrixorigin/matrixone/pkg/udf/pythonservice"
	"github.com/matrixorigin/matrixone/pkg/util/debug/goroutine"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	"github.com/matrixorigin/matrixone/pkg/util/profile"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
)

var (
	configFile        = flag.String("cfg", "", "toml configuration used to start mo-service")
	launchFile        = flag.String("launch", "", "toml configuration used to launch mo cluster")
	versionFlag       = flag.Bool("version", false, "print version information")
	daemon            = flag.Bool("daemon", false, "run mo-service in daemon mode")
	withProxy         = flag.Bool("with-proxy", false, "run mo-service with proxy module started")
	maxProcessor      = flag.Int("max-processor", 0, "set max processor for go runtime")
	globalEtlFS       fileservice.FileService
	globalServiceType string
	globalNodeId      string
)

func init() {
	maxprocs.Set(maxprocs.Logger(func(string, ...interface{}) {}))
}

func main() {
	if *maxProcessor > 0 {
		struntime.GOMAXPROCS(*maxProcessor)
	}

	flag.Parse()
	maybePrintVersion()
	maybeRunInDaemonMode()

	uuid.EnableRandPool()

	if *cpuProfilePathFlag != "" {
		stop := startCPUProfile()
		defer stop()
	}
	if *allocsProfilePathFlag != "" {
		defer writeAllocsProfile()
	}
	if *heapProfilePathFlag != "" {
		defer writeHeapProfile()
	}
	if *httpListenAddr != "" {
		go func() {
			http.ListenAndServe(*httpListenAddr, nil)
		}()
	}

	ctx := context.Background()
	shutdownC := make(chan struct{})

	stopper := stopper.NewStopper("main", stopper.WithLogger(logutil.GetGlobalLogger()))
	if *launchFile != "" {
		if err := startCluster(ctx, stopper, shutdownC); err != nil {
			panic(err)
		}
	} else if *configFile != "" {
		cfg := NewConfig()
		if err := parseConfigFromFile(*configFile, cfg); err != nil {
			panic(fmt.Sprintf("failed to parse config from %s, error: %s", *configFile, err.Error()))
		}
		if err := startService(ctx, cfg, stopper, shutdownC); err != nil {
			panic(err)
		}
	} else {
		panic(errors.New("no configuration specified"))
	}

	waitSignalToStop(stopper, shutdownC)
	logutil.GetGlobalLogger().Info("Shutdown complete")
}

func waitSignalToStop(stopper *stopper.Stopper, shutdownC chan struct{}) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT)

	go saveProfilesLoop(sigchan)

	detail := "Starting shutdown..."
	select {
	case sig := <-sigchan:
		detail += "signal: " + sig.String()
		//dump heap profile before stopping services
		heapProfilePath := saveProfile(profile.HEAP)
		detail += ". heap profile: " + heapProfilePath
		//dump goroutine before stopping services
		routineProfilePath := saveProfile(profile.GOROUTINE)
		detail += " routine profile: " + routineProfilePath
	case <-shutdownC:
		// waiting, give a chance let all log stores and tn stores to get
		// shutdown cmd from ha keeper
		time.Sleep(time.Second * 5)
		detail += "ha keeper issues shutdown command"
	}

	stopAllDynamicCNServices()

	logutil.GetGlobalLogger().Info(detail)
	stopper.Stop()
	if cnProxy != nil {
		if err := cnProxy.Stop(); err != nil {
			logutil.GetGlobalLogger().Error("shutdown cn proxy failed", zap.Error(err))
		}
	}
}

func startService(
	ctx context.Context,
	cfg *Config,
	stopper *stopper.Stopper,
	shutdownC chan struct{},
) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if err := cfg.resolveGossipSeedAddresses(); err != nil {
		return err
	}
	st, err := cfg.getServiceType()
	if err != nil {
		return err
	}

	setupServiceRuntime(cfg, stopper)

	malloc.SetDefaultConfig(cfg.Malloc)

	setupStatusServer(runtime.ServiceRuntime(cfg.mustGetServiceUUID()))

	goroutine.StartLeakCheck(stopper, cfg.Goroutine)

	var gossipNode *gossip.Node
	if st == metadata.ServiceType_CN {
		gossipNode, err = gossip.NewNode(ctx, cfg.CN.UUID)
		if err != nil {
			return err
		}
		for i := range cfg.FileServices {
			cfg.FileServices[i].Cache.KeyRouterFactory = gossipNode.DistKeyCacheGetter()
			cfg.FileServices[i].Cache.QueryClient, err = qclient.NewQueryClient(
				cfg.CN.UUID, cfg.FileServices[i].Cache.RPC,
			)
			if err != nil {
				return err
			}
		}
	}

	fs, err := cfg.createFileService(ctx, st, cfg.mustGetServiceUUID())
	if err != nil {
		return err
	}

	etlFS, err := fileservice.Get[fileservice.FileService](fs, defines.ETLFileServiceName)
	if err != nil {
		return err
	}
	if err = initTraceMetric(ctx, st, cfg, stopper, etlFS, cfg.mustGetServiceUUID()); err != nil {
		return err
	}

	if globalEtlFS == nil {
		globalEtlFS = etlFS
		globalServiceType = st.String()
		globalNodeId = cfg.mustGetServiceUUID()
	}

	switch st {
	case metadata.ServiceType_CN:
		return startCNService(cfg, stopper, fs, gossipNode)
	case metadata.ServiceType_TN:
		return startTNService(cfg, stopper, fs, shutdownC)
	case metadata.ServiceType_LOG:
		return startLogService(cfg, stopper, fs, shutdownC)
	case metadata.ServiceType_PROXY:
		return startProxyService(cfg, stopper)
	case metadata.ServiceType_PYTHON_UDF:
		return startPythonUdfService(cfg, stopper)
	default:
		panic("unknown service type")
	}
}

// serviceWG control motrace/mometric quit as last one.
var serviceWG sync.WaitGroup

func startCNService(
	cfg *Config,
	stopper *stopper.Stopper,
	fileService fileservice.FileService,
	gossipNode *gossip.Node,
) error {
	// start up system module to do some calculation.
	system.Run(stopper)

	if err := waitClusterCondition(cfg.mustGetServiceUUID(), cfg.HAKeeperClient, waitAnyShardReady); err != nil {
		return err
	}
	serviceWG.Add(1)
	return stopper.RunNamedTask("cn-service", func(ctx context.Context) {
		defer serviceWG.Done()
		cfg.initMetaCache()
		c := cfg.getCNServiceConfig()
		commonConfigKVMap, _ := dumpCommonConfig(*cfg)
		s, err := cnservice.NewService(
			&c,
			ctx,
			fileService,
			gossipNode,
			cnservice.WithLogger(logutil.GetGlobalLogger().Named("cn-service").With(zap.String("uuid", cfg.CN.UUID))),
			cnservice.WithMessageHandle(compile.CnServerMessageHandler),
			cnservice.WithConfigData(commonConfigKVMap),
			cnservice.WithTxnTraceData(filepath.Join(cfg.DataDir, c.Txn.Trace.Dir)),
		)
		if err != nil {
			panic(err)
		}
		if err := s.Start(); err != nil {
			panic(err)
		}

		<-ctx.Done()
		// Close the cache client which is used in file service.
		for _, fs := range cfg.FileServices {
			if fs.Cache.QueryClient != nil {
				_ = fs.Cache.QueryClient.Close()
			}
		}
		if err := s.Close(); err != nil {
			logutil.GetGlobalLogger().Error("failed to close cn service", zap.Error(err))
		}
	})
}

func startTNService(
	cfg *Config,
	stopper *stopper.Stopper,
	fileService fileservice.FileService,
	shutdownC chan struct{},
) error {
	if err := waitClusterCondition(cfg.mustGetServiceUUID(), cfg.HAKeeperClient, waitHAKeeperRunning); err != nil {
		return err
	}
	serviceWG.Add(1)
	return stopper.RunNamedTask("tn-service", func(ctx context.Context) {
		defer serviceWG.Done()
		cfg.initMetaCache()
		c := cfg.getTNServiceConfig()
		//notify the tn service it is in the standalone cluster
		c.InStandalone = cfg.IsStandalone
		commonConfigKVMap, _ := dumpCommonConfig(*cfg)
		s, err := tnservice.NewService(
			&c,
			mustGetRuntime(cfg),
			fileService,
			shutdownC,
			tnservice.WithConfigData(commonConfigKVMap))
		if err != nil {
			panic(err)
		}
		if err := s.Start(); err != nil {
			panic(err)
		}

		<-ctx.Done()
		if err := s.Close(); err != nil {
			logutil.GetGlobalLogger().Error("failed to close tn service", zap.Error(err))
		}
	})
}

func startLogService(
	cfg *Config,
	stopper *stopper.Stopper,
	fileService fileservice.FileService,
	shutdownC chan struct{},
) error {
	lscfg := cfg.getLogServiceConfig()
	commonConfigKVMap, _ := dumpCommonConfig(*cfg)
	rt := runtime.ServiceRuntime(lscfg.UUID)
	options := []logservice.Option{
		logservice.WithRuntime(rt),
		logservice.WithConfigData(commonConfigKVMap),
	}
	if lscfg.BootstrapConfig.StandbyEnabled {
		ds, err := datasync.NewDataSync(
			lscfg.UUID,
			stopper,
			rt,
			lscfg.HAKeeperClientConfig,
			fileService,
		)
		if err != nil {
			panic(err)
		}
		options = append(options, logservice.WithDataSync(ds))
	}
	s, err := logservice.NewService(
		lscfg,
		fileService,
		shutdownC,
		options...,
	)
	if err != nil {
		panic(err)
	}
	if err := s.Start(); err != nil {
		panic(err)
	}
	serviceWG.Add(1)
	return stopper.RunNamedTask("log-service", func(ctx context.Context) {
		defer serviceWG.Done()
		if cfg.LogService.BootstrapConfig.BootstrapCluster {
			logutil.Infof("bootstrapping hakeeper...")
			if err := s.BootstrapHAKeeper(ctx, cfg.LogService); err != nil {
				panic(err)
			}
		}

		<-ctx.Done()
		if err := s.Close(); err != nil {
			logutil.GetGlobalLogger().Error("failed to close log service", zap.Error(err))
		}
	})
}

// startProxyService starts the proxy service.
func startProxyService(cfg *Config, stopper *stopper.Stopper) error {
	if err := waitClusterCondition(cfg.mustGetServiceUUID(), cfg.HAKeeperClient, waitHAKeeperRunning); err != nil {
		return err
	}
	serviceWG.Add(1)
	return stopper.RunNamedTask("proxy-service", func(ctx context.Context) {
		defer serviceWG.Done()
		s, err := proxy.NewServer(
			ctx,
			cfg.getProxyConfig(),
			proxy.WithRuntime(runtime.ServiceRuntime(cfg.getProxyConfig().UUID)),
		)
		if err != nil {
			panic(err)
		}
		if err := s.Start(); err != nil {
			panic(err)
		}
		<-ctx.Done()
		if err := s.Close(); err != nil {
			logutil.GetGlobalLogger().Error("failed to close proxy service", zap.Error(err))
		}
	})
}

// startPythonUdfService starts the python udf service.
func startPythonUdfService(cfg *Config, stopper *stopper.Stopper) error {
	if err := waitClusterCondition(cfg.mustGetServiceUUID(), cfg.HAKeeperClient, waitHAKeeperRunning); err != nil {
		return err
	}
	serviceWG.Add(1)
	return stopper.RunNamedTask("python-udf-service", func(ctx context.Context) {
		defer serviceWG.Done()
		s, err := pythonservice.NewService(cfg.PythonUdfServerConfig)
		if err != nil {
			panic(err)
		}
		if err := s.Start(); err != nil {
			panic(err)
		}
		<-ctx.Done()
		if err := s.Close(); err != nil {
			logutil.GetGlobalLogger().Error("failed to close python udf service", zap.Error(err))
		}
	})
}

func initTraceMetric(ctx context.Context, st metadata.ServiceType, cfg *Config, stopper *stopper.Stopper, fs fileservice.FileService, UUID string) error {
	var writerFactory table.WriterFactory
	var err error
	var initWG sync.WaitGroup
	SV := cfg.getObservabilityConfig()

	nodeRole := st.String()
	if *launchFile != "" {
		nodeRole = mometric.LaunchMode
	}

	selector := clusterservice.NewSelector().SelectByLabel(SV.LabelSelector, clusterservice.Contain)
	mustGetRuntime(cfg).SetGlobalVariables(runtime.BackgroundCNSelector, selector)

	if !SV.DisableTrace || !SV.DisableMetric {
		writerFactory = export.GetWriterFactory(fs, UUID, nodeRole, !SV.DisableSqlWriter)
		initWG.Add(1)
		collector := export.NewMOCollector(ctx, cfg.mustGetServiceUUID(), export.WithOBCollectorConfig(&SV.OBCollectorConfig))
		stopper.RunNamedTask("trace", func(ctx context.Context) {
			err, act := motrace.InitWithConfig(ctx,
				&SV,
				motrace.WithNode(UUID, nodeRole),
				motrace.WithBatchProcessor(collector),
				motrace.WithFSWriterFactory(writerFactory),
				motrace.WithSQLExecutor(nil),
			)
			initWG.Done()
			if err != nil {
				panic(err)
			}
			if !act {
				return
			}
			<-ctx.Done()
			logutil.Info("motrace receive shutdown signal, wait other services shutdown complete.")
			serviceWG.Wait()
			logutil.Info("Shutdown service complete.")
			// flush trace/log/error framework
			if err = motrace.Shutdown(ctx); err != nil {
				logutil.Warn("Shutdown trace", logutil.ErrorField(err), logutil.NoReportFiled())
			}
		})
		initWG.Wait()
	}
	if !SV.DisableMetric || SV.EnableMetricToProm {
		stopper.RunNamedTask("metric", func(ctx context.Context) {
			if act := mometric.InitMetric(ctx, nil, &SV, UUID, nodeRole,
				mometric.WithWriterFactory(writerFactory),
				mometric.WithFrontendServerStarted(frontend.MoServerIsStarted),
			); !act {
				return
			}
			<-ctx.Done()
			mometric.StopMetricSync()
		})
	}
	if err = export.InitMerge(ctx, &SV); err != nil {
		return err
	}
	return nil
}

func maybeRunInDaemonMode() {
	if _, isChild := os.LookupEnv("daemon"); *daemon && !isChild {
		childENV := []string{"daemon=true"}
		pwd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		cpid, err := syscall.ForkExec(os.Args[0], os.Args, &syscall.ProcAttr{
			Dir: pwd,
			Env: append(os.Environ(), childENV...),
			Sys: &syscall.SysProcAttr{
				Setsid: true,
			},
			Files: []uintptr{0, 1, 2}, // print message to the same pty
		})
		if err != nil {
			panic(err)
		}
		logutil.Infof("mo-service is running in daemon mode, child process is %d", cpid)
		os.Exit(0)
	}
}
