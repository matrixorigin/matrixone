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
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/dnservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"go.uber.org/zap"
)

var (
	configFile = flag.String("cfg", "", "toml configuration used to start mo-service")
	launchFile = flag.String("launch", "", "toml configuration used to launch mo cluster")
	version    = flag.Bool("version", false, "print version information")
	daemon     = flag.Bool("daemon", false, "run mo-service in daemon mode")
)

func main() {
	flag.Parse()
	maybePrintVersion()
	maybeRunInDaemonMode()

	if *cpuProfilePathFlag != "" {
		stop := startCPUProfile()
		defer stop()
	}
	if *allocsProfilePathFlag != "" {
		defer writeAllocsProfile()
	}
	if *fileServiceProfilePathFlag != "" {
		stop := startFileServiceProfile()
		defer stop()
	}
	if *httpListenAddr != "" {
		go func() {
			http.ListenAndServe(*httpListenAddr, nil)
		}()
	}

	var seed int64
	if err := binary.Read(crand.Reader, binary.LittleEndian, &seed); err != nil {
		panic(err)
	}
	rand.Seed(seed)

	ctx := context.Background()

	stopper := stopper.NewStopper("main", stopper.WithLogger(logutil.GetGlobalLogger()))
	if *launchFile != "" {
		if err := startCluster(ctx, stopper, globalCounterSet); err != nil {
			panic(err)
		}
	} else if *configFile != "" {
		cfg := &Config{}
		if err := parseConfigFromFile(*configFile, cfg); err != nil {
			panic(fmt.Sprintf("failed to parse config from %s, error: %s", *configFile, err.Error()))
		}
		if err := startService(ctx, cfg, stopper, globalCounterSet); err != nil {
			panic(err)
		}
	} else {
		panic(errors.New("no configuration specified"))
	}

	waitSignalToStop(stopper)
	logutil.GetGlobalLogger().Info("Shutdown complete")
}

func waitSignalToStop(stopper *stopper.Stopper) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigchan
	logutil.GetGlobalLogger().Info("Starting shutdown...", zap.String("signal", sig.String()))
	stopper.Stop()
	if cnProxy != nil {
		if err := cnProxy.Stop(); err != nil {
			logutil.GetGlobalLogger().Error("shutdown cn proxy failed", zap.Error(err))
		}
	}
}

func startService(ctx context.Context, cfg *Config, stopper *stopper.Stopper, globalCounterSet *perfcounter.CounterSet) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if err := cfg.resolveGossipSeedAddresses(); err != nil {
		return err
	}
	setupProcessLevelRuntime(cfg, stopper)

	st, err := cfg.getServiceType()
	if err != nil {
		return err
	}

	uuid, err := getNodeUUID(ctx, st, cfg)
	if err != nil {
		return err
	}

	fs, err := cfg.createFileService(defines.LocalFileServiceName, globalCounterSet, st, uuid)
	if err != nil {
		return err
	}

	etlFS, err := fileservice.Get[fileservice.FileService](fs, defines.ETLFileServiceName)
	if err != nil {
		return err
	}
	if err = initTraceMetric(ctx, st, cfg, stopper, etlFS, uuid); err != nil {
		return err
	}

	switch st {
	case metadata.ServiceType_CN:
		return startCNService(cfg, stopper, fs, globalCounterSet)
	case metadata.ServiceType_DN:
		return startDNService(cfg, stopper, fs, globalCounterSet)
	case metadata.ServiceType_LOG:
		return startLogService(cfg, stopper, fs, globalCounterSet)
	default:
		panic("unknown service type")
	}
}

func startCNService(
	cfg *Config,
	stopper *stopper.Stopper,
	fileService fileservice.FileService,
	perfCounterSet *perfcounter.CounterSet,
) error {
	if err := waitClusterCondition(cfg.HAKeeperClient, waitAnyShardReady); err != nil {
		return err
	}
	return stopper.RunNamedTask("cn-service", func(ctx context.Context) {
		ctx = perfcounter.WithCounterSet(ctx, perfCounterSet)
		c := cfg.getCNServiceConfig()
		s, err := cnservice.NewService(
			&c,
			ctx,
			fileService,
			cnservice.WithLogger(logutil.GetGlobalLogger().Named("cn-service").With(zap.String("uuid", cfg.CN.UUID))),
			cnservice.WithMessageHandle(compile.CnServerMessageHandler),
		)
		if err != nil {
			panic(err)
		}
		if err := s.Start(); err != nil {
			panic(err)
		}
		// TODO: global client need to refactor
		err = cnclient.NewCNClient(
			c.ServiceAddress,
			&cnclient.ClientConfig{RPC: cfg.getCNServiceConfig().RPC})
		if err != nil {
			panic(err)
		}

		<-ctx.Done()
		if err := s.Close(); err != nil {
			panic(err)
		}
		if err := cnclient.CloseCNClient(); err != nil {
			panic(err)
		}
	})
}

func startDNService(
	cfg *Config,
	stopper *stopper.Stopper,
	fileService fileservice.FileService,
	perfCounterSet *perfcounter.CounterSet,
) error {
	if err := waitClusterCondition(cfg.HAKeeperClient, waitHAKeeperRunning); err != nil {
		return err
	}
	r, err := getRuntime(metadata.ServiceType_DN, cfg, stopper)
	if err != nil {
		return err
	}
	return stopper.RunNamedTask("dn-service", func(ctx context.Context) {
		ctx = perfcounter.WithCounterSet(ctx, perfCounterSet)
		c := cfg.getDNServiceConfig()
		s, err := dnservice.NewService(
			&c,
			r,
			fileService)
		if err != nil {
			panic(err)
		}
		if err := s.Start(); err != nil {
			panic(err)
		}

		<-ctx.Done()
		if err := s.Close(); err != nil {
			panic(err)
		}
	})
}

func startLogService(
	cfg *Config,
	stopper *stopper.Stopper,
	fileService fileservice.FileService,
	perfCounterSet *perfcounter.CounterSet,
) error {
	lscfg := cfg.getLogServiceConfig()
	s, err := logservice.NewService(lscfg, fileService,
		logservice.WithRuntime(runtime.ProcessLevelRuntime()))
	if err != nil {
		panic(err)
	}
	if err := s.Start(); err != nil {
		panic(err)
	}
	return stopper.RunNamedTask("log-service", func(ctx context.Context) {
		ctx = perfcounter.WithCounterSet(ctx, perfCounterSet)
		if cfg.LogService.BootstrapConfig.BootstrapCluster {
			logutil.Infof("bootstrapping hakeeper...")
			if err := s.BootstrapHAKeeper(ctx, cfg.LogService); err != nil {
				panic(err)
			}
		}

		<-ctx.Done()
		if err := s.Close(); err != nil {
			panic(err)
		}
	})
}

func getNodeUUID(ctx context.Context, st metadata.ServiceType, cfg *Config) (UUID string, err error) {
	switch st {
	case metadata.ServiceType_CN:
		// validate node_uuid
		var uuidErr error
		var nodeUUID uuid.UUID
		if nodeUUID, uuidErr = uuid.Parse(cfg.CN.UUID); uuidErr != nil {
			nodeUUID = uuid.New()
		}
		if err := util.SetUUIDNodeID(ctx, nodeUUID[:]); err != nil {
			return "", moerr.ConvertPanicError(ctx, err)
		}
		UUID = nodeUUID.String()
	case metadata.ServiceType_DN:
		UUID = cfg.DN.UUID
	case metadata.ServiceType_LOG:
		UUID = cfg.LogService.UUID
	}
	UUID = strings.ReplaceAll(UUID, " ", "_") // remove space in UUID for filename
	return
}

func initTraceMetric(ctx context.Context, st metadata.ServiceType, cfg *Config, stopper *stopper.Stopper, fs fileservice.FileService, UUID string) error {
	var writerFactory table.WriterFactory
	var err error
	var initWG sync.WaitGroup
	SV := cfg.getObservabilityConfig()

	nodeRole := st.String()
	if *launchFile != "" {
		nodeRole = "ALL"
	}

	if !SV.DisableTrace || !SV.DisableMetric {
		writerFactory = export.GetWriterFactory(fs, UUID, nodeRole, SV.LogsExtension)
		_ = table.SetPathBuilder(ctx, SV.PathBuilder)
	}
	if !SV.DisableTrace {
		initWG.Add(1)
		collector := export.NewMOCollector(ctx, export.WithOBCollectorConfig(&SV.OBCollectorConfig))
		stopper.RunNamedTask("trace", func(ctx context.Context) {
			if err = motrace.InitWithConfig(ctx,
				&SV,
				motrace.WithNode(UUID, nodeRole),
				motrace.WithBatchProcessor(collector),
				motrace.WithFSWriterFactory(writerFactory),
				motrace.WithSQLExecutor(nil),
			); err != nil {
				panic(err)
			}
			initWG.Done()
			<-ctx.Done()
			// flush trace/log/error framework
			if err = motrace.Shutdown(ctx); err != nil {
				logutil.Warn("Shutdown trace", logutil.ErrorField(err), logutil.NoReportFiled())
			}
		})
		initWG.Wait()
	}
	if !SV.DisableMetric {
		stopper.RunNamedTask("metric", func(ctx context.Context) {
			mometric.InitMetric(ctx, nil, &SV, UUID, nodeRole, mometric.WithWriterFactory(writerFactory))
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
		log.Printf("mo-service is running in daemon mode, child process is %d", cpid)
		os.Exit(0)
	}
}
