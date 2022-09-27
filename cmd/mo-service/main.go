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
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/dnservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

var (
	configFile = flag.String("cfg", "./mo.toml", "toml configuration used to start mo-service")
	launchFile = flag.String("launch", "", "toml configuration used to launch mo cluster")
	version    = flag.Bool("version", false, "print version information")
)

func main() {
	flag.Parse()
	maybePrintVersion()

	if *cpuProfilePathFlag != "" {
		stop := startCPUProfile()
		defer stop()
	}
	if *allocsProfilePathFlag != "" {
		defer writeAllocsProfile()
	}
	rand.Seed(time.Now().UnixNano())

	stopper := stopper.NewStopper("main", stopper.WithLogger(logutil.GetGlobalLogger()))
	if *launchFile != "" {
		if err := startCluster(stopper); err != nil {
			panic(err)
		}
	} else if *configFile != "" {
		cfg := &Config{}
		if err := parseConfigFromFile(*configFile, cfg); err != nil {
			panic(fmt.Sprintf("failed to parse config from %s, error: %s", *configFile, err.Error()))
		}
		if err := startService(cfg, stopper); err != nil {
			panic(err)
		}
	}

	waitSignalToStop(stopper)
}

func waitSignalToStop(stopper *stopper.Stopper) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT)
	<-sigchan
	stopper.Stop()
}

func startService(cfg *Config, stopper *stopper.Stopper) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if err := cfg.resolveGossipSeedAddresses(); err != nil {
		return err
	}

	setupGlobalComponents(cfg, stopper)

	fs, err := cfg.createFileService(localFileServiceName)
	if err != nil {
		return err
	}

	// TODO: Use real task storage. And Each service initializes the logger with its own UUID
	ts := taskservice.NewTaskService(taskservice.NewMemTaskStorage(),
		logutil.GetGlobalLogger().With(zap.String("node", cfg.LogService.UUID)))

	if err = initTraceMetric(context.Background(), cfg, stopper, fs); err != nil {
		return err
	}

	switch strings.ToUpper(cfg.ServiceType) {
	case cnServiceType:
		return startCNService(cfg, stopper, fs)
	case dnServiceType:
		return startDNService(cfg, stopper, fs)
	case logServiceType:
		return startLogService(cfg, stopper, fs, ts)
	default:
		panic("unknown service type")
	}
}

func startCNService(
	cfg *Config,
	stopper *stopper.Stopper,
	fileService fileservice.FileService,
) error {
	return stopper.RunNamedTask("cn-service", func(ctx context.Context) {
		c := cfg.getCNServiceConfig()
		s, err := cnservice.NewService(
			&c,
			ctx,
			fileService,
			cnservice.WithMessageHandle(compile.CnServerMessageHandler),
		)
		if err != nil {
			panic(err)
		}
		if err := s.Start(); err != nil {
			panic(err)
		}
		err = cnclient.NewCNClient(&cnclient.ClientConfig{})
		if err != nil {
			panic(err)
		}

		<-ctx.Done()
		if err := s.Close(); err != nil {
			panic(err)
		}
	})
}

func startDNService(
	cfg *Config,
	stopper *stopper.Stopper,
	fileService fileservice.FileService,
) error {
	return stopper.RunNamedTask("dn-service", func(ctx context.Context) {
		c := cfg.getDNServiceConfig()
		s, err := dnservice.NewService(
			&c,
			fileService,
			dnservice.WithLogger(logutil.GetGlobalLogger().Named("dn-service")))
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
	taskService taskservice.TaskService,
) error {
	lscfg := cfg.getLogServiceConfig()
	s, err := logservice.NewService(lscfg, fileService, taskService)
	if err != nil {
		panic(err)
	}
	if err := s.Start(); err != nil {
		panic(err)
	}
	return stopper.RunNamedTask("log-service", func(ctx context.Context) {
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

func initTraceMetric(ctx context.Context, cfg *Config, stopper *stopper.Stopper, fs fileservice.FileService) error {
	var writerFactory export.FSWriterFactory
	var err error
	var UUID string
	var initWG sync.WaitGroup
	SV := cfg.getObservabilityConfig()

	ServerType := strings.ToUpper(cfg.ServiceType)
	switch ServerType {
	case cnServiceType:
		// validate node_uuid
		var uuidErr error
		var nodeUUID uuid.UUID
		if nodeUUID, uuidErr = uuid.Parse(cfg.CN.UUID); uuidErr != nil {
			nodeUUID = uuid.New()
		}
		if err := util.SetUUIDNodeID(nodeUUID[:]); err != nil {
			return moerr.ConvertPanicError(err)
		}
		UUID = nodeUUID.String()
	case dnServiceType:
		UUID = cfg.DN.UUID
	case logServiceType:
		UUID = cfg.LogService.UUID
	}
	UUID = strings.ReplaceAll(UUID, " ", "_") // remove space in UUID for filename

	if !SV.DisableTrace || !SV.DisableMetric {
		writerFactory = export.GetFSWriterFactory(fs, UUID, ServerType)
	}
	if !SV.DisableTrace {
		initWG.Add(1)
		stopper.RunNamedTask("trace", func(ctx context.Context) {
			if ctx, err = trace.Init(ctx,
				trace.WithMOVersion(SV.MoVersion),
				trace.WithNode(UUID, ServerType),
				trace.EnableTracer(!SV.DisableTrace),
				trace.WithBatchProcessMode(SV.BatchProcessor),
				trace.WithFSWriterFactory(writerFactory),
				trace.WithExportInterval(SV.TraceExportInterval),
				trace.WithLongQueryTime(SV.LongQueryTime),
				trace.DebugMode(SV.EnableTraceDebug),
				trace.WithSQLExecutor(nil),
			); err != nil {
				panic(err)
			}
			initWG.Done()
			<-ctx.Done()
			// flush trace/log/error framework
			if err = trace.Shutdown(trace.DefaultContext()); err != nil {
				logutil.Error("Shutdown trace", logutil.ErrorField(err), logutil.NoReportFiled())
				panic(err)
			}
		})
		initWG.Wait()
	}
	if !SV.DisableMetric {
		metric.InitMetric(ctx, nil, &SV, UUID, ServerType, metric.WithWriterFactory(writerFactory))
	}
	return nil
}
