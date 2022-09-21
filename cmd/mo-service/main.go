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
	"syscall"
	"time"

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

	"github.com/google/uuid"
)

var (
	configFile = flag.String("cfg", "./mo.toml", "toml configuration used to start mo-service")
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
	cfg, err := parseConfigFromFile(*configFile)
	if err != nil {
		panic(fmt.Sprintf("failed to parse config from %s, error: %s", *configFile, err.Error()))
	}

	setupLogger(cfg)

	stopper := stopper.NewStopper("main", stopper.WithLogger(logutil.GetGlobalLogger()))
	if err := startService(cfg, stopper); err != nil {
		panic(err)
	}
	waitSignalToStop(stopper)
}

func setupLogger(cfg *Config) {
	logutil.SetupMOLogger(&cfg.Log)
}

func waitSignalToStop(stopper *stopper.Stopper) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT)
	<-sigchan
	stopper.Stop()
}

func startService(cfg *Config, stopper *stopper.Stopper) error {

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
	case standaloneServiceType:
		return startStandalone(cfg, stopper, fs, ts)
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

func startStandalone(
	cfg *Config,
	stopper *stopper.Stopper,
	fileService fileservice.FileService,
	taskService taskservice.TaskService,
) error {

	// start log service
	if err := startLogService(cfg, stopper, fileService, taskService); err != nil {
		return err
	}

	// wait hakeeper ready
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*30)
	defer cancel()
	var client logservice.CNHAKeeperClient
	for {
		var err error
		client, err = logservice.NewCNHAKeeperClient(ctx, cfg.HAKeeperClient)
		if moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper) {
			// not ready
			logutil.Info("hakeeper not ready, retry")
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			return err
		}
		break
	}

	// start DN
	if err := startDNService(cfg, stopper, fileService); err != nil {
		return err
	}

	// wait shard ready
	for {
		if ok, err := func() (bool, error) {
			details, err := client.GetClusterDetails(ctx)
			if err != nil {
				return false, err
			}
			for _, store := range details.DNStores {
				if len(store.Shards) > 0 {
					return true, nil
				}
			}
			logutil.Info("shard not ready")
			return false, nil
		}(); err != nil {
			return err
		} else if ok {
			logutil.Info("shard ready")
			break
		}
		time.Sleep(time.Second)
	}

	// start CN
	if err := startCNService(cfg, stopper, fileService); err != nil {
		return err
	}

	return nil
}

func initTraceMetric(ctx context.Context, cfg *Config, stopper *stopper.Stopper, fs fileservice.FileService) error {
	var writerFactory export.FSWriterFactory
	var err error
	var UUID string
	SV := cfg.getObservabilityConfig()

	ServerType := strings.ToUpper(cfg.ServiceType)
	switch ServerType {
	case cnServiceType, standaloneServiceType:
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
		stopper.RunNamedTask("trace", func(ctx context.Context) {
			if ctx, err = trace.Init(ctx,
				trace.WithMOVersion(SV.MoVersion),
				trace.WithNode(UUID, ServerType),
				trace.EnableTracer(!SV.DisableTrace),
				trace.WithBatchProcessMode(SV.BatchProcessor),
				trace.WithFSWriterFactory(writerFactory),
				trace.DebugMode(SV.EnableTraceDebug),
				trace.WithSQLExecutor(nil),
			); err != nil {
				panic(err)
			}
			<-ctx.Done()
			// flush trace/log/error framework
			if err = trace.Shutdown(trace.DefaultContext()); err != nil {
				logutil.Error("Shutdown trace", logutil.ErrorField(err), logutil.NoReportFiled())
				panic(err)
			}
		})
	}
	if !SV.DisableMetric {
		metric.InitMetric(ctx, nil, &SV, UUID, metric.ALL_IN_ONE_MODE, metric.WithWriterFactory(writerFactory))
	}
	return nil
}
