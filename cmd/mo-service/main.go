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
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/dnservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	configFile = flag.String("cfg", "./mo.toml", "toml configuration used to start mo-service")
	version    = flag.Bool("version", false, "print version information")
)

func main() {
	flag.Parse()
	maybePrintVersion()

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
	// TODO: start other service
	switch strings.ToUpper(cfg.ServiceType) {
	case cnServiceType:
		panic("not implemented")
	case dnServiceType:
		return startDNService(cfg, stopper)
	case logServiceType:
		return startLogService(cfg, stopper)
	case standaloneServiceType:
		panic("not implemented")
	default:
		panic("unknown service type")
	}
}

func startDNService(cfg *Config, stopper *stopper.Stopper) error {
	return stopper.RunNamedTask("dn-service", func(ctx context.Context) {
		s, err := dnservice.NewService(&cfg.DN,
			cfg.createFileService,
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

func startLogService(cfg *Config, stopper *stopper.Stopper) error {
	lscfg := cfg.getLogServiceConfig()
	s, err := logservice.NewService(lscfg)
	if err != nil {
		panic(err)
	}
	if err := s.Start(); err != nil {
		panic(err)
	}
	return stopper.RunNamedTask("log-service", func(ctx context.Context) {
		if cfg.LogService.BootstrapConfig.BootstrapCluster {
			fmt.Printf("bootstrapping hakeeper...\n")
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
