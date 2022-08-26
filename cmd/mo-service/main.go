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
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"

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
	// TODO: start other service
	switch strings.ToUpper(cfg.ServiceType) {
	case cnServiceType:
		return startCNService(cfg, stopper)
	case dnServiceType:
		return startDNService(cfg, stopper)
	case logServiceType:
		return startLogService(cfg, stopper)
	case standaloneServiceType:
		return startStandalone(cfg, stopper)
	default:
		panic("unknown service type")
	}
}

func startCNService(cfg *Config, stopper *stopper.Stopper) error {
	return stopper.RunNamedTask("cn-service", func(ctx context.Context) {
		c := cfg.getCNServiceConfig()
		s, err := cnservice.NewService(&c, ctx, cnservice.WithMessageHandle(compile.PipelineMessageHandle))
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

func startDNService(cfg *Config, stopper *stopper.Stopper) error {
	return stopper.RunNamedTask("dn-service", func(ctx context.Context) {
		c := cfg.getDNServiceConfig()
		s, err := dnservice.NewService(&c,
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

func startStandalone(cfg *Config, stopper *stopper.Stopper) error {

	// start log service
	if err := startLogService(cfg, stopper); err != nil {
		return err
	}

	// wait hakeeper ready
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*30)
	defer cancel()
	var client logservice.CNHAKeeperClient
	for {
		var err error
		client, err = logservice.NewCNHAKeeperClient(ctx, cfg.HAKeeperClient)
		if errors.Is(err, logservice.ErrNotHAKeeper) {
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
	if err := startDNService(cfg, stopper); err != nil {
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
	if err := startCNService(cfg, stopper); err != nil {
		return err
	}

	return nil
}
