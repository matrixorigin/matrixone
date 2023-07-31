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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/backup"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"go.uber.org/zap"
)

var (
	cnProxy goetty.Proxy
)

func startCluster(
	ctx context.Context,
	stopper *stopper.Stopper,
	perfCounterSet *perfcounter.CounterSet,
	shutdownC chan struct{},
) error {
	if *launchFile == "" {
		panic("launch file not set")
	}

	cfg := &LaunchConfig{}
	if err := parseConfigFromFile(*launchFile, cfg); err != nil {
		return err
	}

	backup.SaveLaunchConfigPath([]string{*launchFile})
	backup.SaveLaunchConfigPath(cfg.LogServiceConfigFiles)
	backup.SaveLaunchConfigPath(cfg.DNServiceConfigsFiles)
	backup.SaveLaunchConfigPath(cfg.CNServiceConfigsFiles)
	if err := startLogServiceCluster(ctx, cfg.LogServiceConfigFiles, stopper, perfCounterSet, shutdownC); err != nil {
		return err
	}
	if err := startDNServiceCluster(ctx, cfg.DNServiceConfigsFiles, stopper, perfCounterSet, shutdownC); err != nil {
		return err
	}
	if err := startCNServiceCluster(ctx, cfg.CNServiceConfigsFiles, stopper, perfCounterSet, shutdownC); err != nil {
		return err
	}
	if *withProxy {
		backup.SaveLaunchConfigPath(cfg.ProxyServiceConfigsFiles)
		if err := startProxyServiceCluster(ctx, cfg.ProxyServiceConfigsFiles, stopper, perfCounterSet, shutdownC); err != nil {
			return err
		}
	}
	return nil
}

func startLogServiceCluster(
	ctx context.Context,
	files []string,
	stopper *stopper.Stopper,
	perfCounterSet *perfcounter.CounterSet,
	shutdownC chan struct{},
) error {
	if len(files) == 0 {
		return moerr.NewBadConfig(context.Background(), "Log service config not set")
	}

	var cfg *Config
	for _, file := range files {
		cfg = NewConfig()
		if err := parseConfigFromFile(file, cfg); err != nil {
			return err
		}
		if err := startService(ctx, cfg, stopper, perfCounterSet, shutdownC); err != nil {
			return err
		}
	}
	return nil
}

func startDNServiceCluster(
	ctx context.Context,
	files []string,
	stopper *stopper.Stopper,
	perfCounterSet *perfcounter.CounterSet,
	shutdownC chan struct{},
) error {
	if len(files) == 0 {
		return moerr.NewBadConfig(context.Background(), "DN service config not set")
	}

	for _, file := range files {
		cfg := NewConfig()
		if err := parseConfigFromFile(file, cfg); err != nil {
			return err
		}
		if err := startService(ctx, cfg, stopper, perfCounterSet, shutdownC); err != nil {
			return nil
		}
	}
	return nil
}

func startCNServiceCluster(
	ctx context.Context,
	files []string,
	stopper *stopper.Stopper,
	perfCounterSet *perfcounter.CounterSet,
	shutdownC chan struct{},
) error {
	if len(files) == 0 {
		return moerr.NewBadConfig(context.Background(), "CN service config not set")
	}

	upstreams := []string{}

	var cfg *Config
	for _, file := range files {
		cfg = NewConfig()
		if err := parseConfigFromFile(file, cfg); err != nil {
			return err
		}
		upstreams = append(upstreams, fmt.Sprintf("127.0.0.1:%d", cfg.getCNServiceConfig().Frontend.Port))
		if err := startService(ctx, cfg, stopper, perfCounterSet, shutdownC); err != nil {
			return err
		}
	}

	if len(upstreams) > 1 {
		// TODO: make configurable for 6001
		cnProxy = goetty.NewProxy("0.0.0.0:6001", logutil.GetGlobalLogger().Named("mysql-proxy"))
		for _, address := range upstreams {
			cnProxy.AddUpStream(address, time.Second*10)
		}
		if err := cnProxy.Start(); err != nil {
			return err
		}
	}
	return nil
}

func startProxyServiceCluster(
	ctx context.Context,
	files []string,
	stopper *stopper.Stopper,
	perfCounterSet *perfcounter.CounterSet,
	shutdownC chan struct{},
) error {
	if len(files) == 0 {
		return moerr.NewBadConfig(context.Background(), "Proxy service config not set")
	}

	var cfg *Config
	for _, file := range files {
		cfg = NewConfig()
		if err := parseConfigFromFile(file, cfg); err != nil {
			return err
		}
		if err := startService(ctx, cfg, stopper, perfCounterSet, shutdownC); err != nil {
			return err
		}
	}

	return nil
}

func waitHAKeeperReady(cfg logservice.HAKeeperClientConfig) (logservice.CNHAKeeperClient, error) {
	// wait hakeeper ready
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*30)
	defer cancel()
	for {
		var err error
		client, err := logservice.NewCNHAKeeperClient(ctx, cfg)
		if moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper) {
			// not ready
			logutil.Info("hakeeper not ready, retry")
			time.Sleep(time.Second)
			continue
		}
		return client, err
	}
}

func waitHAKeeperRunning(client logservice.CNHAKeeperClient) error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute*2)
	defer cancel()

	// wait HAKeeper running
	for {
		state, err := client.GetClusterState(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper) ||
			state.State != logpb.HAKeeperRunning {
			// not ready
			logutil.Info("hakeeper not ready, retry")
			time.Sleep(time.Second)
			continue
		}
		return err
	}
}

func waitAnyShardReady(client logservice.CNHAKeeperClient) error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*30)
	defer cancel()

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
			return nil
		}
		time.Sleep(time.Second)
	}
}

func waitClusterCondition(
	cfg logservice.HAKeeperClientConfig,
	waitFunc func(logservice.CNHAKeeperClient) error,
) error {
	client, err := waitHAKeeperReady(cfg)
	if err != nil {
		return err
	}
	if err := waitFunc(client); err != nil {
		return err
	}
	if err := client.Close(); err != nil {
		logutil.Error("close hakeeper client failed", zap.Error(err))
	}
	return nil
}
