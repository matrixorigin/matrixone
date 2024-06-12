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
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/backup"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

var (
	cnProxy goetty.Proxy
)

func startCluster(
	ctx context.Context,
	stopper *stopper.Stopper,
	shutdownC chan struct{},
) error {
	if *launchFile == "" {
		panic("launch file not set")
	}

	cfg := &LaunchConfig{}
	if err := parseConfigFromFile(*launchFile, cfg); err != nil {
		return err
	}

	if cfg.Dynamic.Enable {
		return startDynamicCluster(ctx, cfg, stopper, shutdownC)
	}

	/*
		When the mo started in local cluster, we save all config files.
		Because we can get all config files conveniently.
	*/
	backup.SaveLaunchConfigPath(backup.LaunchConfig, []string{*launchFile})
	backup.SaveLaunchConfigPath(backup.LogConfig, cfg.LogServiceConfigFiles)
	backup.SaveLaunchConfigPath(backup.DnConfig, cfg.TNServiceConfigsFiles)
	backup.SaveLaunchConfigPath(backup.CnConfig, cfg.CNServiceConfigsFiles)
	if err := startLogServiceCluster(ctx, cfg.LogServiceConfigFiles, stopper, shutdownC); err != nil {
		return err
	}
	if err := startTNServiceCluster(ctx, cfg.TNServiceConfigsFiles, stopper, shutdownC); err != nil {
		return err
	}
	if err := startCNServiceCluster(ctx, cfg.CNServiceConfigsFiles, stopper, shutdownC); err != nil {
		return err
	}
	if *withProxy {
		backup.SaveLaunchConfigPath(backup.ProxyConfig, cfg.ProxyServiceConfigsFiles)
		if err := startProxyServiceCluster(ctx, cfg.ProxyServiceConfigsFiles, stopper, shutdownC); err != nil {
			return err
		}
	}
	if err := startPythonUdfServiceCluster(ctx, cfg.PythonUdfServiceConfigsFiles, stopper, shutdownC); err != nil {
		return err
	}
	return nil
}

func startLogServiceCluster(
	ctx context.Context,
	files []string,
	stopper *stopper.Stopper,
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
		if err := startService(ctx, cfg, stopper, shutdownC); err != nil {
			return err
		}
	}
	return nil
}

func startTNServiceCluster(
	ctx context.Context,
	files []string,
	stopper *stopper.Stopper,
	shutdownC chan struct{},
) error {
	if len(files) == 0 {
		return moerr.NewBadConfig(context.Background(), "DN service config not set")
	}

	for _, file := range files {
		cfg := NewConfig()
		// mo boosting in standalone mode
		cfg.IsStandalone = true
		if err := parseConfigFromFile(file, cfg); err != nil {
			return err
		}
		if err := startService(ctx, cfg, stopper, shutdownC); err != nil {
			return err
		}
	}
	return nil
}

func startCNServiceCluster(
	ctx context.Context,
	files []string,
	stopper *stopper.Stopper,
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
		if err := startService(ctx, cfg, stopper, shutdownC); err != nil {
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
		if err := startService(ctx, cfg, stopper, shutdownC); err != nil {
			return err
		}
	}

	return nil
}

func startPythonUdfServiceCluster(
	ctx context.Context,
	files []string,
	stopper *stopper.Stopper,
	shutdownC chan struct{},
) error {
	if len(files) == 0 {
		return nil
	}

	for _, file := range files {
		cfg := NewConfig()
		if err := parseConfigFromFile(file, cfg); err != nil {
			return err
		}
		if err := startService(ctx, cfg, stopper, shutdownC); err != nil {
			return err
		}
	}
	return nil
}

func waitHAKeeperReady(cfg logservice.HAKeeperClientConfig) (logservice.CNHAKeeperClient, error) {
	getClient := func() (logservice.CNHAKeeperClient, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		client, err := logservice.NewCNHAKeeperClient(ctx, cfg)
		if err != nil {
			logutil.Errorf("hakeeper not ready, err: %v", err)
			return nil, err
		}
		return client, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return nil, moerr.NewInternalErrorNoCtx("wait hakeeper ready timeout")
		default:
			client, err := getClient()
			if err == nil {
				return client, nil
			}
			time.Sleep(time.Second)
		}
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
				if errors.Is(err, context.DeadlineExceeded) {
					logutil.Errorf("wait TN ready timeout: %s", err)
					return false, err
				}
				logutil.Errorf("failed to get cluster details %s", err)
				return false, nil
			}
			for _, store := range details.TNStores {
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
