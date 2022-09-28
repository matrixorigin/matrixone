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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

func startCluster(stopper *stopper.Stopper) error {
	if *launchFile == "" {
		panic("launch file not set")
	}

	cfg := &LaunchConfig{}
	if err := parseConfigFromFile(*launchFile, cfg); err != nil {
		return err
	}

	client, err := startLogServiceCluster(cfg.LogServiceConfigFiles, stopper)
	if err != nil {
		return err
	}
	defer func() {
		if err := client.Close(); err != nil {
			logutil.Error("close hakeeper client failed", zap.Error(err))
		}
	}()

	if err := startDNServiceCluster(cfg.DNServiceConfigsFiles, stopper, client); err != nil {
		return err
	}
	if err := startCNServiceCluster(cfg.CNServiceConfigsFiles, stopper); err != nil {
		return err
	}
	return nil
}

func startLogServiceCluster(
	files []string,
	stopper *stopper.Stopper) (logservice.CNHAKeeperClient, error) {
	if len(files) == 0 {
		return nil, moerr.NewBadConfig("DN service config not set")
	}

	var cfg *Config
	for _, file := range files {
		cfg = &Config{}
		if err := parseConfigFromFile(file, cfg); err != nil {
			return nil, err
		}
		if err := startService(cfg, stopper); err != nil {
			return nil, err
		}
	}
	return waitHAKeeperReady(cfg.HAKeeperClient)
}

func startDNServiceCluster(
	files []string,
	stopper *stopper.Stopper,
	client logservice.CNHAKeeperClient) error {
	if len(files) == 0 {
		return moerr.NewBadConfig("DN service config not set")
	}

	for _, file := range files {
		cfg := &Config{}
		if err := parseConfigFromFile(file, cfg); err != nil {
			return err
		}
		if err := startService(cfg, stopper); err != nil {
			return nil
		}
	}
	return waitAnyShardReady(client)
}

func startCNServiceCluster(
	files []string,
	stopper *stopper.Stopper) error {
	if len(files) == 0 {
		return moerr.NewBadConfig("CN service config not set")
	}

	var cfg *Config
	for _, file := range files {
		cfg = &Config{}
		if err := parseConfigFromFile(file, cfg); err != nil {
			return err
		}
		if err := startService(cfg, stopper); err != nil {
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
