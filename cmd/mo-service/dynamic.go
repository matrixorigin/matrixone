// Copyright 2021 Matrix Origin
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
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixone/pkg/common/chaos"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	baseUUID         = 0
	basePort         = 18000
	baseFrontendPort = 16001
	baseUnixSocket   = 0
)

var (
	dynamicCNServicePIDs     []int
	dynamicCNServiceCommands [][]string
)

func startDynamicCluster(
	ctx context.Context,
	cfg *LaunchConfig,
	stopper *stopper.Stopper,
	shutdownC chan struct{},
) error {
	if err := startLogServiceCluster(ctx, cfg.LogServiceConfigFiles, stopper, shutdownC); err != nil {
		return err
	}
	if err := startTNServiceCluster(ctx, cfg.TNServiceConfigsFiles, stopper, shutdownC); err != nil {
		return err
	}
	if err := startDynamicCNServices("./mo-data", cfg.Dynamic); err != nil {
		return err
	}
	if *withProxy {
		if err := startProxyServiceCluster(ctx, cfg.ProxyServiceConfigsFiles, stopper, shutdownC); err != nil {
			return err
		}
	} else {
		// TODO: make configurable for 6001
		cnProxy = goetty.NewProxy("0.0.0.0:6001", logutil.GetGlobalLogger().Named("mysql-proxy"))
		for i := 0; i < cfg.Dynamic.ServiceCount; i++ {
			port := baseFrontendPort + i
			cnProxy.AddUpStream(fmt.Sprintf("127.0.0.1:%d", port), time.Second*10)
		}
		if err := cnProxy.Start(); err != nil {
			return err
		}
	}
	return startDynamicCtlHTTPServer(cfg.Dynamic.CtlAddress)
}

func startDynamicCNServices(
	baseDir string,
	cfg Dynamic) error {
	if err := genDynamicCNConfigs(baseDir, cfg); err != nil {
		return err
	}

	dynamicCNServiceCommands = make([][]string, cfg.ServiceCount)
	dynamicCNServicePIDs = make([]int, cfg.ServiceCount)
	for i := 0; i < cfg.ServiceCount; i++ {
		dynamicCNServiceCommands[i] = []string{
			os.Args[0],
			"-cfg", "./mo-data/cn-" + fmt.Sprintf("%d", i) + ".toml",
			"-max-processor", fmt.Sprintf("%d", cfg.CpuCount),
			"-debug-http", fmt.Sprintf("127.0.0.1:606%d", i),
		}
		if err := startDynamicCNByIndex(i); err != nil {
			return err
		}
	}
	if !cfg.Chaos.Enable {
		return nil
	}
	cfg.Chaos.Restart.KillFunc = stopDynamicCNByIndex
	cfg.Chaos.Restart.RestartFunc = startDynamicCNByIndex
	chaosTester := chaos.NewChaosTester(cfg.Chaos)
	return chaosTester.Start()
}

func genDynamicCNConfigs(
	baseDir string,
	cfg Dynamic) error {
	baseCNConfig, err := os.ReadFile(cfg.CNTemplate)
	if err != nil {
		return err
	}

	temps := make([]string, 0, cfg.ServiceCount)
	for i := 0; i < cfg.ServiceCount; i++ {
		uuid := baseUUID + i
		port := basePort + i*100
		frontendPort := baseFrontendPort + i
		unixSocketPort := baseUnixSocket + i

		cfgFile := fmt.Sprintf(
			string(baseCNConfig),
			uuid,
			port,
			i,
			frontendPort,
			unixSocketPort)
		f, err := os.CreateTemp(
			baseDir,
			"*.tmp")
		if err != nil {
			return err
		}
		if _, err := f.WriteString(cfgFile); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		temps = append(temps, f.Name())
	}

	d, err := os.Open(baseDir)
	if err != nil {
		return err
	}
	defer func() {
		if err := d.Close(); err != nil {
			panic(err)
		}
	}()
	for i := 0; i < cfg.ServiceCount; i++ {
		if err := os.Rename(
			filepath.Join(temps[i]),
			filepath.Join(baseDir, fmt.Sprintf("cn-%d.toml", i))); err != nil {
			return err
		}
	}
	if err := d.Sync(); err != nil {
		return err
	}
	return nil
}

func startDynamicCtlHTTPServer(addr string) error {
	if addr == "" {
		return nil
	}

	http.HandleFunc("/dynamic/cn",
		func(resp http.ResponseWriter, req *http.Request) {
			cn := req.URL.Query().Get("cn")
			action := req.URL.Query().Get("action")
			if cn == "" || action == "" {
				resp.WriteHeader(http.StatusBadRequest)
				resp.Write([]byte("invalid request"))
				return
			}

			index := format.MustParseStringInt(cn)
			if index < 0 || index >= len(dynamicCNServiceCommands) {
				resp.WriteHeader(http.StatusBadRequest)
				resp.Write([]byte("invalid request"))
				return
			}

			switch action {
			case "start":
				if dynamicCNServicePIDs[index] != 0 {
					resp.WriteHeader(http.StatusBadRequest)
					resp.Write([]byte("already started"))
					return
				}
				if err := startDynamicCNByIndex(index); err != nil {
					resp.Write([]byte(err.Error()))
				} else {
					resp.Write([]byte("OK"))
				}
			case "stop":
				if dynamicCNServicePIDs[index] == 0 {
					resp.WriteHeader(http.StatusBadRequest)
					resp.Write([]byte("already stopped"))
					return
				}

				if err := syscall.Kill(dynamicCNServicePIDs[index], syscall.SIGKILL); err != nil {
					resp.Write([]byte(err.Error()))
				} else {
					resp.Write([]byte("OK"))
					dynamicCNServicePIDs[index] = 0
				}
			default:
				resp.WriteHeader(http.StatusBadRequest)
				resp.Write([]byte("invalid request"))
				return
			}
		})
	go func() {
		http.ListenAndServe(*httpListenAddr, nil)
	}()
	return nil
}

func stopDynamicCNByIndex(index int) error {
	return syscall.Kill(dynamicCNServicePIDs[index], syscall.SIGKILL)
}

func startDynamicCNByIndex(index int) error {
	pwd, err := os.Getwd()
	if err != nil {
		return err
	}
	pid, err := syscall.ForkExec(
		dynamicCNServiceCommands[index][0],
		dynamicCNServiceCommands[index],
		&syscall.ProcAttr{
			Dir: pwd,
			Env: os.Environ(),
			Sys: &syscall.SysProcAttr{
				Setsid: true,
			},
			Files: []uintptr{0, 1, 2}, // print message to the same pty
		})
	if err != nil {
		return err
	}
	dynamicCNServicePIDs[index] = pid
	return nil
}

func stopAllDynamicCNServices() {
	for _, pid := range dynamicCNServicePIDs {
		syscall.Kill(pid, syscall.SIGKILL)
	}
}
