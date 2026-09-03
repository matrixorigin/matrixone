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
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

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
	dynamicCNMu                  sync.RWMutex
	dynamicCNServicePIDs         []int
	dynamicCNServiceCommands     [][]string
	dynamicChaosTester           *chaos.ChaosTester
	launchStartDynamicCNServices = startDynamicCNServices
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
	if _, err := startTNServiceCluster(ctx, cfg.TNServiceConfigsFiles, stopper, shutdownC); err != nil {
		return err
	}
	// Register the dynamic-CN cleanup before starting the first child.  A
	// partial startup must be cleaned by the same ordered supervisor path when
	// a later child (or the chaos tester) fails to start.
	serviceLifecycle.setDynamicCNStop(stopAllDynamicCNServicesGracefully)
	if err := launchStartDynamicCNServices("./mo-data", cfg.Dynamic); err != nil {
		return err
	}
	if *withProxy {
		if err := startProxyServiceCluster(ctx, cfg.ProxyServiceConfigsFiles, stopper, shutdownC); err != nil {
			return err
		}
	}

	proxyOwns6001 := false
	if *withProxy {
		var err error
		proxyOwns6001, err = proxyServiceOwnsPort(cfg.ProxyServiceConfigsFiles, 6001)
		if err != nil {
			return err
		}
	}
	if err := startDynamicBuiltinProxy(cfg.Dynamic.ServiceCount, proxyOwns6001); err != nil {
		return err
	}
	// }
	return startDynamicCtlHTTPServer(cfg.Dynamic.CtlAddress)
}

func startDynamicBuiltinProxy(serviceCount int, proxyOwns6001 bool) error {
	if !shouldStartDynamicBuiltinProxy(serviceCount, proxyOwns6001) {
		return nil
	}
	proxy := launchNewProxy("0.0.0.0:6001", logutil.GetGlobalLogger().Named("mysql-proxy"))
	for i := 0; i < serviceCount; i++ {
		port := baseFrontendPort + i
		proxy.AddUpStream(fmt.Sprintf("127.0.0.1:%d", port), time.Second*10)
	}
	if err := proxy.Start(); err != nil {
		return err
	}
	cnProxy = proxy
	return nil
}

func shouldStartDynamicBuiltinProxy(serviceCount int, proxyOwns6001 bool) bool {
	return serviceCount > 0 && !proxyOwns6001
}

func startDynamicCNServices(
	baseDir string,
	cfg Dynamic) error {
	if err := genDynamicCNConfigs(baseDir, cfg); err != nil {
		return err
	}

	dynamicCNMu.Lock()
	dynamicCNServiceCommands = make([][]string, cfg.ServiceCount)
	dynamicCNServicePIDs = make([]int, cfg.ServiceCount)
	dynamicCNMu.Unlock()
	for i := 0; i < cfg.ServiceCount; i++ {
		command := []string{
			os.Args[0],
			"-cfg", "./mo-data/cn-" + fmt.Sprintf("%d", i) + ".toml",
			"-max-processor", fmt.Sprintf("%d", cfg.CpuCount),
			"-debug-http", fmt.Sprintf("127.0.0.1:606%d", i),
		}
		dynamicCNMu.Lock()
		dynamicCNServiceCommands[i] = command
		dynamicCNMu.Unlock()
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
	dynamicCNMu.Lock()
	dynamicChaosTester = chaosTester
	dynamicCNMu.Unlock()
	if err := chaosTester.Start(); err != nil {
		dynamicCNMu.Lock()
		if dynamicChaosTester == chaosTester {
			dynamicChaosTester = nil
		}
		dynamicCNMu.Unlock()
		return err
	}
	return nil
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
			dynamicCNMu.RLock()
			valid := index >= 0 && index < len(dynamicCNServiceCommands)
			pid := 0
			if valid {
				pid = dynamicCNServicePIDs[index]
			}
			dynamicCNMu.RUnlock()
			if !valid {
				resp.WriteHeader(http.StatusBadRequest)
				resp.Write([]byte("invalid request"))
				return
			}

			switch action {
			case "start":
				if pid != 0 {
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
				if pid == 0 {
					resp.WriteHeader(http.StatusBadRequest)
					resp.Write([]byte("already stopped"))
					return
				}

				if err := stopDynamicCNByIndex(index); err != nil {
					resp.Write([]byte(err.Error()))
				} else {
					resp.Write([]byte("OK"))
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
	dynamicCNMu.RLock()
	if index < 0 || index >= len(dynamicCNServicePIDs) {
		dynamicCNMu.RUnlock()
		return errors.New("invalid dynamic cn index")
	}
	pid := dynamicCNServicePIDs[index]
	dynamicCNMu.RUnlock()
	if pid == 0 {
		return errors.New("dynamic cn is not running")
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		return err
	}
	dynamicCNMu.Lock()
	if dynamicCNServicePIDs[index] == pid {
		dynamicCNServicePIDs[index] = 0
	}
	dynamicCNMu.Unlock()
	return nil
}

func startDynamicCNByIndex(index int) error {
	pwd, err := os.Getwd()
	if err != nil {
		return err
	}
	dynamicCNMu.Lock()
	defer dynamicCNMu.Unlock()
	if index < 0 || index >= len(dynamicCNServiceCommands) {
		return errors.New("invalid dynamic cn index")
	}
	if dynamicCNServicePIDs[index] != 0 {
		return errors.New("dynamic cn is already running")
	}
	command := append([]string(nil), dynamicCNServiceCommands[index]...)
	pid, err := syscall.ForkExec(
		command[0],
		command,
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

// stopAllDynamicCNServicesGracefully is used only by the ordered shutdown
// path; stopDynamicCNByIndex remains the abrupt-exit helper for chaos tests.
func stopAllDynamicCNServicesGracefully(ctx context.Context) error {
	dynamicCNMu.Lock()
	chaosTester := dynamicChaosTester
	dynamicChaosTester = nil
	pids := append([]int(nil), dynamicCNServicePIDs...)
	dynamicCNMu.Unlock()
	var errs error
	if chaosTester != nil {
		errs = errors.Join(errs, chaosTester.Stop())
	}
	type result struct {
		index int
		err   error
	}
	results := make(chan result, len(pids))
	count := 0
	for i, pid := range pids {
		if pid == 0 {
			continue
		}
		count++
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
			results <- result{index: i, err: err}
			continue
		}
		go func(index, childPID int) {
			p, err := os.FindProcess(childPID)
			if err == nil {
				_, err = p.Wait()
			}
			results <- result{index: index, err: err}
		}(i, pid)
	}
	for i := 0; i < count; i++ {
		select {
		case r := <-results:
			if r.err != nil {
				errs = errors.Join(errs, r.err)
			} else {
				dynamicCNMu.Lock()
				if r.index < len(dynamicCNServicePIDs) && dynamicCNServicePIDs[r.index] == pids[r.index] {
					dynamicCNServicePIDs[r.index] = 0
				}
				dynamicCNMu.Unlock()
			}
		case <-ctx.Done():
			return errors.Join(errs, ctx.Err())
		}
	}
	return errs
}
