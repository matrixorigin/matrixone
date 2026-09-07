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
	dynamicCNServiceProcesses    []*dynamicCNChild
	dynamicCNServiceCommands     [][]string
	dynamicChaosTester           dynamicChaosStopper
	dynamicCNStopping            bool
	launchStartDynamicCNServices = startDynamicCNServices
	dynamicStartProcess          = func(argv0 string, argv []string, attr *os.ProcAttr) (dynamicProcess, error) {
		process, err := os.StartProcess(argv0, argv, attr)
		if err != nil {
			return nil, err
		}
		return &osDynamicProcess{process: process}, nil
	}
	dynamicKill = func(child *dynamicCNChild, signal syscall.Signal) error {
		if child == nil || child.process == nil {
			return errors.New("dynamic cn child process is nil")
		}
		return child.process.signal(signal)
	}
	dynamicListenAndServe = http.ListenAndServe
	dynamicWaitProcess    = func(child *dynamicCNChild) dynamicWaitResult {
		if child == nil || child.process == nil {
			return dynamicWaitResult{err: errors.New("dynamic cn child process is nil")}
		}
		return child.process.wait()
	}
)

type dynamicProcess interface {
	pid() int
	signal(syscall.Signal) error
	wait() dynamicWaitResult
}

type dynamicWaitResult struct {
	reaped bool
	err    error
}

type osDynamicProcess struct {
	process *os.Process
}

func (p *osDynamicProcess) pid() int {
	return p.process.Pid
}

func (p *osDynamicProcess) signal(signal syscall.Signal) error {
	return p.process.Signal(signal)
}

func (p *osDynamicProcess) wait() dynamicWaitResult {
	state, err := p.process.Wait()
	if err != nil {
		return dynamicWaitResult{err: err}
	}
	result := dynamicWaitResult{reaped: true}
	if !state.Success() {
		result.err = fmt.Errorf("dynamic cn child exited unsuccessfully: %s", state.String())
	}
	return result
}

type dynamicCNChild struct {
	process dynamicProcess
	pid     int
}

type dynamicChaosStopper interface {
	Stop() error
}

var errDynamicCNStopping = errors.New("dynamic cn is stopping")

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
	if dynamicCNStopping {
		dynamicCNMu.Unlock()
		return errDynamicCNStopping
	}
	dynamicCNServiceCommands = make([][]string, cfg.ServiceCount)
	dynamicCNServicePIDs = make([]int, cfg.ServiceCount)
	dynamicCNServiceProcesses = make([]*dynamicCNChild, cfg.ServiceCount)
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
	cfg.Chaos.Restart.RestartFunc = restartDynamicCNByIndex
	chaosTester := chaos.NewChaosTester(cfg.Chaos)
	dynamicCNMu.Lock()
	if dynamicCNStopping {
		dynamicCNMu.Unlock()
		return errDynamicCNStopping
	}
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

func restartDynamicCNByIndex(index int) error {
	if err := startDynamicCNByIndex(index); err != nil {
		if errors.Is(err, errDynamicCNStopping) {
			return nil
		}
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
		dynamicListenAndServe(*httpListenAddr, nil)
	}()
	return nil
}

func stopDynamicCNByIndex(index int) error {
	dynamicCNMu.Lock()
	defer dynamicCNMu.Unlock()
	if index < 0 || index >= len(dynamicCNServicePIDs) {
		return errors.New("invalid dynamic cn index")
	}
	if dynamicCNStopping {
		return nil
	}
	pid := dynamicCNServicePIDs[index]
	if pid == 0 {
		return errors.New("dynamic cn is not running")
	}
	var child *dynamicCNChild
	if index < len(dynamicCNServiceProcesses) {
		child = dynamicCNServiceProcesses[index]
	}
	if err := dynamicKill(child, syscall.SIGKILL); err != nil {
		return err
	}
	if dynamicCNServicePIDs[index] == pid &&
		index < len(dynamicCNServiceProcesses) && dynamicCNServiceProcesses[index] == child {
		dynamicCNServicePIDs[index] = 0
		dynamicCNServiceProcesses[index] = nil
	}
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
	if dynamicCNStopping {
		return errDynamicCNStopping
	}
	if dynamicCNServicePIDs[index] != 0 {
		return errors.New("dynamic cn is already running")
	}
	command := append([]string(nil), dynamicCNServiceCommands[index]...)
	process, err := dynamicStartProcess(
		command[0],
		command,
		&os.ProcAttr{
			Dir: pwd,
			Env: os.Environ(),
			Sys: &syscall.SysProcAttr{
				Setsid: true,
			},
			Files: []*os.File{os.Stdin, os.Stdout, os.Stderr}, // print message to the same pty
		})
	if err != nil {
		return err
	}
	if process == nil {
		return errors.New("dynamic cn child process is nil")
	}
	child := &dynamicCNChild{process: process, pid: process.pid()}
	dynamicCNServicePIDs[index] = child.pid
	if index >= len(dynamicCNServiceProcesses) {
		dynamicCNServiceProcesses = append(dynamicCNServiceProcesses, make([]*dynamicCNChild, index-len(dynamicCNServiceProcesses)+1)...)
	}
	dynamicCNServiceProcesses[index] = child
	return nil
}

// stopAllDynamicCNServicesGracefully is used only by the ordered shutdown
// path; stopDynamicCNByIndex remains the abrupt-exit helper for chaos tests.
func stopAllDynamicCNServicesGracefully(ctx context.Context) error {
	dynamicCNMu.Lock()
	dynamicCNStopping = true
	chaosTester := dynamicChaosTester
	dynamicChaosTester = nil
	dynamicCNMu.Unlock()
	var errs error
	if chaosTester != nil {
		errs = errors.Join(errs, chaosTester.Stop())
	}
	type childSnapshot struct {
		index int
		pid   int
		child *dynamicCNChild
	}
	dynamicCNMu.RLock()
	children := make([]childSnapshot, 0, len(dynamicCNServicePIDs))
	for index, pid := range dynamicCNServicePIDs {
		if pid == 0 {
			continue
		}
		var child *dynamicCNChild
		if index < len(dynamicCNServiceProcesses) {
			child = dynamicCNServiceProcesses[index]
		}
		children = append(children, childSnapshot{index: index, pid: pid, child: child})
	}
	dynamicCNMu.RUnlock()
	type result struct {
		childSnapshot
		err error
	}
	results := make(chan result)
	startWait := func(child childSnapshot) {
		go func() {
			waitResult := dynamicWaitProcess(child.child)
			if waitResult.reaped {
				dynamicCNMu.Lock()
				if child.index < len(dynamicCNServicePIDs) &&
					dynamicCNServicePIDs[child.index] == child.pid &&
					child.index < len(dynamicCNServiceProcesses) &&
					dynamicCNServiceProcesses[child.index] == child.child {
					dynamicCNServicePIDs[child.index] = 0
					dynamicCNServiceProcesses[child.index] = nil
				}
				dynamicCNMu.Unlock()
			}
			results <- result{childSnapshot: child, err: waitResult.err}
		}()
	}
	for _, child := range children {
		if err := dynamicKill(child.child, syscall.SIGTERM); err != nil {
			errs = errors.Join(errs, err)
			if err := dynamicKill(child.child, syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
				errs = errors.Join(errs, err)
			}
		}
		startWait(child)
	}
	completed := 0
	recordResult := func(r result) {
		completed++
		if r.err != nil {
			errs = errors.Join(errs, r.err)
		}
	}
	for completed < len(children) {
		select {
		case r := <-results:
			recordResult(r)
		case <-ctx.Done():
			errs = errors.Join(errs, ctx.Err())
			for _, child := range children {
				dynamicCNMu.RLock()
				owned := child.index < len(dynamicCNServicePIDs) &&
					dynamicCNServicePIDs[child.index] == child.pid &&
					child.index < len(dynamicCNServiceProcesses) &&
					dynamicCNServiceProcesses[child.index] == child.child
				dynamicCNMu.RUnlock()
				if !owned {
					continue
				}
				if err := dynamicKill(child.child, syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
					errs = errors.Join(errs, err)
				}
			}
			for completed < len(children) {
				recordResult(<-results)
			}
		}
	}
	dynamicCNMu.RLock()
	for _, pid := range dynamicCNServicePIDs {
		if pid != 0 {
			errs = errors.Join(errs, fmt.Errorf("dynamic cn child pid %d remains owned after shutdown", pid))
		}
	}
	dynamicCNMu.RUnlock()
	return errs
}
