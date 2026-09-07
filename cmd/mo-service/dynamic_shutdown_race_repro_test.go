// Copyright 2026 Matrix Origin
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
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingDynamicChaosStopper struct {
	stopStartedOnce sync.Once
	stopStarted     chan struct{}
	releaseOnce     sync.Once
	release         chan struct{}
}

type replacingDynamicChaosStopper struct{}

func (s *replacingDynamicChaosStopper) Stop() error {
	dynamicCNMu.Lock()
	dynamicCNServicePIDs[0] = 101
	dynamicCNServiceProcesses[0] = newTestDynamicChild(101)
	dynamicCNMu.Unlock()
	return nil
}

func newBlockingDynamicChaosStopper() *blockingDynamicChaosStopper {
	return &blockingDynamicChaosStopper{
		stopStarted: make(chan struct{}),
		release:     make(chan struct{}),
	}
}

func (s *blockingDynamicChaosStopper) Stop() error {
	s.stopStartedOnce.Do(func() { close(s.stopStarted) })
	<-s.release
	return nil
}

func (s *blockingDynamicChaosStopper) releaseStop() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func newRealDynamicChild(t *testing.T, argv0 string, argv []string) *dynamicCNChild {
	t.Helper()
	process, err := os.StartProcess(argv0, argv, &os.ProcAttr{
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	})
	require.NoError(t, err)
	child := &dynamicCNChild{
		process: &osDynamicProcess{process: process},
		pid:     process.Pid,
	}
	t.Cleanup(func() {
		_ = process.Kill()
		_, _ = process.Wait()
	})
	return child
}

func TestDynamicShutdownUsesFinalPIDSnapshotAfterChaosQuiesces(t *testing.T) {
	setLaunchTestHooks(t)
	dynamicCNMu.Lock()
	dynamicCNServiceCommands = [][]string{{"mo-service", "-cfg", "cn.toml"}}
	dynamicChaosTester = &replacingDynamicChaosStopper{}
	dynamicCNMu.Unlock()
	setDynamicTestSlots(100)

	type killedProcess struct {
		pid    int
		signal syscall.Signal
	}
	var killed []killedProcess
	var killedChild *dynamicCNChild
	var waitedChild *dynamicCNChild
	dynamicKill = func(child *dynamicCNChild, signal syscall.Signal) error {
		killedChild = child
		killed = append(killed, killedProcess{pid: child.pid, signal: signal})
		return nil
	}
	dynamicWaitProcess = func(child *dynamicCNChild) dynamicWaitResult {
		waitedChild = child
		require.Equal(t, 101, child.pid)
		return dynamicWaitResult{reaped: true}
	}

	require.NoError(t, stopAllDynamicCNServicesGracefully(context.Background()))
	require.Equal(t, []killedProcess{{pid: 101, signal: syscall.SIGTERM}}, killed)
	require.Same(t, killedChild, waitedChild)
	dynamicCNMu.RLock()
	defer dynamicCNMu.RUnlock()
	require.Zero(t, dynamicCNServicePIDs[0])
}

func TestDynamicShutdownDoesNotEscalateAfterWaitReapsChild(t *testing.T) {
	setLaunchTestHooks(t)
	child := &dynamicCNChild{
		process: &testDynamicProcess{
			processPID: 100,
			waitedC:    make(chan struct{}),
		},
		pid: 100,
	}
	dynamicCNMu.Lock()
	dynamicCNServicePIDs = []int{100}
	dynamicCNServiceProcesses = []*dynamicCNChild{child}
	dynamicCNMu.Unlock()

	process := child.process.(*testDynamicProcess)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- stopAllDynamicCNServicesGracefully(ctx) }()

	<-process.waitedC
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		dynamicCNMu.RLock()
		cleared := dynamicCNServicePIDs[0] == 0 && dynamicCNServiceProcesses[0] == nil
		dynamicCNMu.RUnlock()
		if cleared {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("reaped child slot was not cleared before result consumption")
		default:
			runtime.Gosched()
		}
	}
	cancel()

	require.NoError(t, <-done)
	process.mu.Lock()
	signals := append([]syscall.Signal(nil), process.signals...)
	process.mu.Unlock()
	require.Equal(t, []syscall.Signal{syscall.SIGTERM}, signals)
}

func TestDynamicShutdownReportsChildExitOutcomeAndReleasesSlot(t *testing.T) {
	setLaunchTestHooks(t)
	tests := []struct {
		name        string
		argv0       string
		argv        []string
		kill        func(*dynamicCNChild, syscall.Signal) error
		wantErr     bool
		wantErrText string
	}{
		{
			name:    "clean exit",
			argv0:   "/usr/bin/true",
			argv:    []string{"true"},
			kill:    func(*dynamicCNChild, syscall.Signal) error { return nil },
			wantErr: false,
		},
		{
			name:        "nonzero exit",
			argv0:       "/bin/sh",
			argv:        []string{"sh", "-c", "exit 23"},
			kill:        func(*dynamicCNChild, syscall.Signal) error { return nil },
			wantErr:     true,
			wantErrText: "exit status 23",
		},
		{
			name:        "signal exit",
			argv0:       "/bin/sleep",
			argv:        []string{"sleep", "60"},
			kill:        dynamicKill,
			wantErr:     true,
			wantErrText: "signal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			child := newRealDynamicChild(t, tt.argv0, tt.argv)
			dynamicKill = tt.kill
			dynamicCNMu.Lock()
			dynamicCNStopping = false
			dynamicCNServicePIDs = []int{child.pid}
			dynamicCNServiceProcesses = []*dynamicCNChild{child}
			dynamicCNMu.Unlock()

			err := stopAllDynamicCNServicesGracefully(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErrText)
			} else {
				require.NoError(t, err)
			}
			dynamicCNMu.RLock()
			require.Zero(t, dynamicCNServicePIDs[0])
			require.Nil(t, dynamicCNServiceProcesses[0])
			dynamicCNMu.RUnlock()
		})
	}
}

func TestDynamicShutdownEscalatesAndReapsRealChildAfterTimeout(t *testing.T) {
	setLaunchTestHooks(t)
	child := newRealDynamicChild(t, "/bin/sleep", []string{"sleep", "60"})
	var signals []syscall.Signal
	dynamicKill = func(child *dynamicCNChild, signal syscall.Signal) error {
		signals = append(signals, signal)
		if signal == syscall.SIGTERM {
			return nil
		}
		return child.process.signal(signal)
	}
	dynamicCNMu.Lock()
	dynamicCNStopping = false
	dynamicCNServicePIDs = []int{child.pid}
	dynamicCNServiceProcesses = []*dynamicCNChild{child}
	dynamicCNMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := stopAllDynamicCNServicesGracefully(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL}, signals)
	require.Error(t, child.process.signal(syscall.Signal(0)))
	dynamicCNMu.RLock()
	require.Zero(t, dynamicCNServicePIDs[0])
	require.Nil(t, dynamicCNServiceProcesses[0])
	dynamicCNMu.RUnlock()
}

func TestDynamicShutdownRejectsHTTPStartAfterStopping(t *testing.T) {
	setLaunchTestHooks(t)
	oldMux := http.DefaultServeMux
	oldListenAddr := *httpListenAddr
	chaosStopper := newBlockingDynamicChaosStopper()
	t.Cleanup(func() {
		chaosStopper.releaseStop()
		http.DefaultServeMux = oldMux
		*httpListenAddr = oldListenAddr
	})
	http.DefaultServeMux = http.NewServeMux()
	*httpListenAddr = "127.0.0.1:bad"
	dynamicCNMu.Lock()
	dynamicCNServiceCommands = [][]string{{"mo-service", "-cfg", "cn.toml"}}
	dynamicChaosTester = chaosStopper
	dynamicCNMu.Unlock()
	setDynamicTestSlots(0)

	forkCalls := 0
	dynamicStartProcess = func(string, []string, *os.ProcAttr) (dynamicProcess, error) {
		forkCalls++
		return &testDynamicProcess{processPID: 101}, nil
	}
	serverDone := make(chan struct{})
	dynamicListenAndServe = func(string, http.Handler) error {
		close(serverDone)
		return nil
	}
	require.NoError(t, startDynamicCtlHTTPServer(*httpListenAddr))
	<-serverDone

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- stopAllDynamicCNServicesGracefully(context.Background())
	}()
	<-chaosStopper.stopStarted

	request := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		resp := httptest.NewRecorder()
		http.DefaultServeMux.ServeHTTP(resp, req)
		return resp
	}
	require.Equal(t, errDynamicCNStopping.Error(), request("/dynamic/cn?cn=0&action=start").Body.String())
	require.Zero(t, forkCalls)
	chaosStopper.releaseStop()

	require.NoError(t, <-shutdownDone)
	dynamicCNMu.RLock()
	defer dynamicCNMu.RUnlock()
	require.Zero(t, dynamicCNServicePIDs[0])
}

func TestDynamicShutdownRejectsChaosRestartAfterStopping(t *testing.T) {
	setLaunchTestHooks(t)
	chaosStopper := newBlockingDynamicChaosStopper()
	t.Cleanup(chaosStopper.releaseStop)
	dynamicCNMu.Lock()
	dynamicCNServiceCommands = [][]string{{"mo-service", "-cfg", "cn.toml"}}
	dynamicChaosTester = chaosStopper
	dynamicCNMu.Unlock()
	setDynamicTestSlots(0)

	forkCalls := 0
	dynamicStartProcess = func(string, []string, *os.ProcAttr) (dynamicProcess, error) {
		forkCalls++
		return &testDynamicProcess{processPID: 101}, nil
	}

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- stopAllDynamicCNServicesGracefully(context.Background())
	}()
	<-chaosStopper.stopStarted

	// This is the exact cfg.Chaos.Restart.RestartFunc producer installed by
	// startDynamicCNServices.
	require.NoError(t, restartDynamicCNByIndex(0))
	require.ErrorIs(t, startDynamicCNByIndex(0), errDynamicCNStopping)
	require.Zero(t, forkCalls)
	chaosStopper.releaseStop()

	require.NoError(t, <-shutdownDone)
	dynamicCNMu.RLock()
	defer dynamicCNMu.RUnlock()
	require.Zero(t, dynamicCNServicePIDs[0])
}
