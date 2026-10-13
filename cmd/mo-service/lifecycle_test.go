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
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/stretchr/testify/require"
)

func TestServiceSupervisorStopsRolesInDependencyOrder(t *testing.T) {
	s := newServiceSupervisor()
	var mu sync.Mutex
	var stopped []serviceRole
	roles := []serviceRole{
		serviceRoleProxy,
		serviceRoleCN,
		serviceRolePython,
		serviceRoleTN,
		serviceRoleLog,
	}
	for _, role := range roles {
		finish := s.registerTask(role)
		go func(role serviceRole, finish func(error)) {
			ctx, cancel := s.roleContext(context.Background(), role)
			defer cancel()
			<-ctx.Done()
			mu.Lock()
			stopped = append(stopped, role)
			mu.Unlock()
			finish(nil)
		}(role, finish)
	}

	require.NoError(t, s.shutdown(context.Background()))
	require.Equal(t, roles, stopped)
}

func TestServiceSupervisorKeepsPythonProviderUntilCNDrains(t *testing.T) {
	s := newServiceSupervisor()
	var mu sync.Mutex
	var events []string
	record := func(event string) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	}

	cnRelease := make(chan struct{})
	cnStarted := make(chan struct{})
	finishCN := s.registerTask(serviceRoleCN)
	go func() {
		ctx, cancel := s.roleContext(context.Background(), serviceRoleCN)
		defer cancel()
		<-ctx.Done()
		record("cn-stop")
		close(cnStarted)
		<-cnRelease
		record("cn-finish")
		finishCN(nil)
	}()

	finishPython := s.registerTask(serviceRolePython)
	go func() {
		ctx, cancel := s.roleContext(context.Background(), serviceRolePython)
		defer cancel()
		<-ctx.Done()
		record("python-stop")
		finishPython(nil)
	}()

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- s.shutdown(context.Background()) }()
	select {
	case <-cnStarted:
	case <-time.After(time.Second):
		t.Fatal("CN phase did not start")
	}
	mu.Lock()
	eventsAtCN := append([]string(nil), events...)
	mu.Unlock()
	require.NotContains(t, eventsAtCN, "python-stop")

	close(cnRelease)
	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after CN drained")
	}
	mu.Lock()
	defer mu.Unlock()
	cnFinish := -1
	pythonStop := -1
	for i, event := range events {
		switch event {
		case "cn-finish":
			cnFinish = i
		case "python-stop":
			pythonStop = i
		}
	}
	require.GreaterOrEqual(t, cnFinish, 0)
	require.Greater(t, pythonStop, cnFinish)
}

func TestServiceSupervisorRoleTimeoutDoesNotAdvanceDependency(t *testing.T) {
	s := newServiceSupervisor()
	finish := s.registerTask(serviceRoleCN)
	defer finish(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, s.stopRole(ctx, serviceRoleCN), context.DeadlineExceeded)
}

func TestServiceSupervisorRoleErrorDoesNotAdvanceDependency(t *testing.T) {
	s := newServiceSupervisor()
	finishCN := s.registerTask(serviceRoleCN)
	finishCN(errors.New("cn close failed"))
	finishTN := s.registerTask(serviceRoleTN)
	defer finishTN(nil)

	err := s.shutdown(context.Background())
	require.ErrorContains(t, err, "cn close failed")
	select {
	case <-s.roles[serviceRoleTN].stopC:
		require.FailNow(t, "dependent TN role was stopped after CN failure")
	default:
	}
}

func TestServiceSupervisorConcurrentShutdownRunsOnce(t *testing.T) {
	s := newServiceSupervisor()
	finish := s.registerTask(serviceRoleCN)
	go func() {
		ctx, cancel := s.roleContext(context.Background(), serviceRoleCN)
		defer cancel()
		<-ctx.Done()
		finish(nil)
	}()

	const callers = 2
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.shutdown(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestServiceSupervisorNilAndRoleHelpers(t *testing.T) {
	var s *serviceSupervisor
	s.registerTask(serviceRoleCN)(errors.New("ignored"))
	ctx, cancel := s.roleContext(context.Background(), serviceRoleCN)
	cancel()
	require.NoError(t, s.shutdown(context.Background()))
	s.setDynamicCNStop(func(context.Context) error { return errors.New("ignored") })
	require.Equal(t, "unknown", serviceRole(serviceRoleCount).String())
	require.NotNil(t, ctx)

	s = newServiceSupervisor()
	finish := s.registerTask(serviceRoleCN)
	finish(errors.New("first"))
	finish(errors.New("second"))
	err := s.stopRole(context.Background(), serviceRoleCN)
	require.ErrorContains(t, err, "first")
	require.NotContains(t, err.Error(), "second")
}

func TestServiceSupervisorRoleContextFollowsParent(t *testing.T) {
	s := newServiceSupervisor()
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := s.roleContext(parent, serviceRoleCN)
	cancelParent()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("role context did not follow parent cancellation")
	}
	cancel()
}

func TestServiceSupervisorProxyFailureStopsBeforeRoles(t *testing.T) {
	oldProxy := cnProxy
	t.Cleanup(func() { cnProxy = oldProxy })
	proxyErr := errors.New("proxy close failed")
	cnProxy = &testProxy{stopErr: proxyErr}
	s := newServiceSupervisor()
	finish := s.registerTask(serviceRoleProxy)
	defer finish(nil)
	require.ErrorIs(t, s.shutdown(context.Background()), proxyErr)
	select {
	case <-s.roles[serviceRoleProxy].stopC:
		t.Fatal("role shutdown advanced after builtin proxy failure")
	default:
	}
}

func TestServiceSupervisorConfiguredProxyFailureStopsBeforeCN(t *testing.T) {
	oldProxy := cnProxy
	cnProxy = nil
	t.Cleanup(func() { cnProxy = oldProxy })

	s := newServiceSupervisor()
	proxyErr := errors.New("configured proxy close failed")
	proxyServer := &testProxyServerLifecycle{closeErr: proxyErr}
	finishProxy := s.registerTask(serviceRoleProxy)
	go func() {
		ctx, cancel := s.roleContext(context.Background(), serviceRoleProxy)
		defer cancel()
		finishProxy(runProxyServerUntilCanceled(ctx, proxyServer))
	}()
	finishCN := s.registerTask(serviceRoleCN)
	defer finishCN(nil)

	require.ErrorIs(t, s.shutdown(context.Background()), proxyErr)
	require.Equal(t, 1, proxyServer.closeCalls)
	select {
	case <-s.roles[serviceRoleCN].stopC:
		t.Fatal("CN phase opened after configured proxy close failed")
	default:
	}
}

func TestConfiguredProxyStartupFailureWakesShutdown(t *testing.T) {
	oldLifecycle := serviceLifecycle
	oldProfileInterval := *profileInterval
	t.Cleanup(func() {
		serviceLifecycle = oldLifecycle
		*profileInterval = oldProfileInterval
	})

	s := newServiceSupervisor()
	serviceLifecycle = s
	*profileInterval = 0
	startupErr := errors.New("configured proxy file service failed")
	finishProxy := s.registerTask(serviceRoleProxy)
	finishProxy(startupErr)
	s.notifyFatal(startupErr)

	mainStopper := stopper.NewStopper("test-main")
	defer mainStopper.Stop()
	shutdownC := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- waitSignalToStop(mainStopper, shutdownC) }()

	select {
	case err := <-done:
		require.ErrorIs(t, err, startupErr)
	case <-time.After(time.Second):
		t.Fatal("configured proxy startup failure did not wake shutdown")
	}
}

func TestServiceSupervisorFatalCleanupReapsDynamicCN(t *testing.T) {
	oldProxy := cnProxy
	cnProxy = nil
	t.Cleanup(func() { cnProxy = oldProxy })

	s := newServiceSupervisor()
	cleanupCalls := 0
	s.setDynamicCNStop(func(context.Context) error {
		cleanupCalls++
		return nil
	})
	finishProxy := s.registerTask(serviceRoleProxy)
	finishProxy(errors.New("proxy startup failed"))

	require.Error(t, s.shutdownAfterFatal(context.Background()))
	require.Equal(t, 1, cleanupCalls)
	require.Error(t, s.shutdownAfterFatal(context.Background()))
	require.Equal(t, 1, cleanupCalls)
}

func TestNormalShutdownReapsDynamicCNAfterProxyFailure(t *testing.T) {
	setLaunchTestHooks(t)
	oldProfileInterval := *profileInterval
	t.Cleanup(func() { *profileInterval = oldProfileInterval })
	*profileInterval = 0
	launchSleep = func(time.Duration) {}

	for _, test := range []struct {
		name         string
		shutdownC    func() chan struct{}
		notifySignal bool
	}{
		{name: "signal", shutdownC: func() chan struct{} { return make(chan struct{}) }, notifySignal: true},
		{name: "hakeeper", shutdownC: func() chan struct{} {
			shutdownC := make(chan struct{})
			close(shutdownC)
			return shutdownC
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			launchSignalNotify = func(ch chan<- os.Signal, _ ...os.Signal) {
				if test.notifySignal {
					ch <- syscall.SIGTERM
				}
			}
			launchSignalStop = func(chan<- os.Signal) {}

			proxyErr := errors.New("builtin proxy close failed")
			cnProxy = &testProxy{stopErr: proxyErr}
			s := newServiceSupervisor()
			serviceLifecycle = s
			child := newRealDynamicChild(t, "/bin/sleep", []string{"sleep", "60"})
			dynamicCNMu.Lock()
			dynamicCNStopping = false
			dynamicCNServicePIDs = []int{child.pid}
			dynamicCNServiceProcesses = []*dynamicCNChild{child}
			dynamicCNMu.Unlock()
			s.setDynamicCNStop(stopAllDynamicCNServicesGracefully)

			mainStopper := stopper.NewStopper("test-main")
			defer mainStopper.Stop()
			done := make(chan error, 1)
			go func() { done <- waitSignalToStop(mainStopper, test.shutdownC()) }()

			select {
			case err := <-done:
				require.ErrorIs(t, err, proxyErr)
			case <-time.After(time.Second):
				t.Fatal("normal shutdown did not finish")
			}
			require.Error(t, child.process.signal(syscall.Signal(0)))
			dynamicCNMu.RLock()
			require.Zero(t, dynamicCNServicePIDs[0])
			require.Nil(t, dynamicCNServiceProcesses[0])
			dynamicCNMu.RUnlock()
			select {
			case <-s.roles[serviceRoleCN].stopC:
				t.Fatal("CN phase opened after builtin proxy close failure")
			default:
			}
		})
	}
}

func TestServiceSupervisorDynamicCNFailureStopsBeforeTN(t *testing.T) {
	oldProxy := cnProxy
	cnProxy = nil
	t.Cleanup(func() { cnProxy = oldProxy })
	s := newServiceSupervisor()
	dynamicErr := errors.New("dynamic CN close failed")
	s.setDynamicCNStop(func(context.Context) error { return dynamicErr })
	finishTN := s.registerTask(serviceRoleTN)
	defer finishTN(nil)

	require.ErrorIs(t, s.shutdown(context.Background()), dynamicErr)
	select {
	case <-s.roles[serviceRoleTN].stopC:
		t.Fatal("TN was stopped after dynamic CN failed to close")
	default:
	}
}

func TestServiceSupervisorShutdownContextStopsBlockedRole(t *testing.T) {
	oldProxy := cnProxy
	cnProxy = nil
	t.Cleanup(func() { cnProxy = oldProxy })
	s := newServiceSupervisor()
	finish := s.registerTask(serviceRoleCN)
	defer finish(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, s.shutdown(ctx), context.DeadlineExceeded)
	select {
	case <-s.roles[serviceRoleTN].stopC:
		t.Fatal("shutdown advanced after CN timeout")
	default:
	}
}
