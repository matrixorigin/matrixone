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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServiceSupervisorStopsRolesInDependencyOrder(t *testing.T) {
	s := newServiceSupervisor()
	var mu sync.Mutex
	var stopped []serviceRole
	roles := []serviceRole{
		serviceRoleProxy,
		serviceRolePython,
		serviceRoleCN,
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
