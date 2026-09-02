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
