// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAutomaticCacheLimits(t *testing.T) {
	for _, tc := range []struct {
		name          string
		total, cgroup uint64
		want          int64
	}{
		{"host", 8 << 30, 0, 2 << 30}, {"container", 8 << 30, 2 << 30, 512 << 20},
		{"unlimited-cgroup", 8 << 30, ^uint64(0), 2 << 30},
		{"container-only", 0, 2 << 30, 512 << 20}, {"unknown", 0, 0, fallbackCacheBytes},
		{"tiny", 1, 0, 1}, {"overflow", ^uint64(0), 0, absoluteHostCacheCeiling},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, automaticHostLimit(tc.total, tc.cgroup)) })
	}
	for _, tc := range []struct {
		name             string
		count            int
		countErr, memErr error
		memory           uint64
		want             int64
	}{
		{"two-devices", 2, nil, nil, 8 << 30, 8 << 30},
		{"no-device", 0, nil, nil, 0, fallbackCacheBytes},
		{"count-error", 1, errors.New("count"), nil, 0, fallbackCacheBytes},
		{"memory-error", 1, nil, errors.New("memory"), 0, fallbackCacheBytes},
		{"unknown-memory", 1, nil, nil, 0, fallbackCacheBytes},
		{"overflow", 2, nil, nil, ^uint64(0), absoluteDeviceCacheCeiling},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := automaticDeviceCapacity(func() (int, error) { return tc.count, tc.countErr },
				func(int) (uint64, error) { return tc.memory, tc.memErr })
			require.Equal(t, tc.want, got)
		})
	}
}

func TestTenantOverrideCannotBypassAutomaticCNLimit(t *testing.T) {
	c := newBoundCache(t)
	defaults := c.defaultLimits()
	sp := govProc(t, c, 1, caps{host: defaults.host * 2, device: defaults.device * 2}, caps{})
	tenant, sys := c.limits(sp)
	require.Equal(t, defaults, sys)
	require.Greater(t, tenant.host, sys.host)
	require.Greater(t, tenant.device, sys.device)
}
