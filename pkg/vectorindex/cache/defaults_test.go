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

package cache

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// The budget is a share of what the machine has -- and when that cannot be read, an ERROR
// rather than an invented number. A guessed budget is refused-or-over-committed silently;
// naming the missing input lets the operator set the variable.
func TestAutomaticHostLimit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		total, cgroup uint64
		want          int64
		wantErr       bool
	}{
		{name: "host", total: 8 << 30, want: (8 << 30) / 100 * 90},
		{name: "container", total: 8 << 30, cgroup: 2 << 30, want: (2 << 30) / 100 * 90},
		{name: "unlimited-cgroup", total: 8 << 30, cgroup: ^uint64(0), want: (8 << 30) / 100 * 90},
		{name: "container-only", cgroup: 2 << 30, want: (2 << 30) / 100 * 90},
		{name: "tiny", total: 1, want: 1},
		{name: "invalid-capacity-sentinel", total: ^uint64(0), wantErr: true},
		{name: "unknown", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := automaticHostLimit(tc.total, tc.cgroup)
			if tc.wantErr {
				require.Error(t, err, "an unreadable machine is reported, never guessed")
				require.Contains(t, err.Error(), "max_index_cache_size")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestAutomaticDeviceCapacity(t *testing.T) {
	for _, tc := range []struct {
		name             string
		count            int
		countErr, memErr error
		memory           uint64
		want             int64
		wantErr          bool
	}{
		{name: "two-devices", count: 2, memory: 8 << 30, want: 2 * ((8 << 30) / 100 * 90)},
		{name: "overflow", count: 2, memory: ^uint64(0), want: maxRepresentableBudget},
		// No GPU is not a failure: the arena does not apply, so it gets no budget.
		{name: "no-device", count: 0, want: 0},
		{name: "count-error", count: 1, countErr: errors.New("count"), wantErr: true},
		{name: "memory-error", count: 1, memErr: errors.New("memory"), wantErr: true},
		{name: "unknown-memory", count: 1, memory: 0, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := automaticDeviceCapacity(
				func() (int, error) { return tc.count, tc.countErr },
				func(int) (uint64, error) { return tc.memory, tc.memErr })
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "max_gpu_index_cache_size")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// A tenant cannot raise its own ceiling past the CN-wide one: the CN budget is resolved from
// the SYS value alone, and enforce applies both.
func TestTenantOverrideCannotBypassAutomaticCNLimit(t *testing.T) {
	c := newBoundCache(t)
	defaults, err, err2 := c.defaultLimits()
	require.NoError(t, err2)
	require.NoError(t, err)

	sp := govProc(t, c, 1, caps{host: defaults.host * 2, device: defaults.device * 2}, caps{})
	tenant, sys, serrs := c.limits(sp)
	require.NoError(t, serrs.host)
	require.NoError(t, serrs.device)
	require.Equal(t, defaults, sys, "the CN budget ignores what the tenant asked for")
	require.Greater(t, tenant.host, sys.host)
}
