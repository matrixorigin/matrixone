// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package incrservice

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestAutoIDCacheOptInConfig(t *testing.T) {
	var cfg Config
	require.False(t, cfg.EnableAutoIDCache)
	_, err := cfg.forTable(t.Context(), 0)
	require.NoError(t, err)
	_, err = cfg.forTable(t.Context(), 1)
	require.ErrorContains(t, err, "AUTO_ID_CACHE is disabled")
	_, err = toml.Decode("enable-auto-id-cache=true", &cfg)
	require.NoError(t, err)
	require.True(t, cfg.EnableAutoIDCache)
	effective, err := cfg.forTable(t.Context(), 1)
	require.NoError(t, err)
	require.True(t, effective.demandOnly)
	require.Equal(t, 1, effective.CountPerAllocate)
}

func TestAutoIDCacheDisabledHasNoAllocatorSideEffects(t *testing.T) {
	runtime.RunTest(t.Name(), func(runtime.Runtime) {
		store := &autoIDCacheStore{IncrValueStore: NewMemStore()}
		s := NewIncrService(t.Name(), store, Config{}).(*service)
		defer s.Close()
		cols := []AutoColumn{{TableID: 42, ColName: "id", Step: 1, CacheSize: 1}}
		// Nil txn is intentional: reject before callbacks, metadata writes or cache construction.
		require.ErrorContains(t, s.Create(t.Context(), 42, cols, nil), "AUTO_ID_CACHE is disabled")
		got, err := store.GetColumns(t.Context(), 42, nil)
		require.NoError(t, err)
		require.Empty(t, got)
		// An existing table must fail closed on a disabled CN, not revert to its CN default.
		require.NoError(t, store.Create(t.Context(), 42, cols, nil))
		_, err = s.InsertValues(t.Context(), 42, 0, nil, nil, 1, 0)
		require.ErrorContains(t, err, "AUTO_ID_CACHE is disabled")
		_, err = s.CurrentValue(t.Context(), 42, "id")
		require.ErrorContains(t, err, "AUTO_ID_CACHE is disabled")
		require.Empty(t, store.requests())
		got, err = store.GetColumns(t.Context(), 42, nil)
		require.NoError(t, err)
		require.Equal(t, cols, got)
	})
}

func TestAutoIDCacheDDLAndProtocolGate(t *testing.T) {
	runtime.RunTest(t.Name(), func(rt runtime.Runtime) {
		oldService, _ := rt.GetGlobalVariables(runtime.AutoIncrementService)
		oldVersion, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
		defer rt.SetGlobalVariables(runtime.AutoIncrementService, oldService)
		defer rt.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion)
		require.NoError(t, CheckAutoIDCache(t.Context(), t.Name(), 0))
		require.ErrorContains(t, CheckAutoIDCache(t.Context(), t.Name(), 1), "AUTO_ID_CACHE is disabled")
		require.ErrorContains(t, CheckAutoIDCache(t.Context(), t.Name(), MaxAutoIDCache+1), "between 0")
		disabled := NewIncrService(t.Name(), NewMemStore(), Config{})
		defer disabled.Close()
		rt.SetGlobalVariables(runtime.AutoIncrementService, disabled)
		require.ErrorContains(t, CheckAutoIDCache(t.Context(), t.Name(), 1), "AUTO_ID_CACHE is disabled")
		enabled := NewIncrService(t.Name(), NewMemStore(), Config{EnableAutoIDCache: true})
		defer enabled.Close()
		rt.SetGlobalVariables(runtime.AutoIncrementService, enabled)
		for _, version := range []any{nil, "59", defines.MORPCVersion57, defines.MORPCVersion58} {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
			require.ErrorContains(t, CheckAutoIDCache(t.Context(), t.Name(), 1), "version 59")
			// The service path is independently fenced even if the caller bypasses DDL planning.
			require.ErrorContains(t, enabled.Create(t.Context(), 42, []AutoColumn{{ColName: "id", Step: 1, CacheSize: 1}}, nil), "version 59")
			require.NoError(t, CheckAutoIDCache(t.Context(), t.Name(), 0))
		}
		rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion59)
		require.NoError(t, CheckAutoIDCache(t.Context(), t.Name(), 1))
	})
}
