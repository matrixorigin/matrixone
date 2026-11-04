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

package runtime

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestLoadDataTestConfigScopeAndRestore(t *testing.T) {
	sid := t.Name()
	rt := NewRuntime(metadata.ServiceType_CN, sid, nil)
	SetupServiceBasedRuntime(sid, rt)
	defer CompareAndDeleteServiceBasedRuntime(sid, rt)
	_, ok := GetLoadDataTestConfig(sid, "input.csv")
	require.False(t, ok, "normal runtimes keep the production threshold")
	_, ok = GetLoadDataTestConfig(sid+"-absent", "input.csv")
	require.False(t, ok)
	_, ok = GetLoadDataTestConfig("", "input.csv")
	require.False(t, ok)
	SetupServiceRuntimeTestingContext(sid)
	tc := MustGetTestingContext(sid)
	_, ok = GetLoadDataTestConfig(sid, "input.csv")
	require.False(t, ok, "an unconfigured testing runtime is inert")
	for _, minSize := range []int64{0, -1} {
		tc.SetLoadDataTestConfig(LoadDataTestConfig{Filepath: "input.csv", MinSize: minSize})
		_, ok = GetLoadDataTestConfig(sid, "input.csv")
		require.False(t, ok)
	}
	tc.SetLoadDataTestConfig(LoadDataTestConfig{})
	var called bool
	previous := tc.SetLoadDataTestConfig(LoadDataTestConfig{
		Filepath: "input.csv", MinSize: 10,
		OnRanges: func(_ int, _ []int64) {
			// The returned callback is invoked after releasing the context lock.
			_, called = GetLoadDataTestConfig(sid, "input.csv")
		},
	})
	defer tc.SetLoadDataTestConfig(previous)
	_, ok = GetLoadDataTestConfig(sid, "other.csv")
	require.False(t, ok)
	cfg, ok := GetLoadDataTestConfig(sid, "input.csv")
	require.True(t, ok)
	cfg.OnRanges(2, []int64{0, 10, 10, -1})
	require.True(t, called)
	tc.SetLoadDataTestConfig(previous)
	_, ok = GetLoadDataTestConfig(sid, "input.csv")
	require.False(t, ok)
}

func TestLoadDataTestConfigConcurrentAccess(t *testing.T) {
	sid := t.Name()
	rt := NewRuntime(metadata.ServiceType_CN, sid, nil)
	SetupServiceBasedRuntime(sid, rt)
	defer CompareAndDeleteServiceBasedRuntime(sid, rt)
	SetupServiceRuntimeTestingContext(sid)
	tc := MustGetTestingContext(sid)
	var calls atomic.Int32
	cfg := LoadDataTestConfig{Filepath: "input.csv", MinSize: 10, OnRanges: func(_ int, _ []int64) {
		calls.Add(1)
	}}
	tc.SetLoadDataTestConfig(cfg)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 256 {
			tc.SetLoadDataTestConfig(cfg)
		}
	}()
	go func() {
		defer wg.Done()
		for range 256 {
			if current, ok := GetLoadDataTestConfig(sid, "input.csv"); ok {
				current.OnRanges(2, nil)
			}
		}
	}()
	wg.Wait()
	require.EqualValues(t, 256, calls.Load())
}
