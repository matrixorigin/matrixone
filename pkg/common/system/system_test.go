// Copyright 2024 Matrix Origin
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

package system

import (
	"testing"
	"time"
)

func TestShouldRefreshQuotaConfig(t *testing.T) {
	// Reset state
	lastQuotaRefreshTime.Store(0)

	t.Run("first call should allow refresh", func(t *testing.T) {
		if !shouldRefreshQuotaConfig() {
			t.Error("first call should return true")
		}
	})

	t.Run("immediate second call should be debounced", func(t *testing.T) {
		// Simulate a refresh
		lastQuotaRefreshTime.Store(time.Now().Unix())

		// Immediate call should be blocked
		if shouldRefreshQuotaConfig() {
			t.Error("immediate second call should return false (debounced)")
		}
	})

	t.Run("call after debounce period should allow refresh", func(t *testing.T) {
		// Set last refresh to 2 seconds ago
		lastQuotaRefreshTime.Store(time.Now().Unix() - 2)

		if !shouldRefreshQuotaConfig() {
			t.Error("call after debounce period should return true")
		}
	})

	t.Run("call exactly at debounce boundary should allow refresh", func(t *testing.T) {
		// Set last refresh to exactly quotaRefreshDebounceSeconds ago
		lastQuotaRefreshTime.Store(time.Now().Unix() - quotaRefreshDebounceSeconds)

		if !shouldRefreshQuotaConfig() {
			t.Error("call at debounce boundary should return true")
		}
	})
}

func TestRefreshQuotaConfigDebounce(t *testing.T) {
	// Reset state
	lastQuotaRefreshTime.Store(0)

	t.Run("refreshQuotaConfig updates timestamp", func(t *testing.T) {
		before := time.Now().Unix()
		refreshQuotaConfig()
		after := lastQuotaRefreshTime.Load()

		if after < before {
			t.Errorf("timestamp not updated: got %d, want >= %d", after, before)
		}
	})

	t.Run("multiple rapid refreshes are debounced", func(t *testing.T) {
		lastQuotaRefreshTime.Store(0)

		// First refresh should succeed
		if !shouldRefreshQuotaConfig() {
			t.Fatal("first refresh should be allowed")
		}
		refreshQuotaConfig()

		// Rapid subsequent calls should be blocked
		blocked := 0
		for i := 0; i < 5; i++ {
			if !shouldRefreshQuotaConfig() {
				blocked++
			}
			time.Sleep(100 * time.Millisecond)
		}

		if blocked == 0 {
			t.Error("expected some calls to be debounced")
		}
	})
}

func TestDebounceSimulatesK8sScaling(t *testing.T) {
	// Simulate k8s vertical scaling scenario where kubelet updates
	// both cpu.max and memory.max in quick succession

	lastQuotaRefreshTime.Store(0)

	// First event (cpu.max changed)
	if !shouldRefreshQuotaConfig() {
		t.Fatal("first event should trigger refresh")
	}
	refreshQuotaConfig()
	firstRefreshTime := lastQuotaRefreshTime.Load()

	// Second event (memory.max changed) - happens immediately after
	time.Sleep(10 * time.Millisecond)
	if shouldRefreshQuotaConfig() {
		t.Error("second event should be debounced (< 1s)")
	}

	// Verify timestamp wasn't updated by the debounced call
	if lastQuotaRefreshTime.Load() != firstRefreshTime {
		t.Error("debounced call should not update timestamp")
	}

	// After debounce period, next event should trigger refresh
	time.Sleep(time.Duration(quotaRefreshDebounceSeconds) * time.Second)
	if !shouldRefreshQuotaConfig() {
		t.Error("event after debounce period should trigger refresh")
	}
}

func BenchmarkShouldRefreshQuotaConfig(b *testing.B) {
	lastQuotaRefreshTime.Store(time.Now().Unix() - 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		shouldRefreshQuotaConfig()
	}
}
