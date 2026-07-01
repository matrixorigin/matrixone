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

package icebergdelete

import (
	"context"
	"strings"
	"testing"
)

func TestPositionDeleteMaskUsesGlobalRowOrdinal(t *testing.T) {
	state := NewApplyState(Options{MaxMemoryBytes: 1024})
	state.Position.Add("data.parquet", 12)
	state.Position.Add("data.parquet", 14)

	keep, err := state.ApplyPositionMask(context.Background(), "data.parquet", 10, 5)
	if err != nil {
		t.Fatalf("apply position mask: %v", err)
	}
	want := []bool{true, true, false, true, false}
	for i := range want {
		if keep[i] != want[i] {
			t.Fatalf("row %d keep=%v want %v; mask=%v", i, keep[i], want[i], keep)
		}
	}
	if state.Profile.PositionRowsFiltered != 2 {
		t.Fatalf("unexpected profile: %+v", state.Profile)
	}
	if got := RowOrdinals(10, 3); len(got) != 3 || got[0] != 10 || got[2] != 12 {
		t.Fatalf("unexpected row ordinals: %v", got)
	}
}

func TestEqualityDeleteMaskAndMemoryLimit(t *testing.T) {
	state := NewApplyState(Options{MaxMemoryBytes: 4096})
	state.Equality.AddKey(int32(1), []byte("alice"))
	state.Equality.AddKey(int64(2), "bob")
	keep, err := state.ApplyEqualityMask(context.Background(), [][]any{
		{int64(1), "alice"},
		{int64(3), "carol"},
		{int64(2), "bob"},
	})
	if err != nil {
		t.Fatalf("apply equality mask: %v", err)
	}
	want := []bool{false, true, false}
	for i := range want {
		if keep[i] != want[i] {
			t.Fatalf("row %d keep=%v want %v; mask=%v", i, keep[i], want[i], keep)
		}
	}
	if state.Profile.EqualityRowsFiltered != 2 {
		t.Fatalf("unexpected profile: %+v", state.Profile)
	}

	limited := NewApplyState(Options{MaxMemoryBytes: 1})
	limited.Equality.AddKey(int64(1), "alice")
	_, err = limited.ApplyEqualityMask(context.Background(), [][]any{{int64(1), "alice"}})
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_PLANNING_LIMIT_EXCEEDED") {
		t.Fatalf("expected memory limit error, got %v", err)
	}
}

func TestPositionDeleteMemoryLimit(t *testing.T) {
	state := NewApplyState(Options{MaxMemoryBytes: 1})
	state.Position.Add("data.parquet", 12)
	_, err := state.ApplyPositionMask(context.Background(), "data.parquet", 10, 5)
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_PLANNING_LIMIT_EXCEEDED") {
		t.Fatalf("expected position memory limit error, got %v", err)
	}
}

func TestDeleteApplySpillEnabledFailsFastUntilImplemented(t *testing.T) {
	state := NewApplyState(Options{MaxMemoryBytes: 1, SpillEnabled: true})
	state.Equality.AddKey(int64(1), "alice")
	_, err := state.ApplyEqualityMask(context.Background(), [][]any{{int64(1), "alice"}})
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_UNSUPPORTED_FEATURE") {
		t.Fatalf("expected unsupported spill error, got %v", err)
	}
	if !state.Profile.SpillEnabled {
		t.Fatalf("expected profile to record spill intent: %+v", state.Profile)
	}
}
