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

func TestEqualityDeleteLayoutsAreIndependent(t *testing.T) {
	state := NewApplyState(Options{MaxMemoryBytes: 4096})
	state.AddEqualityKey("1", int64(10))
	state.AddEqualityKey("1,2", int64(20), "ksa")

	keepSingle, err := state.ApplyEqualityMaskForLayout(context.Background(), "1", [][]any{
		{int64(10)},
		{int64(20)},
	})
	if err != nil {
		t.Fatalf("apply single-field equality layout: %v", err)
	}
	if keepSingle[0] || !keepSingle[1] {
		t.Fatalf("single-field layout should delete only id=10, got %v", keepSingle)
	}

	keepComposite, err := state.ApplyEqualityMaskForLayout(context.Background(), "1,2", [][]any{
		{int64(20), "ksa"},
		{int64(20), "uae"},
		{int64(10), "ksa"},
	})
	if err != nil {
		t.Fatalf("apply composite equality layout: %v", err)
	}
	want := []bool{false, true, true}
	for idx := range want {
		if keepComposite[idx] != want[idx] {
			t.Fatalf("composite row %d keep=%v want=%v mask=%v", idx, keepComposite[idx], want[idx], keepComposite)
		}
	}
	if state.Profile.EqualityRowsFiltered != 2 {
		t.Fatalf("unexpected equality profile: %+v", state.Profile)
	}
}

func TestEqualityDeleteCompositeKeyEncodingDoesNotCollide(t *testing.T) {
	idx := NewEqualityIndex()
	idx.AddKey("a\x00s:b")
	if idx.ShouldDelete("a", "b") {
		t.Fatal("single string containing the old separator collided with a two-column tuple")
	}
	if !idx.ShouldDelete("a\x00s:b") {
		t.Fatal("the exact equality-delete tuple was not found")
	}
}

func TestDeleteApplyMemoryLimitCoversPathsLayoutsAndAggregateBudget(t *testing.T) {
	position := NewApplyState(Options{MaxMemoryBytes: 80})
	position.Position.Add(strings.Repeat("p", 64), 1)
	if err := position.CheckMemory(context.Background()); err == nil {
		t.Fatal("position delete path memory must count toward the limit")
	}

	layout := NewApplyState(Options{MaxMemoryBytes: 80})
	layout.AddEqualityKey(strings.Repeat("l", 64), int64(1))
	if err := layout.CheckMemory(context.Background()); err == nil {
		t.Fatal("equality layout key memory must count toward the limit")
	}

	left := NewApplyState(Options{MaxMemoryBytes: 256})
	right := NewApplyState(Options{MaxMemoryBytes: 256})
	left.Equality.AddKey("left")
	right.Equality.AddKey("right")
	if err := left.CheckMemory(context.Background()); err != nil {
		t.Fatalf("left state should fit independently: %v", err)
	}
	if err := right.CheckMemory(context.Background()); err != nil {
		t.Fatalf("right state should fit independently: %v", err)
	}
	aggregate := left.MemoryBytes() + right.MemoryBytes()
	if err := CheckMemoryLimit(context.Background(), Options{MaxMemoryBytes: aggregate - 1}, aggregate); err == nil {
		t.Fatal("aggregate delete state memory must be checked against one operator budget")
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
