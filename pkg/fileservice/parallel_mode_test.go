// Copyright 2025 Matrix Origin
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

package fileservice

import (
	"context"
	"testing"
)

func TestParseParallelModeVariants(t *testing.T) {
	cases := []struct {
		in     string
		expect ParallelMode
		ok     bool
	}{
		{"", ParallelOff, true},
		{"off", ParallelOff, true},
		{"false", ParallelOff, true},
		{"0", ParallelOff, true},
		{"auto", ParallelAuto, true},
		{"force", ParallelForce, true},
		{"on", ParallelForce, true},
		{"1", ParallelForce, true},
		{"xxx", ParallelOff, false},
	}

	for _, c := range cases {
		mode, ok := parseParallelMode(c.in)
		if mode != c.expect || ok != c.ok {
			t.Fatalf("input %s got (%v,%v) expect (%v,%v)", c.in, mode, ok, c.expect, c.ok)
		}
	}
}

func TestWithParallelModeOnContext(t *testing.T) {
	base := context.Background()
	ctx := WithParallelMode(base, ParallelForce)
	mode, ok := parallelModeFromContext(ctx)
	if !ok || mode != ParallelForce {
		t.Fatalf("expected force override, got %v %v", mode, ok)
	}
	if _, ok := parallelModeFromContext(base); ok {
		t.Fatalf("unexpected mode on base context")
	}
}

func TestETLParallelModePriority(t *testing.T) {
	t.Setenv("MO_ETL_PARALLEL_MODE", "off")
	if m := etlParallelMode(context.Background()); m != ParallelOff {
		t.Fatalf("expect off from env, got %v", m)
	}

	ctx := WithParallelMode(context.Background(), ParallelForce)
	if m := etlParallelMode(ctx); m != ParallelForce {
		t.Fatalf("ctx override should win, got %v", m)
	}

	t.Setenv("MO_ETL_PARALLEL_MODE", "auto")
	if m := etlParallelMode(context.Background()); m != ParallelAuto {
		t.Fatalf("env auto not applied, got %v", m)
	}
}
