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

package api

import (
	"bytes"
	"errors"
	"testing"
)

func TestReadAllBoundedBoundsRetainedCapacity(t *testing.T) {
	data, err := ReadAllBounded(bytes.NewReader(bytes.Repeat([]byte{'x'}, 513)), 600)
	if err != nil {
		t.Fatalf("read bounded: %v", err)
	}
	if len(data) != 513 || cap(data) > 600 {
		t.Fatalf("unexpected bounded result len=%d cap=%d", len(data), cap(data))
	}

	_, err = ReadAllBounded(bytes.NewReader(bytes.Repeat([]byte{'x'}, 601)), 600)
	if !errors.Is(err, ErrMaterializationLimitExceeded) {
		t.Fatalf("expected materialization limit, got %v", err)
	}
}

func TestBoundedReadBufferRejectsBackingArrayGrowthPeak(t *testing.T) {
	buffer := boundedReadBuffer{maxBytes: 900}
	if _, err := buffer.Write(bytes.Repeat([]byte{'x'}, 500)); err != nil {
		t.Fatalf("seed bounded buffer: %v", err)
	}
	// The final 900-byte capacity fits by itself, but the old 512-byte array and
	// new 900-byte array would coexist and violate the 900-byte peak allowance.
	if _, err := buffer.Write(bytes.Repeat([]byte{'y'}, 20)); !errors.Is(err, ErrMaterializationLimitExceeded) {
		t.Fatalf("expected growth-peak rejection, got %v", err)
	}
}
