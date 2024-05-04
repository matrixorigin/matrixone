// Copyright 2022 Matrix Origin
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

package malloc

import "testing"

func TestAllocFree(t *testing.T) {
	// Test that Alloc and Free work.
	for i := 0; i < 100; i++ {
		b := Alloc(1024)
		if len(b) != 1024 {
			t.Errorf("Alloc returned slice of length %d, want 1024", len(b))
		}
		Free(b)
	}
}

func TestAllocZero(t *testing.T) {
	// Test that Alloc(0) returns a non-nil slice.
	b := Alloc(0)
	if b == nil {
		t.Errorf("Alloc(0) returned nil slice")
	}
	Free(b)
}
