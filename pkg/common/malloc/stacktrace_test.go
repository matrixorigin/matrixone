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

package malloc

import (
	"strings"
	"testing"
)

func TestGetStacktrace(t *testing.T) {
	ids := make([]Stacktrace, 0, 1024)
	for range 1024 {
		ids = append(ids, GetStacktrace(0))
	}
	for _, id := range ids {
		msg := id.String()
		if !strings.Contains(msg, "TestGetStacktrace") {
			t.Fatalf("got %v\n", msg)
		}
	}
}

func BenchmarkGetStacktrace(b *testing.B) {
	for range b.N {
		GetStacktrace(0)
	}
}

func FuzzGetStacktrace(f *testing.F) {
	var exec func(i uint)
	exec = func(i uint) {
		if i > 0 {
			exec(i - 1)
			return
		}
		trace := GetStacktrace(0)
		_ = trace.String()
	}
	f.Fuzz(func(t *testing.T, i uint) {
		exec(i % 65536)
	})
}
