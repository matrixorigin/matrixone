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

package common

import (
	"math"
	"testing"
)

func TestConvertUint64BytesToHumanReadable(t *testing.T) {
	tests := []struct {
		bytes uint64
		want  string
	}{
		{0, "0 bytes"},
		{1023, "1023 bytes"},
		{1024, "1.00 KiB"},
		{128 * MiB, "128.00 MiB"},
		{EiB - 1, "1024.00 PiB"},
		{EiB, "1.00 EiB"},
		{math.MaxUint64, "16.00 EiB"},
	}
	for _, test := range tests {
		if got := ConvertUint64BytesToHumanReadable(test.bytes); got != test.want {
			t.Fatalf("ConvertUint64BytesToHumanReadable(%d) = %q, want %q", test.bytes, got, test.want)
		}
	}
}
