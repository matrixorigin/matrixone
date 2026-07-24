// Copyright 2021 - 2026 Matrix Origin
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

package main

import "testing"

func TestShouldStartBuiltinCNProxy(t *testing.T) {
	tests := []struct {
		name                string
		upstreamCount       int
		proxyServiceEnabled bool
		want                bool
	}{
		{
			name:          "multiple CNs without proxy service",
			upstreamCount: 2,
			want:          true,
		},
		{
			name:                "real proxy service owns the SQL entrypoint",
			upstreamCount:       2,
			proxyServiceEnabled: true,
			want:                false,
		},
		{
			name:          "single CN needs no builtin proxy",
			upstreamCount: 1,
			want:          false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := shouldStartBuiltinCNProxy(test.upstreamCount, test.proxyServiceEnabled)
			if got != test.want {
				t.Fatalf("shouldStartBuiltinCNProxy(%d, %t) = %t, want %t",
					test.upstreamCount, test.proxyServiceEnabled, got, test.want)
			}
		})
	}
}
