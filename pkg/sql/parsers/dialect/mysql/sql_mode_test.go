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

package mysql

import "testing"

func TestHasMatrixOneNativeSQLMode(t *testing.T) {
	tests := []struct {
		name string
		mode string
		want bool
	}{
		{
			name: "empty",
			mode: "",
			want: false,
		},
		{
			name: "exact token",
			mode: "MATRIXONE_NATIVE",
			want: true,
		},
		{
			name: "case insensitive with spaces",
			mode: " ansi_quotes , matrixone_native ",
			want: true,
		},
		{
			name: "suffix does not match",
			mode: "MATRIXONE_NATIVE_EXTRA",
			want: false,
		},
		{
			name: "substring does not match",
			mode: "NO_MATRIXONE_NATIVE",
			want: false,
		},
		{
			name: "other tokens only",
			mode: "ANSI_QUOTES,PIPES_AS_CONCAT",
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := HasMatrixOneNativeSQLMode(test.mode); got != test.want {
				t.Fatalf("HasMatrixOneNativeSQLMode(%q) = %v, want %v", test.mode, got, test.want)
			}
		})
	}
}
