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

package function

import (
	"reflect"
	"testing"
)

func TestParseDatalink(t *testing.T) {

	type testCase struct {
		name          string
		data          string
		wantMoUrl     string
		wantUrlParams []int
	}
	tests := []testCase{
		{
			name:          "Test1 - File",
			data:          "file:///a/b/c/1.txt",
			wantMoUrl:     "/a/b/c/1.txt",
			wantUrlParams: []int{0, -1},
		},
		{
			name:          "Test2 - File",
			data:          "file:///a/b/c/1.txt?offset=1&size=2",
			wantMoUrl:     "/a/b/c/1.txt",
			wantUrlParams: []int{1, 2},
		},
		{
			name:          "Test3 - File",
			data:          "file:///a/b/c/1.txt?offset=1",
			wantMoUrl:     "/a/b/c/1.txt",
			wantUrlParams: []int{1, -1},
		},
		{
			name:          "Test4 - File",
			data:          "file:///a/b/c/1.txt?size=2",
			wantMoUrl:     "/a/b/c/1.txt",
			wantUrlParams: []int{0, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, got2, err := ParseDatalink(tt.data, nil)
			if err != nil {
				t.Errorf("ParseDatalink() error = %v", err)
			}
			if !reflect.DeepEqual(got1, tt.wantMoUrl) {
				t.Errorf("ParseDatalink() = %v, want %v", got1, tt.wantMoUrl)
			}
			if !reflect.DeepEqual(got2, tt.wantUrlParams) {
				t.Errorf("ParseDatalink() = %v, want %v", got2, tt.wantUrlParams)
			}
		})
	}
}
