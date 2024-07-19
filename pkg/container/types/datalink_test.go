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

package types

import (
	"reflect"
	"testing"
)

func TestDataLinkExtract(t *testing.T) {

	type testCase struct {
		name          string
		data          string
		wantUrlParts  []string
		wantUrlParams map[string]string
	}
	tests := []testCase{
		{
			name:         "Test1 - HTTP",
			data:         "http://www.google.com?offset=0&size=10",
			wantUrlParts: []string{"http", "", "www.google.com", ""},
			wantUrlParams: map[string]string{
				"offset": "0",
				"size":   "10",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, got2, err := ParseDatalink(tt.data)
			if err != nil {
				t.Errorf("ParseDatalink() error = %v", err)
			}
			if !reflect.DeepEqual(got1, tt.wantUrlParts) {
				t.Errorf("ParseDatalink() = %v, want %v", got1, tt.wantUrlParts)
			}
			if !reflect.DeepEqual(got2, tt.wantUrlParams) {
				t.Errorf("ParseDatalink() = %v, want %v", got2, tt.wantUrlParams)
			}
		})
	}
}
