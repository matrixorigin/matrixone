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

package util

import (
	"testing"
)

const timeFormat = "2006-01-02 15:04:05"

func TestNowNS(t *testing.T) {
	tests := []struct {
		name string
		want TimeNano
	}{
		{
			name: "normal",
			want: NowNS(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NowNS(); got <= tt.want {
				t.Errorf("NowNS() = %v, want %v", got, tt.want)
			}
		})
	}
}
