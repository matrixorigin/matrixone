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
	"time"
)

const timeFormat = "2006-01-02 15:04:05"

func TestNow(t *testing.T) {
	tests := []struct {
		name  string
		delta time.Duration
		want  time.Time
	}{
		{
			name:  "normal",
			delta: 100 * time.Nanosecond,
			want:  time.Now(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.want = tt.want.Add(tt.delta)
			if got := Now(); got.Second() != tt.want.Second() || got.Format(timeFormat) != tt.want.Format(timeFormat) {
				t.Errorf("Now() = %v, want %v", got, tt.want)
			} else {
				t.Logf("Now() = %v, want %v", got, tt.want)
			}
		})
	}
}

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
