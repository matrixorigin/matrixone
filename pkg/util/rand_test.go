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
	"math/rand"
	"testing"
)

func TestFastrand64(t *testing.T) {
	tests := []struct {
		name    string
		wantNot uint64
	}{
		{
			name:    "normal",
			wantNot: rand.Uint64(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Fastrand64(); got == tt.wantNot {
				t.Errorf("Fastrand64() = %v, wantNot %v", got, tt.wantNot)
			}
		})
	}
}

func Test_fastrand32(t *testing.T) {
	tests := []struct {
		name    string
		wantNot uint32
	}{
		{
			name:    "normal",
			wantNot: rand.Uint32(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fastrand32(); got == tt.wantNot {
				t.Errorf("fastrand32() = %v, wantNot %v", got, tt.wantNot)
			}
		})
	}
}
