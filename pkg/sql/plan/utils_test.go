// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_removeIf(t *testing.T) {
	strs := []string{"abc", "bc", "def"}

	del1 := make(map[string]struct{})
	del1["abc"] = struct{}{}
	res1 := RemoveIf[string](strs, func(t string) bool {
		return Find[string](del1, t)
	})
	assert.Equal(t, []string{"bc", "def"}, res1)

	del2 := make(map[string]struct{})
	for _, str := range strs {
		del2[str] = struct{}{}
	}
	res2 := RemoveIf[string](strs, func(t string) bool {
		return Find[string](del2, t)
	})
	assert.Equal(t, []string{}, res2)

	assert.Equal(t, []string(nil), RemoveIf[string](nil, nil))
}

func TestOffsetToString(t *testing.T) {
	tests := []struct {
		offset int
		want   string
	}{
		{3600, "+01:00"},
		{7200, "+02:00"},
		{-3600, "-01:00"},
		{-7200, "-02:00"},
		{0, "+00:00"},
		{3660, "+01:01"},
		{-3660, "-01:01"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("offset %d", tt.offset), func(t *testing.T) {
			if got := offsetToString(tt.offset); got != tt.want {
				t.Errorf("offsetToString(%d) = %v, want %v", tt.offset, got, tt.want)
			}
		})
	}
}
