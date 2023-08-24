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

package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsEmptyDN(t *testing.T) {
	v := DNShard{}
	assert.True(t, v.IsEmpty())

	v.ShardID = 1
	assert.False(t, v.IsEmpty())
}

func TestValidStateString(t *testing.T) {
	cases := []struct {
		s string
		r bool
	}{
		{s: "", r: false},
		{s: "n", r: false},
		{s: "no", r: false},
		{s: "working", r: true},
		{s: "woRKing", r: true},
		{s: "draining", r: true},
		{s: "drainINg", r: true},
		{s: "drained", r: true},
		{s: "DRained", r: true},
	}
	for _, c := range cases {
		assert.Equal(t, c.r, ValidStateString(c.s))
	}
}
