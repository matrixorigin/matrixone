// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExcludedFilter(t *testing.T) {
	cases := []struct {
		inputs   []*Store
		excluded []string
		expected []*Store
	}{
		{
			inputs:   []*Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			excluded: []string{"a"},
			expected: []*Store{{ID: "b"}, {ID: "c"}},
		},
		{
			inputs:   []*Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			excluded: []string{"a", "b"},
			expected: []*Store{{ID: "c"}},
		},
		{
			inputs:   []*Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			excluded: []string{"a", "b", "c"},
			expected: nil,
		},
		{
			inputs:   []*Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			excluded: []string{"A", "B", "C"},
			expected: []*Store{{ID: "a"}, {ID: "b"}, {ID: "c"}},
		},
	}

	for _, c := range cases {
		outputs := FilterStore(c.inputs, []IFilter{NewExcludedFilter(c.excluded...)})
		assert.Equal(t, c.expected, outputs)
	}
}
