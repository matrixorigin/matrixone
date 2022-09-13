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

package task

import (
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetExpiredTasks(t *testing.T) {
	cases := []struct {
		tasks     []task.Task
		expiredCN []string

		expected []task.Task
	}{
		{
			tasks:     nil,
			expiredCN: nil,

			expected: nil,
		},
		{
			tasks:     []task.Task{{TaskRunner: "a"}, {TaskRunner: "b"}},
			expiredCN: []string{"a"},

			expected: []task.Task{{TaskRunner: "a"}},
		},
	}

	for _, c := range cases {
		results := getExpiredTasks(c.tasks, c.expiredCN)
		assert.Equal(t, c.expected, results)
	}
}

func TestGetCNOrderedMap(t *testing.T) {
	cases := []struct {
		tasks     []task.Task
		workingCN []string

		expected *OrderedMap
	}{
		{
			tasks:     nil,
			workingCN: nil,

			expected: NewOrderedMap(),
		},
		{
			tasks:     []task.Task{{TaskRunner: "a"}, {TaskRunner: "b"}, {TaskRunner: "b"}},
			workingCN: []string{"a", "b"},

			expected: &OrderedMap{
				Map:         map[string]uint32{"a": 1, "b": 2},
				OrderedKeys: []string{"a", "b"},
			},
		},
		{
			tasks:     []task.Task{{TaskRunner: "a"}, {TaskRunner: "b"}, {TaskRunner: "a"}, {TaskRunner: "a"}},
			workingCN: []string{"a", "b"},

			expected: &OrderedMap{
				Map:         map[string]uint32{"a": 3, "b": 1},
				OrderedKeys: []string{"b", "a"},
			},
		},
	}

	for _, c := range cases {
		results := getCNOrderedMap(c.tasks, c.workingCN)
		assert.Equal(t, c.expected, results)
	}
}
