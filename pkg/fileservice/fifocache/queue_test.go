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

package fifocache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	queue := NewQueue[int]()
	for i := 0; i < maxQueuePartCapacity*1024; i++ {
		queue.enqueue(i)
	}
	for i := 0; i < maxQueuePartCapacity*1024; i++ {
		n, ok := queue.dequeue()
		assert.True(t, ok)
		assert.Equal(t, i, n)
	}
}
