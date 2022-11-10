// Copyright 2021 Matrix Origin
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

package sm

import (
	"testing"

	"github.com/lni/goutils/leaktest"
)

func TestLoop1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	q1 := make(chan any, 100)
	fn := func(batch []any, q chan any) {
		for _, item := range batch {
			t.Logf("loop1 %d", item.(int))
		}
	}
	loop := NewLoop(q1, nil, fn, 100)
	loop.Start()
	for i := 0; i < 10; i++ {
		q1 <- i
	}
	loop.Stop()
}
