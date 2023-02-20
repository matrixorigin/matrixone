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

package gc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func noopTestJob(_ context.Context) (err error) { return }

func TestManager(t *testing.T) {
	assert.Panics(
		t,
		func() {
			_ = NewManager(
				WithCronJob("n1", time.Second, noopTestJob),
				WithCronJob("n1", time.Second, noopTestJob),
			)
		},
	)

	var ints []int
	var cnt atomic.Int32
	mgr := NewManager(
		WithCronJob("n2", 3*time.Millisecond, func(_ context.Context) (err error) {
			ints = append(ints, 2)
			cnt.Add(1)
			return
		}),
		WithCronJob("n1", 5*time.Millisecond, func(_ context.Context) (err error) {
			ints = append(ints, 1)
			cnt.Add(1)
			return
		}),
	)

	mgr.Start()
	testutils.WaitExpect(1000, func() bool {
		return cnt.Load() >= int32(2)
	})
	time.Sleep(20 * time.Millisecond)
	mgr.Stop()
	t.Log(ints)
	assert.Equal(t, []int{2, 1}, ints[0:2])
}
