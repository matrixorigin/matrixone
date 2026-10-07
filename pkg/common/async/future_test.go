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

package async

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFuture(t *testing.T) {
	t.Run("forwards arguments", func(t *testing.T) {
		sentinel := new(int)
		for _, tc := range []struct {
			name string
			args []interface{}
		}{
			{name: "zero"},
			{name: "single", args: []interface{}{42}},
			{name: "multiple", args: []interface{}{42, nil, sentinel, "last"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					future := AsyncCall(func(args ...interface{}) (interface{}, error) {
						return append([]interface{}(nil), args...), nil
					}, tc.args...)
					got := future.MustGet().([]interface{})
					require.Equal(t, tc.args, got)
					if tc.name == "multiple" {
						require.Same(t, sentinel, got[2])
					}
				})
			})
		}
	})

	t.Run("waits for operation and reports readiness", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			started := make(chan struct{})
			release := make(chan struct{})
			releaseOperation := sync.OnceFunc(func() { close(release) })
			defer releaseOperation()
			future := AsyncCall(func(...interface{}) (interface{}, error) {
				close(started)
				<-release
				return 42, nil
			})

			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("operation did not start")
			}
			require.False(t, future.IsReady())
			releaseOperation()
			synctest.Wait()
			require.True(t, future.IsReady(), "completed result must be observable before Get")
			require.Equal(t, 42, future.MustGet())
			require.True(t, future.IsReady())
		})
	})

	t.Run("preserves operation errors", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wantErr := errors.New("expected failure")
			future := AsyncCall(func(...interface{}) (interface{}, error) {
				return nil, wantErr
			})

			synctest.Wait()
			require.True(t, future.IsReady())
			value, err := future.Get()
			require.Nil(t, value)
			require.ErrorIs(t, err, wantErr)
			require.True(t, future.IsReady())
		})
	})

	t.Run("keeps concurrent futures independent", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			const count = 3
			started := make(chan struct{}, count)
			release := make(chan struct{})
			releaseOperations := sync.OnceFunc(func() { close(release) })
			defer releaseOperations()
			futures := make([]*Future, count)
			for i := range futures {
				futures[i] = AsyncCall(func(args ...interface{}) (interface{}, error) {
					started <- struct{}{}
					<-release
					if len(args) != 1 {
						return nil, errors.New("expected one argument")
					}
					return args[0], nil
				}, i)
			}

			for range futures {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("concurrent operation did not start")
				}
			}
			for _, future := range futures {
				require.False(t, future.IsReady())
			}

			releaseOperations()
			synctest.Wait()
			for i, future := range futures {
				require.True(t, future.IsReady())
				require.Equal(t, i, future.MustGet())
			}
		})
	})
}
