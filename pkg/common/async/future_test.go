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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFuture(t *testing.T) {
	t.Run("waits for operation and reports readiness", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		future := AsyncCall(func(...interface{}) (interface{}, error) {
			close(started)
			<-release
			return 42, nil
		})

		<-started
		require.False(t, future.IsReady())
		close(release)
		require.Equal(t, 42, future.MustGet())
		require.True(t, future.IsReady())
	})

	t.Run("preserves operation errors", func(t *testing.T) {
		wantErr := errors.New("expected failure")
		future := AsyncCall(func(...interface{}) (interface{}, error) {
			return nil, wantErr
		})

		value, err := future.Get()
		require.Nil(t, value)
		require.ErrorIs(t, err, wantErr)
		require.True(t, future.IsReady())
	})

	t.Run("keeps concurrent futures independent", func(t *testing.T) {
		const count = 3
		started := make(chan struct{}, count)
		release := make(chan struct{})
		futures := make([]*Future, count)
		for i := range futures {
			want := i
			futures[i] = AsyncCall(func(...interface{}) (interface{}, error) {
				started <- struct{}{}
				<-release
				return want, nil
			})
		}

		for range futures {
			<-started
		}
		for _, future := range futures {
			require.False(t, future.IsReady())
		}

		close(release)
		for i, future := range futures {
			require.Equal(t, i, future.MustGet())
		}
	})
}
