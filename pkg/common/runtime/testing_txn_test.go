// Copyright 2026 Matrix Origin
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

package runtime

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBeforeLockHookWithoutAdjustHook(t *testing.T) {
	tc := &TestingContext{}
	called := false
	tc.SetBeforeLockFunc(func(_ []byte, tableID uint64) {
		require.Equal(t, uint64(42), tableID)
		called = true
	})
	tc.GetBeforeLockFunc()(nil, 42)
	require.True(t, called)

	tc.SetBeforeLockFunc(nil)
	require.NotPanics(t, func() { tc.GetBeforeLockFunc()(nil, 42) })
}
