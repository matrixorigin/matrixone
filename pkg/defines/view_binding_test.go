// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package defines

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPersistentViewBindingScope(t *testing.T) {
	parent, cancel := context.WithCancel(AttachAccountId(t.Context(), 42))
	defer cancel()
	binding := WithPersistentViewBinding(parent)
	require.False(t, IsPersistentViewBinding(parent))
	require.True(t, IsPersistentViewBinding(binding))
	require.Same(t, binding, WithPersistentViewBinding(binding))
	require.False(t, IsPublicationViewBinding(binding))
	require.False(t, IsNoEvaluationViewBinding(binding))
	account, err := GetAccountId(binding)
	require.NoError(t, err)
	require.Equal(t, uint32(42), account)
	cancel()
	require.ErrorIs(t, binding.Err(), context.Canceled)
}
