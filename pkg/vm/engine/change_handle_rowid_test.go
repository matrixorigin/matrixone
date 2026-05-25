// Copyright 2026 Matrix Origin
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

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetainRowIDContextRoundTrip(t *testing.T) {
	require.False(t, RetainRowIDFromContext(nil))
	require.Nil(t, WithRetainRowID(nil, true))

	ctx := context.Background()
	require.Equal(t, ctx, WithRetainRowID(ctx, false))
	require.False(t, RetainRowIDFromContext(ctx))

	retainCtx := WithRetainRowID(ctx, true)
	require.True(t, RetainRowIDFromContext(retainCtx))
	require.False(t, RetainRowIDFromContext(context.WithValue(ctx, retainRowIDKey{}, false)))
}
