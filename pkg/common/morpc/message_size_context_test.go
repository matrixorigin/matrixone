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

package morpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaxMessageSizeContext(t *testing.T) {
	ctx := ContextWithMaxMessageSize(context.Background(), 32*1024)
	size, ok := MaxMessageSizeFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, 32*1024, size)

	_, ok = MaxMessageSizeFromContext(ContextWithMaxMessageSize(ctx, 0))
	require.False(t, ok)
	_, ok = MaxMessageSizeFromContext(ContextWithMaxMessageSize(ctx, ^uint64(0)))
	require.False(t, ok)
	_, ok = MaxMessageSizeFromContext(nil)
	require.False(t, ok)
}
