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

package process

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWarningSinkContextGeneration(t *testing.T) {
	ctx := context.Background()
	require.Same(t, ctx, ContextWithWarningSink(ctx, nil))
	require.Nil(t, ContextWithWarningSink(nil, nil))
	require.Nil(t, WarningSinkFromContext(nil))
	old, next := new(int), new(int)
	first := ContextWithWarningSink(ctx, old)
	second := ContextWithWarningSink(first, next)
	require.Same(t, old, WarningSinkFromContext(first))
	require.Same(t, next, WarningSinkFromContext(second))
	require.Nil(t, WarningSinkFromContext(ContextWithWarningSink(second, nil)))
	require.Same(t, old, WarningSinkFromContext(ContextWithWarningSink(nil, old)))
}
