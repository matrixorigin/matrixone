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

package process

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestPipelineStoppedCauseIdentity(t *testing.T) {
	require.Equal(t, "pipeline consumer finished", ErrPipelineStopped.Error())
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cancel(ErrPipelineStopped)
	require.Same(t, ErrPipelineStopped, context.Cause(ctx))
	require.ErrorIs(t, context.Cause(ctx), ErrPipelineStopped)
	require.False(t, IsPipelineFailure(ErrPipelineStopped))
	var executionError *moerr.Error
	require.False(t, errors.As(ErrPipelineStopped, &executionError))

	wrapped := fmt.Errorf("consumer: %w", ErrPipelineStopped)
	require.ErrorIs(t, wrapped, ErrPipelineStopped)
	require.True(t, IsPipelineCancellationError(wrapped))
	require.True(t, IsPipelineCancellationError(errors.Join(wrapped, context.Canceled)))

	// The same text is not the retirement token, and a substantive joined
	// failure must not be suppressed merely because another leaf is the token.
	unrelated := errors.New(ErrPipelineStopped.Error())
	require.NotErrorIs(t, unrelated, ErrPipelineStopped)
	require.False(t, IsPipelineCancellationError(unrelated))
	require.False(t, IsPipelineCancellationError(errors.Join(wrapped, unrelated)))
	failure := MarkPipelineFailure(ErrPipelineStopped)
	require.True(t, IsPipelineFailure(failure))
	require.Same(t, ErrPipelineStopped, UnwrapPipelineFailure(failure))
}
