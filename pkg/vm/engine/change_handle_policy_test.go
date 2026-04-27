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

func TestSnapshotReadPolicyContextRoundTrip(t *testing.T) {
	base := context.Background()

	require.Equal(t, SnapshotReadPolicyCheckpointReplay, SnapshotReadPolicyFromContext(nil))
	require.Equal(t, SnapshotReadPolicyCheckpointReplay, SnapshotReadPolicyFromContext(base))
	require.True(t, WithSnapshotReadPolicy(base, SnapshotReadPolicyCheckpointReplay) == base)

	ctx := WithSnapshotReadPolicy(base, SnapshotReadPolicyVisibleState)
	require.Equal(t, SnapshotReadPolicyVisibleState, SnapshotReadPolicyFromContext(ctx))
}

func TestRetainRowIDContextRoundTrip(t *testing.T) {
	base := context.Background()

	require.False(t, RetainRowIDFromContext(nil))
	require.False(t, RetainRowIDFromContext(base))
	require.True(t, WithRetainRowID(base, false) == base)

	ctx := WithRetainRowID(base, true)
	require.True(t, RetainRowIDFromContext(ctx))
}
