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

package isolated

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestPKCheckAppliedConflictFrontier(t *testing.T) {
	ctx := context.Background()
	snapshot := timestamp.Timestamp{PhysicalTime: 20}
	earlier := timestamp.Timestamp{PhysicalTime: 10}
	later := timestamp.Timestamp{PhysicalTime: 30}
	calls := 0
	wait := func(_ context.Context, target timestamp.Timestamp) (timestamp.Timestamp, error) {
		calls++
		require.Equal(t, snapshot, target)
		return snapshot.Next(), nil
	}
	got, err := appliedConflictFrontier(ctx, snapshot, earlier, wait)
	require.NoError(t, err)
	require.Equal(t, snapshot, got)
	require.Equal(t, 1, calls)

	got, err = appliedConflictFrontier(ctx, snapshot, later, wait)
	require.NoError(t, err)
	require.Equal(t, later, got)
	require.Equal(t, 1, calls, "an already-applied frontier must not wait again")

	failure := errors.New("wait canceled")
	_, err = appliedConflictFrontier(ctx, snapshot, earlier, func(context.Context, timestamp.Timestamp) (timestamp.Timestamp, error) {
		return timestamp.Timestamp{}, failure
	})
	require.ErrorIs(t, err, failure)
	_, err = appliedConflictFrontier(ctx, snapshot, earlier, func(context.Context, timestamp.Timestamp) (timestamp.Timestamp, error) {
		return snapshot, nil
	})
	require.Error(t, err, "an exclusive applied timestamp must advance past the requested inclusive timestamp")
}
