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

package subscriptionmetadata

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

type snapshotOwner struct {
	admission *client.SnapshotAdmission
	err       error
}

func (o snapshotOwner) SubscriptionMetadataSnapshot(ctx context.Context) (*client.SnapshotAdmission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return o.admission, o.err
}

func TestAuthorizationSnapshotOwner(t *testing.T) {
	admission, err := AuthorizationSnapshot(t.Context())
	require.NoError(t, err)
	require.Nil(t, admission, "trusted internal SQL creates its own current read")
	owner := snapshotOwner{admission: &client.SnapshotAdmission{}}
	ctx := context.WithValue(t.Context(), AuthorizationSnapshotKey{}, owner)
	admission, err = AuthorizationSnapshot(ctx)
	require.NoError(t, err)
	require.Same(t, owner.admission, admission, "borrow rather than create a new authorization point")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	admission, err = AuthorizationSnapshot(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, admission)
	owner.err = context.DeadlineExceeded
	ctx = context.WithValue(t.Context(), AuthorizationSnapshotKey{}, owner)
	_, err = AuthorizationSnapshot(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "an owner failure cannot fall back to data T")
}
