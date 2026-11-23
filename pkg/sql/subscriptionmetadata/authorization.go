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

	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// AuthorizationSnapshotKey connects the origin-only metadata scan to the
// existing frontend statement owner. It carries no independent lifetime or
// cached decision, and is never populated from SQL arguments or a remote plan.
type AuthorizationSnapshotKey struct{}

// AuthorizationSnapshot borrows the current statement's authorization point.
// Trusted internal SQL without a frontend owner instead uses one fresh SI
// transaction for the complete candidate scan, never its historical data txn.
func AuthorizationSnapshot(ctx context.Context) (*client.SnapshotAdmission, error) {
	owner, ok := ctx.Value(AuthorizationSnapshotKey{}).(interface {
		SubscriptionMetadataSnapshot(context.Context) (*client.SnapshotAdmission, error)
	})
	if !ok {
		return nil, nil
	}
	return owner.SubscriptionMetadataSnapshot(ctx)
}
