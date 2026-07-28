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

package write

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const DefaultRecoveryTimeout = 10 * time.Second

// NewRecoveryContext preserves request-scoped values but not cancellation.
// Commit verification and orphan registration must still run when the client
// disconnects, otherwise a successful remote commit can be misreported or an
// uploaded object can become undiscoverable. The fixed deadline prevents that
// recovery work from turning into an unbounded query teardown; it can become a
// service-level setting if recovery is later moved to a durable async worker.
func NewRecoveryContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeoutCause(
		context.WithoutCancel(parent),
		DefaultRecoveryTimeout,
		api.NewError(api.ErrMetadataIOTimeout, "Iceberg bounded recovery work timed out", nil),
	)
}
