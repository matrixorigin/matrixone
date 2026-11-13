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

package compile

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// waitM0RemoteReadSnapshot checks the returned watermark, not just a successful
// wait. Registration remains mo_m0-only; the shared implementation is exercised
// with the receiving scan's cancellation and snapshot in both test modes.
func waitM0RemoteReadSnapshot(ctx context.Context, cli client.TxnClient, snapshot timestamp.Timestamp) error {
	if ctx == nil || cli == nil || snapshot.IsEmpty() {
		return moerr.NewInternalErrorNoCtx("M0: remote snapshot watermark is unavailable")
	}
	applied, err := cli.WaitLogTailAppliedAt(ctx, snapshot.Prev())
	if err != nil {
		return err
	}
	if applied.Less(snapshot) {
		return moerr.NewInternalErrorNoCtx("M0: remote logtail watermark precedes scan snapshot")
	}
	return nil
}
