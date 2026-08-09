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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestClassifyViewRefreshFailureUsesTypedErrors(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		code        viewRefreshFailureCode
		disposition viewRefreshDisposition
	}{
		{"missing dependency", moerr.NewNoSuchTableNoCtx("db", "t"), viewRefreshFailureDependencyUnavailable, viewRefreshRetry},
		{"persisted dependency unavailable", &viewRefreshDependencyUnavailableError{cause: context.Canceled}, viewRefreshFailureDependencyUnavailable, viewRefreshRetry},
		{"parser incompatible", moerr.NewParseErrorNoCtx("bad persisted SQL"), viewRefreshFailurePlannerIncompatible, viewRefreshMarkInvalid},
		{"invalid View", moerr.NewBadView(context.Background(), "db", "v"), viewRefreshFailurePermanentlyInvalid, viewRefreshMarkInvalid},
		{"txn conflict", moerr.NewTxnNeedRetryNoCtx(), viewRefreshFailureTxnConflict, viewRefreshRetry},
		{"canceled", context.Canceled, viewRefreshFailureCanceled, viewRefreshRetry},
		{"deadline", context.DeadlineExceeded, viewRefreshFailureCanceled, viewRefreshRetry},
		{"rpc", moerr.NewRPCTimeoutNoCtx(), viewRefreshFailureInfrastructure, viewRefreshRetry},
		{"identity", &viewRefreshIdentityChangedError{cause: context.Canceled}, viewRefreshFailureIdentityChanged, viewRefreshMarkInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure := classifyViewRefreshFailure(test.err)
			require.Equal(t, test.code, failure.code)
			require.Equal(t, test.disposition, failure.disposition)
			require.ErrorIs(t, failure, test.err)
		})
	}
}
