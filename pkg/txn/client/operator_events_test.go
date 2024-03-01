// Copyright 2023 Matrix Origin
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

package client

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClosedEvent(t *testing.T) {
	runClosedEventTests(t,
		func(tc *txnOperator) func(context.Context) error {
			return tc.Commit
		},
		txn.TxnStatus_Committed)

	runClosedEventTests(t,
		func(tc *txnOperator) func(context.Context) error {
			return tc.Rollback
		},
		txn.TxnStatus_Aborted)
}

func runClosedEventTests(
	t *testing.T,
	getAction func(tc *txnOperator) func(context.Context) error,
	status txn.TxnStatus) {
	runOperatorTests(
		t,
		func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
			cnt := 0
			tc.AppendEventCallback(ClosedEvent,
				func(tm txn.TxnMeta, _ error) {
					cnt++
					assert.Equal(t, status, tm.Status)
				})
			require.NoError(t, getAction(tc)(ctx))
			assert.Equal(t, 1, cnt)
		})
}
