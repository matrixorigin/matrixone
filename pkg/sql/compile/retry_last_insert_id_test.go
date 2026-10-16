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

package compile

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

// A retry restores the LAST_INSERT_ID the statement started with, not the value
// captured when the COM_QUERY began: an earlier statement of the same COM_QUERY
// may have generated one since (#29903).
func TestRetryRestoresStatementStartLastInsertID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	_, txnOp := newTestTxnClientAndOp(ctrl)

	c := NewMockCompile(t)
	c.proc.Base.TxnOperator = txnOp
	c.proc.Base.SessionInfo.LastInsertID = 0 // the COM_QUERY began with no value
	c.proc.SetLastInsertID(7)                // an earlier statement generated 7
	c.lastInsertIDBaseline = c.proc.GetLastInsertID()

	// the failed attempt generated 42
	c.proc.SetLastInsertID(42)
	c.proc.SetStatementLastInsertID(42)

	var wait time.Duration
	require.NoError(t, c.prepareRetryTransition(&wait))
	require.Equal(t, uint64(7), c.proc.GetLastInsertID())
	require.Zero(t, c.proc.GetStatementLastInsertID())
}
