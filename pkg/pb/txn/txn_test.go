// Copyright 2021 - 2022 Matrix Origin
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

package txn

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
)

func TestErrorTypeConversion(t *testing.T) {
	err := moerr.NewInfo(context.TODO(), "test")

	txnErr := WrapError(err, 0)
	assert.Equal(t, txnErr.TxnErrCode, uint32(err.ErrorCode()))
	assert.Equal(t, err, txnErr.UnwrapError())

	txnErr = WrapError(err, moerr.ErrTAEError)
	assert.Equal(t, txnErr.TxnErrCode, uint32(moerr.ErrTAEError))
	assert.Equal(t, err, txnErr.UnwrapError())
}

func TestLegacyProtocolValuesRemainStable(t *testing.T) {
	assert.Equal(t, TxnStatus(1), TxnStatus_Prepared)
	assert.Equal(t, TxnStatus(2), TxnStatus_Committing)
	assert.Equal(t, TxnStatus(3), TxnStatus_Committed)
	assert.Equal(t, TxnStatus(4), TxnStatus_Aborting)
	assert.Equal(t, TxnStatus(5), TxnStatus_Aborted)

	assert.Equal(t, TxnMethod(4), TxnMethod_Prepare)
	assert.Equal(t, TxnMethod(5), TxnMethod_GetStatus)
	assert.Equal(t, TxnMethod(6), TxnMethod_CommitTNShard)
	assert.Equal(t, TxnMethod(7), TxnMethod_RollbackTNShard)
	assert.Equal(t, TxnMethod(8), TxnMethod_RemoveMedata)
	assert.Equal(t, TxnMethod(9), TxnMethod_DEBUG)
}
