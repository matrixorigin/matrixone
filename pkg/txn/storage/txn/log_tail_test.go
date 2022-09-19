// Copyright 2022 Matrix Origin
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

package txnstorage

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
	"github.com/stretchr/testify/assert"
)

func testLogTail(
	t *testing.T,
	newStorage func() (*Storage, error),
) {

	// new
	s, err := newStorage()
	assert.Nil(t, err)
	defer s.Close(context.TODO())

	// txn
	txnMeta := txn.TxnMeta{
		ID:     []byte("1"),
		Status: txn.TxnStatus_Active,
		SnapshotTS: timestamp.Timestamp{
			PhysicalTime: 1,
			LogicalTime:  1,
		},
	}

	// test get log tail
	{
		resp := testRead[txnengine.GetLogTailResp](
			t, s, txnMeta,
			txnengine.OpGetLogTail,
			txnengine.GetLogTailReq{
				//TODO args
			},
		)
		//TODO asserts
		_ = resp
	}
}
