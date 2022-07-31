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

package txnengine

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type TableReader struct {
	ctx         context.Context
	txnOperator client.TxnOperator
	iterInfos   []IterInfo
}

type IterInfo struct {
	Node   logservicepb.DNNode
	IterID string
}

var _ engine.Reader = new(TableReader)

func (t *TableReader) Read(colNames []string, plan *plan.Expr, mh *mheap.Mheap) (*batch.Batch, error) {

	for {

		if len(t.iterInfos) == 0 {
			return nil, io.EOF
		}

		resps, err := doTxnRequest(
			t.ctx,
			t.txnOperator.Read,
			[]logservicepb.DNNode{t.iterInfos[0].Node},
			txn.TxnMethod_Read,
			OpRead,
			ReadReq{
				IterID:   t.iterInfos[0].IterID,
				ColNames: colNames,
			},
		)
		if err != nil {
			return nil, err
		}

		var r ReadResp
		if err := gob.NewDecoder(bytes.NewReader(resps[0])).Decode(&r); err != nil {
			return nil, err
		}

		if r.Batch == nil {
			// no more
			t.iterInfos = t.iterInfos[1:]
			continue
		}

		return r.Batch, nil
	}

}

func (t *TableReader) Close() error {
	for _, info := range t.iterInfos {
		_, err := doTxnRequest(
			t.ctx,
			t.txnOperator.Read,
			[]logservicepb.DNNode{info.Node},
			txn.TxnMethod_Read,
			OpCloseTableIter,
			CloseTableIterReq{
				IterID: info.IterID,
			},
		)
		_ = err // ignore error
	}
	return nil
}
