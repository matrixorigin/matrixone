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

package memoryengine

import (
	"context"

	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func (t *Table) GetLogTail(
	ctx context.Context,
	from *timestamp.Timestamp,
	to *timestamp.Timestamp,
	targetShard Shard,
) (
	resp *apipb.SyncLogTailResp,
	err error,
) {

	resps, err := DoTxnRequest[apipb.SyncLogTailResp](
		ctx,
		t.txnOperator,
		true,
		thisShard(targetShard),
		OpGetLogTail,
		&apipb.SyncLogTailReq{
			CnHave: from,
			CnWant: to,
			Table: &apipb.TableID{
				TbId: uint64(t.id),
			},
		},
	)
	if err != nil {
		return nil, err
	}

	return &resps[0], nil
}
