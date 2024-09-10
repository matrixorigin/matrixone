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

package taestorage

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

// Write implements storage.TxnTAEStorage
func (s *taeStorage) Write(
	ctx context.Context,
	txnMeta txn.TxnMeta,
	op uint32,
	payload []byte) (result []byte, err error) {
	switch op {
	case uint32(apipb.OpCode_OpPreCommit):
		return handleWrite(ctx, txnMeta, payload, s.taeHandler.HandlePreCommitWrite)
	default:
		return nil, moerr.NewNotSupportedf(ctx, "unknown write op: %v", op)
	}
}

func handleWrite[PReq unmashaler[Req], PResp mashaler[Resp], Req, Resp any](
	ctx context.Context,
	meta txn.TxnMeta,
	payload []byte,
	fn func(context.Context, txn.TxnMeta, PReq, PResp) error,
) (res []byte, err error) {

	var preq PReq = new(Req)
	if err := preq.UnmarshalBinary(payload); err != nil {
		return nil, err
	}

	var presp PResp = new(Resp)
	defer logReq("write", preq, meta, presp, &err)()

	err = fn(ctx, meta, preq, presp)
	if err != nil {
		return nil, err
	}

	data, err := presp.MarshalBinary()
	if err != nil {
		return nil, err
	}
	res = data

	return
}
