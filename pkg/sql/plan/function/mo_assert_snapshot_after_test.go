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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

func TestMoAssertSnapshotAfter(t *testing.T) {
	ctx := t.Context()
	bound, err := GetFunctionByName(ctx, "mo_assert_snapshot_after", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	ov, ok := GetFunctionByIdWithoutError(bound.GetEncodedOverloadID())
	require.True(t, ok)
	require.True(t, ov.IsVolatile())
	require.True(t, ov.CannotFold())

	snapshot := timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4294967294}
	require.NoError(t, assertSnapshotAfter(ctx, snapshot, "123-4294967293"))
	require.NoError(t, assertSnapshotAfter(ctx, snapshot, "122-4294967295"))
	require.NoError(t, assertSnapshotAfter(ctx, snapshot, "0-0"))
	for _, watermark := range []string{"123-4294967294", "123-4294967295", "124-0"} {
		err = assertSnapshotAfter(ctx, snapshot, watermark)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrSnapshotNotAfter), "%s: %v", watermark, err)
		require.Equal(t, "40001", err.(*moerr.Error).SqlState())
	}
	for _, watermark := range []string{"", "123", "0123-1", "123-01", "123-4294967296", "-1-0", "+123-0", "123-0 "} {
		err = assertSnapshotAfter(ctx, snapshot, watermark)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%q: %v", watermark, err)
	}
}

type assertionTxnOperator struct {
	client.TxnOperator
	opts txn.TxnOptions
	meta txn.TxnMeta
}

func (op assertionTxnOperator) TxnOptions() txn.TxnOptions { return op.opts }
func (op assertionTxnOperator) Txn() txn.TxnMeta           { return op.meta }

type assertionLockService struct {
	lockservice.LockService
}

func (assertionLockService) GetConfig() lockservice.Config {
	return lockservice.Config{ServiceID: "transaction-cn"}
}

func TestMoAssertSnapshotAfterUsesLiveLocalTransaction(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.LockService = assertionLockService{}
	op := &assertionTxnOperator{
		opts: txn.TxnOptions{CN: "transaction-cn", ByBegin: true}.WithUserTxn(),
		meta: txn.TxnMeta{
			Status:     txn.TxnStatus_Active,
			SnapshotTS: timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4},
		},
	}
	proc.Base.TxnOperator = op
	input, err := vector.NewConstBytes(types.T_text.ToType(), []byte("123-4"), 1, proc.Mp())
	require.NoError(t, err)
	defer input.Free(proc.Mp())
	id := EncodeOverloadID(MO_ASSERT_SNAPSHOT_AFTER, 0)
	_, err = RunFunctionDirectly(proc, id, []*vector.Vector{input}, 1)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrSnapshotNotAfter), "%v", err)
	require.Equal(t, uint32(4), op.meta.SnapshotTS.LogicalTime)

	// Reuse the same SQL expression after the actual snapshot changes.
	op.meta.SnapshotTS.LogicalTime = 5
	result, err := RunFunctionDirectly(proc, id, []*vector.Vector{input}, 1)
	require.NoError(t, err)
	defer result.Free(proc.Mp())
	require.True(t, vector.GetFixedAtNoTypeCheck[bool](result, 0))
	require.Equal(t, uint32(5), op.meta.SnapshotTS.LogicalTime)

	op.opts.ByBegin = false
	_, err = RunFunctionDirectly(proc, id, []*vector.Vector{input}, 1)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)

	op.opts.ByBegin = true
	proc.Ctx = context.WithValue(proc.Ctx, defines.RemoteRunContext{}, true)
	_, err = RunFunctionDirectly(proc, id, []*vector.Vector{input}, 1)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
}
