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

package taestorage

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/rpchandle"
	"github.com/stretchr/testify/require"
)

type recordingCommitMergeHandler struct {
	rpchandle.Handler
	calls int
}

func (h *recordingCommitMergeHandler) HandleCommitMerge(
	context.Context,
	txn.TxnMeta,
	*api.MergeCommitEntry,
	*api.TNStringResponse,
) error {
	h.calls++
	return nil
}

func TestWriteCommitMergeCompatibility(t *testing.T) {
	entry := &api.MergeCommitEntry{}
	payload, err := entry.MarshalBinary()
	require.NoError(t, err)

	handler := new(recordingCommitMergeHandler)
	storage := &taeStorage{taeHandler: handler}
	for _, op := range []api.OpCode{
		api.OpCode_OpCommitMerge,
		api.OpCode_OpCommitMergeV2,
	} {
		_, err = storage.Write(context.Background(), txn.TxnMeta{}, uint32(op), payload)
		require.NoError(t, err)
	}
	require.Equal(t, 2, handler.calls)

	_, err = storage.Write(context.Background(), txn.TxnMeta{}, 999_999, payload)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
	require.Equal(t, 2, handler.calls)
}
