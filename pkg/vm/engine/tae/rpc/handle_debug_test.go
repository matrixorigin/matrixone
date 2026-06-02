// Copyright 2021 - 2024 Matrix Origin
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

package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

func TestHandleBackup(t *testing.T) {
	h := mockTAEHandle(context.Background(), t, &options.Options{})

	req := &cmd_util.Checkpoint{
		FlushDuration: time.Second,
	}
	resp := &api.SyncLogTailResp{}

	cb, err := h.HandleBackup(context.Background(), txn.TxnMeta{}, req, resp)
	require.NoError(t, err)
	if cb != nil {
		cb()
	}
}

func TestHandleDiskCleaner_AddCheckerTTL(t *testing.T) {
	h := mockTAEHandle(context.Background(), t, &options.Options{})

	resp := &api.TNStringResponse{}

	// valid ttl
	req := &cmd_util.DiskCleaner{
		Op:    cmd_util.AddChecker,
		Key:   cmd_util.CheckerKeyTTL,
		Value: "2h",
	}
	cb, err := h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.NoError(t, err)
	if cb != nil {
		cb()
	}

	// invalid ttl
	req.Value = "invalid"
	_, err = h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.Error(t, err)

	// less than 1 hour
	req.Value = "30m"
	_, err = h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.Error(t, err)
}

func TestHandleDiskCleaner_AddCheckerMinTS(t *testing.T) {
	h := mockTAEHandle(context.Background(), t, &options.Options{})

	resp := &api.TNStringResponse{}

	// valid minTS
	req := &cmd_util.DiskCleaner{
		Op:    cmd_util.AddChecker,
		Key:   cmd_util.CheckerKeyMinTS,
		Value: "1234567890-1",
	}
	cb, err := h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.NoError(t, err)
	if cb != nil {
		cb()
	}

	// invalid format
	req.Value = "1234567890"
	_, err = h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.Error(t, err)

	// invalid time
	req.Value = "invalid-1"
	_, err = h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.Error(t, err)

	// invalid logic time
	req.Value = "1234567890-invalid"
	_, err = h.HandleDiskCleaner(context.Background(), txn.TxnMeta{}, req, resp)
	require.Error(t, err)
}
