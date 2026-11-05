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

package indexfile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// An index file is read as the process's internal SQL reads: at its named snapshot when that is
// historical, and as the account that SQL executes as.
func TestReadTarget(t *testing.T) {
	sp := sqlexec.NewSqlProcess(testutil.NewProcess(t))
	ctrl := gomock.NewController(t)
	now := timestamp.Timestamp{PhysicalTime: 100}
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: now}).AnyTimes()
	sp.Proc.Base.TxnOperator = op

	ctx, got, err := ReadTarget(sp)
	require.NoError(t, err)
	require.Same(t, op, got, "no named snapshot: the process's transaction")
	account, err := defines.GetAccountId(ctx)
	require.NoError(t, err)
	want, err := sp.GetAccountID()
	require.NoError(t, err)
	require.Equal(t, want, account)

	historic := timestamp.Timestamp{PhysicalTime: 50}
	clone := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().CloneSnapshotOp(historic).Return(clone)
	sp.SnapshotTS = &historic
	sp.WithExecutionIdentity(7, "pub_db")
	ctx, got, err = ReadTarget(sp)
	require.NoError(t, err)
	require.Same(t, clone, got, "a historical named snapshot: the transaction pinned at it")
	account, err = defines.GetAccountId(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(7), account, "the account the internal SQL executes as")

	sp.Proc = nil
	_, _, err = ReadTarget(sp)
	require.Error(t, err)
	_, err = OpenWithProcess(sp, Ref{}, "")
	require.Error(t, err)
}

func TestNewEnv(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = op
	sp := sqlexec.NewSqlProcess(proc)
	cache := newCache(t, t.TempDir())
	eng := mock_frontend.NewMockEngine(ctrl)
	other := mock_frontend.NewMockTxnOperator(ctrl)

	env := NewEnv(sp, &Files{Engine: eng, Cache: cache}, other, "/spill")
	require.Same(t, eng, env.Engine)
	require.Same(t, cache, env.Cache)
	require.Same(t, other, env.Txn, "the given transaction")
	require.Same(t, proc, env.Proc)
	require.Equal(t, "/spill", env.TempDir)

	env = NewEnv(sp, &Files{Cache: cache}, nil, "")
	require.Nil(t, env.Cache, "no cache without a storage engine")
	require.Same(t, op, env.Txn, "nil means the process's transaction")

	env = NewEnv(sp, nil, nil, "")
	require.Nil(t, env.Engine, "the test process has no storage engine")
	require.Nil(t, env.Cache)
	_, err := OpenWithProcess(sp, Ref{}, "")
	require.ErrorContains(t, err, "storage engine")

	env = NewEnv(sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(proc.Ctx, "", nil, 0, nil)), &Files{Engine: eng}, nil, "")
	require.Nil(t, env.Proc)
	require.Nil(t, env.Txn, "no process and no transaction given")
}

func TestTailFrame(t *testing.T) {
	frame := cuvscdc.FrameCdcChunk([]byte("records"), []byte("header"), 1, 2, 0)
	crc, ok := cuvscdc.CdcChunkChecksum(frame)
	require.True(t, ok)
	checksum := vectorindex.CdcChunkSetChecksum([]uint32{crc})

	ref := TailFrameRef("db", "idx", 9, int64(len(frame)), checksum)
	require.Equal(t, vectorindex.CdcTailId, ref.IndexID)
	require.Equal(t, int64(vectorindex.Tag_CdcEvents), ref.Tag)
	require.Equal(t, int64(9), ref.FirstChunk)
	require.NoError(t, ref.validate())

	path := filepath.Join(t.TempDir(), "frame")
	require.NoError(t, os.WriteFile(path, frame, 0o644))
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, ref.Verify(f, ref.Size, checksum))
	require.ErrorContains(t, VerifyTailFrame(f, ref.Size, "other"), "checksum mismatch")
	require.Error(t, VerifyTailFrame(f, ref.Size+1, checksum), "a read past the end fails")

	garbled := append([]byte(nil), frame...)
	garbled[len(garbled)-1] ^= 1
	require.Error(t, VerifyTailFrameBytes(garbled, checksum))
}
