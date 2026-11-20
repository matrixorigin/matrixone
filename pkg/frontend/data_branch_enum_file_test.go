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

package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestDataBranchPortableSQLRejectsEnumErrorOrdinal(t *testing.T) {
	columns := []types.Type{types.T_int32.ToType(), types.T_enum.ToType(), types.T_enum.ToType()}
	require.NoError(t, validatePortableDataBranchEnumValues(columns, []any{int32(1), types.Enum(1), nil}))
	require.NoError(t, validatePortableDataBranchEnumValues(columns, []any{int32(2), types.Enum(2), types.Enum(1)}))
	require.ErrorContains(t, validatePortableDataBranchEnumValues(columns, []any{int32(3), types.Enum(0), nil}), "ENUM error ordinal 0")
	require.ErrorContains(t, validatePortableDataBranchEnumValues(columns, []any{int32(4), "0", nil}), "stored ENUM ordinal")
	require.ErrorContains(t, validatePortableDataBranchEnumValues(columns, []any{int32(5)}), "row shape mismatch")
}

type enumFileFlushMutator struct{ appends, closes int }

func (m *enumFileFlushMutator) Mutate(context.Context, ...fileservice.IOEntry) error { return nil }
func (m *enumFileFlushMutator) Append(context.Context, ...fileservice.IOEntry) error {
	m.appends++
	if m.appends == 2 {
		return moerr.NewInternalErrorNoCtx("injected final flush failure")
	}
	return nil
}
func (m *enumFileFlushMutator) Close() error { m.closes++; return nil }

type enumFileFlushFS struct {
	fileservice.MutableFileService
	mutator          *enumFileFlushMutator
	failCleanup      bool
	deletes, closes  int
	cleanupContext   error
	cleanupRemaining time.Duration
	filePath         string
}

func (fs *enumFileFlushFS) Write(ctx context.Context, v fileservice.IOVector) error {
	fs.filePath = v.FilePath
	return fs.MutableFileService.Write(ctx, v)
}
func (fs *enumFileFlushFS) Delete(ctx context.Context, paths ...string) error {
	fs.deletes++
	if fs.deletes == 2 {
		fs.cleanupContext = ctx.Err()
		if deadline, ok := ctx.Deadline(); ok {
			fs.cleanupRemaining = time.Until(deadline)
		}
		if fs.failCleanup {
			return moerr.NewInternalErrorNoCtx("injected file cleanup failure")
		}
	}
	return fs.MutableFileService.Delete(ctx, paths...)
}
func (fs *enumFileFlushFS) Close(context.Context) { fs.closes++ }
func (fs *enumFileFlushFS) NewMutator(context.Context, string) (fileservice.Mutator, error) {
	fs.mutator = &enumFileFlushMutator{}
	return fs.mutator, nil
}

func TestDataBranchFinalFlushFailureCleansFile(t *testing.T) {
	for _, failCleanup := range []bool{false, true} {
		t.Run(map[bool]string{false: "cleanup succeeds", true: "cleanup fails"}[failCleanup], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ses := newValidateSession(t)
			ses.SetMysqlResultSet(&MysqlResultSet{})
			local, err := fileservice.NewLocalETLFS(defines.SharedFileServiceName, t.TempDir())
			require.NoError(t, err)
			defer local.Close(context.Background())
			target := &enumFileFlushFS{MutableFileService: local, failCleanup: failCleanup}
			installDataBranchTestFileService(t, ses, target)
			tbl := newTestBranchTableStuff(gomock.NewController(t))
			tbl.def.pkKind = fakeKind
			tbl.def.baseColNames = append([]string(nil), tbl.def.colNames...)
			bat := tbl.retPool.acquireRetBatch(tbl, false)
			t.Cleanup(func() { tbl.retPool.freeAllRetBatches(ses.proc.Mp()) })
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, ses.proc.Mp()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("a"), false, ses.proc.Mp()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[2], []byte("hidden"), false, ses.proc.Mp()))
			bat.SetRowCount(1)
			rows := make(chan batchWithKind, 1)
			rows <- batchWithKind{name: "target", kind: diffInsert, batch: bat}
			close(rows)
			stmt := &tree.DataBranchDiff{OutputOpt: &tree.DiffOutputOpt{DirPath: defines.SharedFileServiceName + ":/diff"}}
			err = satisfyDiffOutputOpt(ctx, cancel, func() {}, ses, nil, stmt, branchMetaInfo{}, tbl, rows)
			require.ErrorContains(t, err, "injected final flush failure")
			if failCleanup {
				require.ErrorContains(t, err, "injected file cleanup failure")
			} else {
				_, statErr := local.StatFile(context.Background(), target.filePath)
				require.True(t, moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound))
			}
			require.Zero(t, ses.GetMysqlResultSet().GetRowCount())
			require.Equal(t, 2, target.deletes)
			require.Equal(t, 1, target.mutator.closes)
			require.Zero(t, target.closes, "shared service is borrowed")
			require.NoError(t, target.cleanupContext)
			require.Greater(t, target.cleanupRemaining, time.Duration(0))
			require.LessOrEqual(t, target.cleanupRemaining, diffOutputCleanupTimeout)
		})
	}
}
