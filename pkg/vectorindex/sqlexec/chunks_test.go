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

package sqlexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	executortest "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func TestChunkInserterBinaryRanges(t *testing.T) {
	for _, size := range []int{0, 1, 65535, 65536, 65537, 3*65536 + 7} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := make([]byte, size+13)
			for i := range data {
				data[i] = byte(i)
			}
			mp := mpool.MustNewZero()
			txn := executortest.NewMockTxnExecutor(gomock.NewController(t))
			var actual []byte
			var ids []string
			txn.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(sql string, opts executor.StatementOption) (executor.Result, error) {
				require.NotContains(t, sql, "load_file")
				require.Contains(t, sql, "CAST(? AS LONGBLOB)")
				account, role := opts.SessionIdentity()
				require.Equal(t, "tenant", account)
				require.Equal(t, "accountadmin", role)
				vec, err := opts.Params(mp)
				require.NoError(t, err)
				defer vec.Free(mp)
				for i := 0; i < vec.Length(); i += 2 {
					ids = append(ids, string(vec.GetRawBytesAt(i)))
					actual = append(actual, vec.GetRawBytesAt(i+1)...)
				}
				return executor.Result{}, nil
			}).AnyTimes()
			writer, err := NewChunkInserter(t.Context(), txn, executor.StatementOption{}.WithSessionIdentity("tenant", "accountadmin"), "db", "store", 2)
			require.NoError(t, err)
			next, err := writer.WriteRange(bytes.NewReader(data), 13, int64(size), "id'\\\x00", 7, 1)
			require.NoError(t, err)
			require.Equal(t, int64(7+(size+65535)/65536), next)
			require.NoError(t, writer.Flush())
			require.Equal(t, data[13:], append([]byte{}, actual...))
			for _, id := range ids {
				require.Equal(t, "id'\\\x00", id)
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestChunkInserterBatchBytesAndFrames(t *testing.T) {
	data := bytes.Repeat([]byte{0xff}, 513*vectorindex.MaxChunkSize)
	txn := executortest.NewMockTxnExecutor(gomock.NewController(t))
	var batches []int
	txn.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ string, opts executor.StatementOption) (executor.Result, error) {
		mp := mpool.MustNewZero()
		vec, err := opts.Params(mp)
		require.NoError(t, err)
		defer vec.Free(mp)
		var total int
		for i := 0; i < vec.Length(); i++ {
			total += len(vec.GetRawBytesAt(i))
		}
		require.LessOrEqual(t, total, maxChunkParamBytes)
		batches = append(batches, vec.Length()/2)
		return executor.Result{}, nil
	}).AnyTimes()
	w, err := NewChunkInserter(t.Context(), txn, executor.StatementOption{}, "db", "store", 2000)
	require.NoError(t, err)
	next, err := w.WriteRange(bytes.NewReader(data), 0, int64(len(data)), "x", 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(513), next)
	require.NoError(t, w.Flush())
	require.Equal(t, []int{127, 127, 127, 127, 5}, batches)
}

func TestChunkInserterFailureIsTerminal(t *testing.T) {
	for _, tc := range []struct {
		name               string
		offset, length, id int64
		cancel             bool
		want               error
	}{
		{"short read", 0, 4, 0, false, io.ErrUnexpectedEOF},
		{"negative range", -1, 1, 0, false, nil},
		{"range overflow", math.MaxInt64, 1, 0, false, nil},
		{"chunk overflow", 0, 1, math.MaxInt64, false, nil},
		{"cancel", 0, 1, 0, true, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			txn := executortest.NewMockTxnExecutor(gomock.NewController(t))
			w, err := NewChunkInserter(ctx, txn, executor.StatementOption{}, "db", "store", 1)
			require.NoError(t, err)
			if tc.cancel {
				cancel()
			}
			_, err = w.WriteRange(bytes.NewReader([]byte{1}), tc.offset, tc.length, "id", tc.id, 0)
			require.Error(t, err)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
			}
			require.ErrorIs(t, w.Flush(), err)
			require.Empty(t, w.params)
		})
	}
	t.Run("execution failure after a completed batch", func(t *testing.T) {
		txn := executortest.NewMockTxnExecutor(gomock.NewController(t))
		fail := errors.New("owned storage failure")
		txn.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(executor.Result{}, nil)
		txn.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(executor.Result{}, fail)
		w, err := NewChunkInserter(t.Context(), txn, executor.StatementOption{}, "db", "store", 1)
		require.NoError(t, err)
		_, err = w.WriteRange(bytes.NewReader(make([]byte, 3*65536)), 0, 3*65536, "id", 0, 0)
		require.ErrorIs(t, err, fail)
		require.ErrorIs(t, w.Flush(), fail)
		require.Empty(t, w.params)
	})
}
