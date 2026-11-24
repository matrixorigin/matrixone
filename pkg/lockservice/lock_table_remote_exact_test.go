// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestRemoteRangeAfterExactRows(t *testing.T) {
	const limit = 16
	for _, count := range []int{limit, limit + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			runLockServiceTestsWithLevel(
				t, zapcore.ErrorLevel, []string{"owner", "caller"}, 10*time.Second,
				func(_ *lockTableAllocator, services []*service) {
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()
					owner, caller := services[0], services[1]
					const table = uint64(91)
					reader, writer := newTestTxnID(1), newTestTxnID(2)
					exclusive := newTestRowExclusiveOptions()
					exclusive.Policy = pb.WaitPolicy_FastFail
					_, err := owner.Lock(ctx, table, [][]byte{{1}}, writer, exclusive)
					require.NoError(t, err)
					require.NoError(t, owner.Unlock(ctx, writer, timestamp.Timestamp{}))
					defer func() { require.NoError(t, caller.Unlock(ctx, reader, timestamp.Timestamp{})) }()

					points := make([][]byte, count)
					for i := range points {
						points[i] = []byte{byte(i + 2)}
					}
					for chunk := range slices.Chunk(points, 4) {
						_, err = caller.Lock(ctx, table, chunk, reader, newTestRowSharedOptions().WithExactRows())
						require.NoError(t, err)
					}

					// A disjoint ordinary range still has to pass the owner's
					// capacity check. The covering range can merge the exact keys.
					_, err = caller.Lock(ctx, table, [][]byte{{40}, {41}}, reader, newTestRangeSharedOptions())
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade), "%v", err)
					_, err = caller.Lock(ctx, table, [][]byte{{1}, {30}}, reader, newTestRangeSharedOptions())
					require.NoError(t, err)
					_, err = owner.Lock(ctx, table, points[len(points)-1:], writer, exclusive)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockConflict), "%v", err)
					require.NoError(t, owner.Unlock(ctx, writer, timestamp.Timestamp{}))
					require.NoError(t, caller.Unlock(ctx, reader, timestamp.Timestamp{}))
					_, err = owner.Lock(ctx, table, points[len(points)-1:], writer, exclusive)
					require.NoError(t, err)
					require.NoError(t, owner.Unlock(ctx, writer, timestamp.Timestamp{}))
				},
				func(cfg *Config) {
					cfg.MaxLockRowCount = 4
					cfg.MaxFixedSliceSize = limit
					cfg.EnableRemoteLocalProxy = false
				})
		})
	}
}
