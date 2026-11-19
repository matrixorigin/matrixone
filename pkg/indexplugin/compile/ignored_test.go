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

package compile

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// ignoredCtx answers the count of the ignored table with n and its sample row with pk, code and
// reason ("" is NULL).
type ignoredCtx struct {
	CompileContext
	t      *testing.T
	n      int64
	reason string
	err    error
	sqls   []string
}

func (c *ignoredCtx) RunSqlWithResult(sql string) (executor.Result, error) {
	c.sqls = append(c.sqls, sql)
	if c.err != nil {
		return executor.Result{}, c.err
	}
	mp := mpool.MustNewZero()
	if strings.HasPrefix(sql, "SELECT COUNT(*)") {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(c.t, vector.AppendFixed(bat.Vecs[0], c.n, false, mp))
		bat.SetRowCount(1)
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(c.t, vector.AppendBytes(bat.Vecs[0], []byte("7"), false, mp))
	require.NoError(c.t, vector.AppendFixed(bat.Vecs[1], int32(moerr.ErrIndexDatalink), false, mp))
	require.NoError(c.t, vector.AppendBytes(bat.Vecs[2], []byte(c.reason), c.reason == "", mp))
	bat.SetRowCount(1)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
}

func TestCheckIgnoredRows(t *testing.T) {
	def := &plan.IndexDef{IndexTableName: "ign", IndexAlgoParams: `{"max_bad_records":"2"}`}

	ctx := &ignoredCtx{t: t, n: 0}
	require.NoError(t, CheckIgnoredRows(ctx, def, "db", 5))
	require.Len(t, ctx.sqls, 1, "no ignored row: no sample read")

	ctx = &ignoredCtx{t: t, n: 2}
	require.NoError(t, CheckIgnoredRows(ctx, def, "db", 5), "within the limit")

	ctx = &ignoredCtx{t: t, n: 3, reason: "stage://s/a.pdf: file not found"}
	err := CheckIgnoredRows(ctx, def, "db", 5)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrIndexTooManyBadRecords), "%v", err)
	require.Contains(t, err.Error(), "row 7: stage://s/a.pdf: file not found")

	ctx = &ignoredCtx{t: t, n: 2}
	err = CheckIgnoredRows(ctx, def, "db", 2)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrIndexAllRowsBad), "%v", err)
	require.Contains(t, err.Error(), moerr.MessageOf(moerr.ErrIndexDatalink), "the message of the code without a reason")

	ctx = &ignoredCtx{t: t, err: moerr.NewInternalErrorNoCtx("boom")}
	require.ErrorContains(t, CheckIgnoredRows(ctx, def, "db", 2), "boom")

	require.Error(t, CheckIgnoredRows(&ignoredCtx{t: t}, &plan.IndexDef{IndexAlgoParams: `{"max_bad_records":"x"}`}, "db", 1))
}
