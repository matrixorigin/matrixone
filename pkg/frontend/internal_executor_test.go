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

package frontend

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

func mockResultSet() *MysqlResultSet {
	set := &MysqlResultSet{}
	col := &MysqlColumn{}
	col.SetName("test")
	col.SetColumnType(defines.MYSQL_TYPE_LONG)
	col.SetSigned(true)
	set.AddColumn(col)
	set.AddRow([]any{42})
	return set
}

func TestIe(t *testing.T) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())

	ctx := context.TODO()
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	setGlobalPu(pu)
	executor := newIe()
	executor.ApplySessionOverride(ie.NewOptsBuilder().Username("dump").Finish())
	sess := executor.newCmdSession(ctx, ie.NewOptsBuilder().Database("mo_catalog").Internal(true).Finish())
	assert.Equal(t, "dump", sess.GetMysqlProtocol().GetUserName())

	err := executor.Exec(ctx, "whatever", ie.NewOptsBuilder().Finish())
	assert.Error(t, err)
	res := executor.Query(ctx, "whatever", ie.NewOptsBuilder().Finish())
	assert.Error(t, err)
	assert.Equal(t, uint64(0), res.RowCount())
}

func TestIeProto(t *testing.T) {
	setGlobalPu(config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	// Mock autoIncrCaches
	setGlobalAicm(&defines.AutoIncrCacheManager{})

	executor := NewInternalExecutor()
	p := executor.proto
	assert.True(t, p.IsEstablished())
	p.SetEstablished()
	p.Quit()
	p.ResetStatistics()
	_ = p.GetStats()
	_ = p.ConnectionID()
	ctx := context.TODO()
	assert.Panics(t, func() { p.GetRequest([]byte{1}) })
	assert.Nil(t, p.SendColumnDefinitionPacket(ctx, nil, 1))
	assert.Nil(t, p.SendColumnCountPacket(1))
	assert.Nil(t, p.SendEOFPacketIf(0, 1))
	assert.Nil(t, p.sendOKPacket(1, 1, 0, 0, ""))
	assert.Nil(t, p.sendEOFOrOkPacket(0, 1))

	p.stashResult = true
	p.SendResponse(ctx, &Response{
		category:     OkResponse,
		status:       0,
		affectedRows: 1,
		data:         nil,
	})
	assert.Nil(t, nil, p.result.resultSet)
	assert.Equal(t, uint64(1), p.result.affectedRows)
	p.SendResponse(ctx, &Response{
		category: ResultResponse,
		status:   0,
		data: &MysqlExecutionResult{
			affectedRows: 1,
			mrs:          mockResultSet(),
		},
	})
	v, _ := p.result.Value(ctx, 0, 0)
	assert.Equal(t, 42, v.(int))

	p.ResetStatistics()
	assert.NoError(t, p.SendResultSetTextBatchRowSpeedup(mockResultSet(), 1))
	r := p.swapOutResult()
	v, e := r.Value(ctx, 0, 0)
	assert.NoError(t, e)
	assert.Equal(t, 42, v.(int))
	p.ResetStatistics()
	assert.NoError(t, p.SendResultSetTextBatchRow(mockResultSet(), 1))
	r = p.swapOutResult()
	v, e = r.Value(ctx, 0, 0)
	assert.NoError(t, e)
	assert.Equal(t, 42, v.(int))
	assert.Equal(t, uint64(1), r.affectedRows)
	p.ResetStatistics()

	r = p.swapOutResult()
	assert.Equal(t, uint64(0), r.resultSet.GetRowCount())
}

func TestIeResult(t *testing.T) {
	set := mockResultSet()
	result := &internalExecResult{affectedRows: 1, resultSet: set, err: moerr.NewInternalError(context.TODO(), "random")}
	require.Equal(t, "internal error: random", result.Error().Error())
	require.Equal(t, uint64(1), result.ColumnCount())
	require.Equal(t, uint64(1), result.RowCount())
	n, ty, s, e := result.Column(context.TODO(), 0)
	require.NoError(t, e)
	require.Equal(t, "test", n)
	require.Equal(t, defines.MYSQL_TYPE_LONG, defines.MysqlType(ty))
	require.True(t, s)
	_, _, _, e = result.Column(context.TODO(), 1)
	require.Error(t, e)
	r, e := result.Row(context.TODO(), 0)
	require.Equal(t, 42, r[0].(int))
	require.NoError(t, e)
	_, e = result.Row(context.TODO(), 1)
	require.Error(t, e)
	v, e := result.Value(context.TODO(), 0, 0)
	require.NoError(t, e)
	require.Equal(t, 42, v.(int))
	v, e = result.ValueByName(context.TODO(), 0, "test")
	require.NoError(t, e)
	require.Equal(t, 42, v.(int))
	str, e := result.StringValueByName(context.TODO(), 0, "test")
	require.NoError(t, e)
	require.Equal(t, "42", str)
	str, e = result.StringValueByName(context.TODO(), 0, "tet")
	require.Error(t, e)
	require.Equal(t, "", str)
}

func DebugPrintInternalResult(ctx context.Context, res ie.InternalExecResult) string {
	buf := &bytes.Buffer{}
	for i := uint64(0); i < res.ColumnCount(); i++ {
		col, _, _, _ := res.Column(context.TODO(), i)
		buf.WriteString(col + ": ")
		for j := uint64(0); j < res.RowCount(); j++ {
			s, _ := res.StringValueByName(ctx, j, col)
			buf.WriteString(" | ")
			buf.WriteString(s)
		}
		buf.WriteString("\n")
	}
	return buf.String()
}
