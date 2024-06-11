// Copyright 2021 Matrix Origin
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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/testutil/testengine"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestScopeSerialization(t *testing.T) {
	testCases := []string{
		"select 1",
		"select * from R",
		"select count(*) from R",
		"select * from R limit 2, 1",
		"select * from R left join S on R.uid = S.uid",
	}

	var sourceScopes = generateScopeCases(t, testCases)

	for i, sourceScope := range sourceScopes {
		data, errEncode := encodeScope(sourceScope)
		require.NoError(t, errEncode)
		targetScope, errDecode := decodeScope(data, sourceScope.Proc, false, nil)
		require.NoError(t, errDecode)

		// Just do simple check
		require.Equal(t, len(sourceScope.PreScopes), len(targetScope.PreScopes), fmt.Sprintf("related SQL is '%s'", testCases[i]))
		require.Equal(t, len(sourceScope.Instructions), len(targetScope.Instructions), fmt.Sprintf("related SQL is '%s'", testCases[i]))
		for j := 0; j < len(sourceScope.Instructions); j++ {
			require.Equal(t, sourceScope.Instructions[j].Op, targetScope.Instructions[j].Op)
		}
		if sourceScope.DataSource == nil {
			require.Nil(t, targetScope.DataSource)
		} else {
			require.Equal(t, sourceScope.DataSource.SchemaName, targetScope.DataSource.SchemaName)
			require.Equal(t, sourceScope.DataSource.RelationName, targetScope.DataSource.RelationName)
			require.Equal(t, sourceScope.DataSource.PushdownId, targetScope.DataSource.PushdownId)
			require.Equal(t, sourceScope.DataSource.PushdownAddr, targetScope.DataSource.PushdownAddr)
		}
		require.Equal(t, sourceScope.NodeInfo.Addr, targetScope.NodeInfo.Addr)
		require.Equal(t, sourceScope.NodeInfo.Id, targetScope.NodeInfo.Id)
	}

}

func generateScopeCases(t *testing.T, testCases []string) []*Scope {
	// getScope method generate and return the scope of a SQL string.
	getScope := func(t1 *testing.T, sql string) *Scope {
		proc := testutil.NewProcess()
		proc.SessionInfo.Buf = buffer.New()
		e, _, compilerCtx := testengine.New(defines.AttachAccountId(context.Background(), catalog.System_Account))
		opt := plan2.NewBaseOptimizer(compilerCtx)
		ctx := compilerCtx.GetContext()
		stmts, err := mysql.Parse(ctx, sql, 1, 0)
		require.NoError(t1, err)
		qry, err := opt.Optimize(stmts[0], false)
		require.NoError(t1, err)
		proc.Ctx = ctx
		c := NewCompile("test", "test", sql, "", "", context.Background(), e, proc, nil, false, nil, time.Now())
		qry.Nodes[0].Stats.Cost = 10000000 // to hint this is ap query for unit test
		err = c.Compile(ctx, &plan.Plan{Plan: &plan.Plan_Query{Query: qry}}, func(batch *batch.Batch) error {
			return nil
		})
		require.NoError(t1, err)
		// ignore the last operator if it's output
		if c.scope[0].Instructions[len(c.scope[0].Instructions)-1].Op == vm.Output {
			c.scope[0].Instructions = c.scope[0].Instructions[:len(c.scope[0].Instructions)-1]
		}
		return c.scope[0]
	}

	result := make([]*Scope, len(testCases))
	for i, sql := range testCases {
		result[i] = getScope(t, sql)
	}
	return result
}

func TestMessageSenderOnClientReceive(t *testing.T) {
	sender := new(messageSenderOnClient)
	sender.receiveCh = make(chan morpc.Message, 1)

	// case 1: use source context, and source context is canceled
	{
		sourceCtx, sourceCancel := context.WithCancel(context.Background())
		sender.ctx = sourceCtx
		sender.ctxCancel = sourceCancel
		sourceCancel()
		v, err := sender.receiveMessage()
		require.NoError(t, err)
		require.Equal(t, nil, v)
	}

	// case 2: use derived context, and source context is canceled
	{
		sourceCtx, sourceCancel := context.WithCancel(context.Background())
		receiveCtx, receiveCancel := context.WithTimeout(sourceCtx, 3*time.Second)
		sender.ctx = receiveCtx
		sender.ctxCancel = receiveCancel
		sourceCancel()

		startTime := time.Now()
		v, err := sender.receiveMessage()
		require.NoError(t, err)
		require.Equal(t, nil, v)
		require.True(t, time.Since(startTime) < 3*time.Second)
		receiveCancel()
	}

	// case 3: receive a nil message
	{
		sourceCtx, sourceCancel := context.WithCancel(context.Background())
		sender.ctx = sourceCtx
		sender.ctxCancel = sourceCancel
		sender.receiveCh <- nil
		_, err := sender.receiveMessage()
		require.NotNil(t, err)
		sourceCancel()
	}

	// case 4: receive a message
	{
		sourceCtx, sourceCancel := context.WithCancel(context.Background())
		sender.ctx = sourceCtx
		sender.ctxCancel = sourceCancel
		data := &pipeline.Message{}
		sender.receiveCh <- data
		v, err := sender.receiveMessage()
		require.NoError(t, err)
		require.Equal(t, data, v)
		sourceCancel()
	}

	// case 5: channel is closed
	{
		sourceCtx, sourceCancel := context.WithCancel(context.Background())
		sender.ctx = sourceCtx
		sender.ctxCancel = sourceCancel
		close(sender.receiveCh)
		_, err := sender.receiveMessage()
		require.NotNil(t, err)
	}
}
