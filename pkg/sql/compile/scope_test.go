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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/testutil/testengine"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func checkSrcOpsWithDst(srcRoot vm.Operator, dstRoot vm.Operator) bool {
	if srcRoot == nil && dstRoot == nil {
		return true
	}
	if srcRoot == nil || dstRoot == nil {
		return false
	}
	if srcRoot.OpType() != dstRoot.OpType() {
		return false
	}
	srcNumChildren := srcRoot.GetOperatorBase().NumChildren()
	dstNumChildren := dstRoot.GetOperatorBase().NumChildren()
	if srcNumChildren != dstNumChildren {
		return false
	}
	for i := 0; i < srcNumChildren; i++ {
		res := checkSrcOpsWithDst(srcRoot.GetOperatorBase().GetChildren(i), dstRoot.GetOperatorBase().GetChildren(i))
		if !res {
			return false
		}
	}
	return true
}

func TestScopeSerialization(t *testing.T) {
	testCases := []string{
		"select 1",
		"select * from R",
		//	"select count(*) from R",  todo, because MemRelationData.MarshalBinary() is not support now
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
		require.Equal(t, checkSrcOpsWithDst(sourceScope.RootOp, targetScope.RootOp), true, fmt.Sprintf("related SQL is '%s'", testCases[i]))

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

func checkScopeRoot(t *testing.T, s *Scope) {
	require.NotEqual(t, nil, s.RootOp)
	for i := range s.PreScopes {
		checkScopeRoot(t, s.PreScopes[i])
	}
}

func TestScopeSerialization2(t *testing.T) {
	testCompile := NewMockCompile(t)
	var reg process.WaitRegister
	testCompile.proc.Reg.MergeReceivers = []*process.WaitRegister{&reg}

	// join->Shuffle->Dispatch
	s := generateScopeWithRootOperator(
		testCompile.proc,
		[]vm.OpType{vm.HashJoin, vm.Shuffle, vm.Dispatch})
	s.IsEnd = true
	//join->connector
	s1 := generateScopeWithRootOperator(
		testCompile.proc,
		[]vm.OpType{vm.HashJoin, vm.Connector})
	s.PreScopes = []*Scope{s1}

	// tablescan-> projection -> connector.)
	s2 := generateScopeWithRootOperator(
		testCompile.proc,
		[]vm.OpType{vm.TableScan, vm.Projection, vm.Connector})
	s.PreScopes[0].PreScopes = []*Scope{s2}
	scopeData, err := encodeScope(s)
	require.NoError(t, err)
	scope, err := decodeScope(scopeData, testCompile.proc, true, nil)
	require.NoError(t, err)
	checkScopeRoot(t, scope)
}

func generateScopeCases(t *testing.T, testCases []string) []*Scope {
	// getScope method generate and return the scope of a SQL string.
	getScope := func(t1 *testing.T, sql string) *Scope {
		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()
		ctrl := gomock.NewController(t)
		txnCli, txnOp := newTestTxnClientAndOp(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		e, _, compilerCtx := testengine.New(defines.AttachAccountId(context.Background(), catalog.System_Account))
		opt := plan2.NewBaseOptimizer(compilerCtx)
		ctx := compilerCtx.GetContext()
		stmts, err := mysql.Parse(ctx, sql, 1)
		require.NoError(t1, err)
		qry, err := opt.Optimize(stmts[0], false)
		require.NoError(t1, err)
		proc.Ctx = ctx
		proc.ReplaceTopCtx(ctx)
		c := NewCompile("test", "test", sql, "", "", e, proc, nil, false, nil, time.Now())
		qry.Nodes[0].Stats.Cost = 10000000 // to hint this is ap query for unit test
		err = c.Compile(ctx, &plan.Plan{Plan: &plan.Plan_Query{Query: qry}}, func(batch *batch.Batch, crs *perfcounter.CounterSet) error {
			return nil
		})
		require.NoError(t1, err)
		// ignore the last operator if it's output
		if c.scopes[0].RootOp.OpType() == vm.Output {
			c.scopes[0].RootOp = c.scopes[0].RootOp.GetOperatorBase().GetChildren(0)
		}
		return c.scopes[0]
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

func TestMessageSenderOnClientReceiveBatchContextDone(t *testing.T) {
	t.Run("cancel returns query interrupted", func(t *testing.T) {
		sender := new(messageSenderOnClient)
		sender.receiveCh = make(chan morpc.Message, 1)
		ctx, cancel := context.WithCancel(context.Background())
		sender.ctx = ctx
		sender.ctxCancel = cancel
		cancel()

		bat, over, err := sender.receiveBatch()
		require.Nil(t, bat)
		require.False(t, over)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted))
	})

	t.Run("upstream deadline returns query interrupted", func(t *testing.T) {
		sender := new(messageSenderOnClient)
		sender.receiveCh = make(chan morpc.Message, 1)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		sender.ctx = ctx
		sender.ctxCancel = cancel
		defer cancel()

		<-ctx.Done()

		bat, over, err := sender.receiveBatch()
		require.Nil(t, bat)
		require.False(t, over)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted))
	})

	t.Run("internal deadline returns rpc timeout", func(t *testing.T) {
		sender := new(messageSenderOnClient)
		sender.receiveCh = make(chan morpc.Message, 1)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		sender.ctx = ctx
		sender.ctxCancel = cancel
		sender.useInternalTimeout = true
		defer cancel()

		<-ctx.Done()

		bat, over, err := sender.receiveBatch()
		require.Nil(t, bat)
		require.False(t, over)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrRPCTimeout))
	})

	t.Run("cancel during merge loop returns query interrupted", func(t *testing.T) {
		sender := new(messageSenderOnClient)
		sender.receiveCh = make(chan morpc.Message, 1)
		ctx, cancel := context.WithCancel(context.Background())
		sender.ctx = ctx
		sender.ctxCancel = cancel
		defer cancel()

		sender.receiveCh <- &pipeline.Message{Sid: pipeline.Status_WaitingNext, Data: []byte("partial")}
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		bat, over, err := sender.receiveBatch()
		require.Nil(t, bat)
		require.False(t, over)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted))
	})
}

func TestNewParallelScope(t *testing.T) {
	// function `newParallelScope` will dispatch one scope's work into n scopes.
	testCompile := NewMockCompile(t)

	var reg process.WaitRegister
	testCompile.proc.Reg.MergeReceivers = []*process.WaitRegister{&reg}

	// 1. test (rightSemi -> projection -> limit -> connector.)
	{
		scopeToParallel := generateScopeWithRootOperator(
			testCompile.proc,
			[]vm.OpType{vm.HashJoin, vm.Projection, vm.Limit, vm.Connector})

		scopeToParallel.NodeInfo.Mcpu = 4
		_, ss := newParallelScope(scopeToParallel)
		require.NoError(t, checkScopeWithExpectedList(ss[0], []vm.OpType{vm.HashJoin, vm.Projection, vm.Limit, vm.Connector}))
		require.NoError(t, checkScopeWithExpectedList(ss[1], []vm.OpType{vm.HashJoin, vm.Projection, vm.Limit, vm.Connector}))
		require.NoError(t, checkScopeWithExpectedList(ss[2], []vm.OpType{vm.HashJoin, vm.Projection, vm.Limit, vm.Connector}))
		require.NoError(t, checkScopeWithExpectedList(ss[3], []vm.OpType{vm.HashJoin, vm.Projection, vm.Limit, vm.Connector}))
	}

	// 2. test (right -> filter -> projection -> connector.)
	{
		scopeToParallel := generateScopeWithRootOperator(
			testCompile.proc,
			[]vm.OpType{vm.HashJoin, vm.Filter, vm.Projection, vm.Connector})

		scopeToParallel.NodeInfo.Mcpu = 4

		_, ss := newParallelScope(scopeToParallel)
		require.NoError(t, checkScopeWithExpectedList(ss[0], []vm.OpType{vm.HashJoin, vm.Filter, vm.Projection, vm.Connector}))
		require.NoError(t, checkScopeWithExpectedList(ss[1], []vm.OpType{vm.HashJoin, vm.Filter, vm.Projection, vm.Connector}))
		require.NoError(t, checkScopeWithExpectedList(ss[2], []vm.OpType{vm.HashJoin, vm.Filter, vm.Projection, vm.Connector}))
		require.NoError(t, checkScopeWithExpectedList(ss[3], []vm.OpType{vm.HashJoin, vm.Filter, vm.Projection, vm.Connector}))
	}

	// 3. test (rightanti -> shuffle  -> dispatch.)
	{
		scopeToParallel := generateScopeWithRootOperator(
			testCompile.proc,
			[]vm.OpType{vm.HashJoin, vm.Shuffle, vm.Dispatch})

		scopeToParallel.NodeInfo.Mcpu = 3

		_, ss := newParallelScope(scopeToParallel)
		require.NoError(t, checkScopeWithExpectedList(ss[0], []vm.OpType{vm.HashJoin, vm.Shuffle, vm.Dispatch}))
		require.NoError(t, checkScopeWithExpectedList(ss[1], []vm.OpType{vm.HashJoin, vm.Shuffle, vm.Dispatch}))
		require.NoError(t, checkScopeWithExpectedList(ss[2], []vm.OpType{vm.HashJoin, vm.Shuffle, vm.Dispatch}))
	}
}

func TestCompileExternValueScan(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{engine.Node{Addr: "cn1:6001"}, engine.Node{Addr: "cn2:6001"}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: "test.csv",
		},
	}
	n := &plan.Node{
		TableDef:   &plan.TableDef{},
		ExternScan: &plan.ExternScan{},
	}
	rs, err := testCompile.compileExternValueScan(n, param, true)
	require.NoError(t, err)
	require.NoError(t, checkScopeWithExpectedList(rs[0], []vm.OpType{vm.External}))
}

func TestCompileExternScanParallelWrite(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{engine.Node{Addr: "cn1:6001", Mcpu: 4}, engine.Node{Addr: "cn2:6001", Mcpu: 4}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: "test.csv",
		},
	}
	n := &plan.Node{
		TableDef:   &plan.TableDef{},
		ExternScan: &plan.ExternScan{},
	}
	rs, err := testCompile.compileExternScanParallelWrite(n, param, []string{"a", "b"}, []int64{100000, 100000}, true)
	require.NoError(t, err)
	require.NoError(t, checkScopeWithExpectedList(rs[0], []vm.OpType{vm.Merge}))
	require.NoError(t, checkScopeWithExpectedList(rs[0].PreScopes[0], []vm.OpType{vm.External, vm.Dispatch}))
}

func TestCompileExternScanParallelReadWrite(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{engine.Node{Addr: "cn1:6001", Mcpu: 4}, engine.Node{Addr: "cn2:6001", Mcpu: 4}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	ctx := context.TODO()
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: "test.csv",
			Tail:     &tree.TailParameter{},
		},
		ExParam: tree.ExParam{
			Ctx:    ctx,
			Strict: true,
		},
	}
	n := &plan.Node{
		TableDef:   &plan.TableDef{},
		ExternScan: &plan.ExternScan{},
	}
	filePath := fmt.Sprintf("%s/../../../test/distributed/resources/load_data/parallel_1.txt", GetFilePath())
	filePath = path.Clean("/" + filePath)
	fileSize := []int64{int64(colexec.WriteS3Threshold) * 2}
	_, err := testCompile.compileExternScanParallelReadWrite(n, param, []string{filePath}, fileSize, true)
	require.NoError(t, err)
	param.Strict = false
	_, err = testCompile.compileExternScanParallelReadWrite(n, param, []string{filePath}, fileSize, false)
	require.NoError(t, err)
	fileSize = []int64{int64(colexec.WriteS3Threshold) * 3}
	_, err = testCompile.compileExternScanParallelReadWrite(n, param, []string{filePath}, fileSize, false)
	require.NoError(t, err)

	// Compressed files should not be split for parallel read.
	gzPath := fmt.Sprintf("%s/../../../test/distributed/resources/load_data/parallel_1.txt.gz", GetFilePath())
	gzPath = path.Clean("/" + gzPath)
	_, err = testCompile.compileExternScanParallelReadWrite(n, param, []string{gzPath}, fileSize, false)
	require.Error(t, err)
}

func TestCompileExternScanParallelReadWriteRejectsParquet(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.proc.Ctx = context.Background()
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 4}}
	testCompile.addr = "cn1:6001"
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format:   tree.PARQUET,
			Filepath: "test.parq",
			Tail:     &tree.TailParameter{},
		},
		ExParam: tree.ExParam{
			Parallel: true,
		},
	}
	n := &plan.Node{
		TableDef:   &plan.TableDef{},
		ExternScan: &plan.ExternScan{},
	}

	_, err := testCompile.compileExternScanParallelReadWrite(n, param, []string{"test.parq"}, []int64{int64(colexec.WriteS3Threshold) * 2}, true)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	require.Contains(t, err.Error(), "parquet load cannot use byte-offset parallel read")
}

func TestGetReadWriteParallelFlagDisablesParquetReadSplit(t *testing.T) {
	testCompile := NewMockCompile(t)
	fileList := []string{"test.parq"}

	readParallel, writeParallel := testCompile.getReadWriteParallelFlag(
		&tree.ExternParam{
			ExParamConst: tree.ExParamConst{Format: tree.PARQUET},
			ExParam:      tree.ExParam{Parallel: true},
		},
		fileList,
	)
	require.False(t, readParallel)
	require.True(t, writeParallel)

	readParallel, writeParallel = testCompile.getReadWriteParallelFlag(
		&tree.ExternParam{
			ExParamConst: tree.ExParamConst{Format: tree.PARQUET},
			ExParam: tree.ExParam{
				Local:    true,
				Parallel: true,
			},
		},
		fileList,
	)
	require.False(t, readParallel)
	require.True(t, writeParallel)

	readParallel, writeParallel = testCompile.getReadWriteParallelFlag(
		&tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				Format:       tree.PARQUET,
				CompressType: tree.GZIP,
			},
			ExParam: tree.ExParam{
				Parallel: true,
			},
		},
		[]string{"test.parq.gz"},
	)
	require.False(t, readParallel)
	require.True(t, writeParallel)

	readParallel, writeParallel = testCompile.getReadWriteParallelFlag(
		&tree.ExternParam{
			ExParamConst: tree.ExParamConst{Format: tree.CSV},
			ExParam:      tree.ExParam{Parallel: true},
		},
		[]string{"test.csv"},
	)
	require.True(t, readParallel)
	require.True(t, writeParallel)

	readParallel, writeParallel = testCompile.getReadWriteParallelFlag(
		&tree.ExternParam{
			ExParamConst: tree.ExParamConst{Format: tree.PARQUET},
			ExParam: tree.ExParam{
				Parallel:              false,
				ParallelLoadRequested: true,
			},
		},
		fileList,
	)
	require.False(t, readParallel)
	require.False(t, writeParallel)
}

func TestCompileExternScanHiveFileFanout(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 2}, {Addr: "cn2:6001", Mcpu: 2}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}

	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType:         tree.S3,
			Filepath:         "warehouse/sales",
			Format:           tree.PARQUET,
			HivePartitioning: true,
			Tail:             &tree.TailParameter{},
		},
		ExParam: tree.ExParam{
			Parallel: true,
		},
	}
	n := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			TbColToDataCol: map[string]int32{},
		},
	}
	fileList := []string{
		"warehouse/sales/year=2024/month=01/p0.parquet",
		"warehouse/sales/year=2024/month=02/p1.parquet",
		"warehouse/sales/year=2025/month=01/p2.parquet",
		"warehouse/sales/year=2025/month=02/p3.parquet",
	}
	fileSize := []int64{10, 20, 30, 40}

	ss, err := testCompile.compileExternScanHiveFileFanout(n, param, fileList, fileSize, true)
	require.NoError(t, err)
	require.Len(t, ss, 4)
	require.Equal(t, "warehouse/sales", param.Filepath)
	require.True(t, param.Parallel)

	totalFiles := 0
	for _, scope := range ss {
		require.NoError(t, checkScopeWithExpectedList(scope, []vm.OpType{vm.External}))
		require.Equal(t, 1, scope.NodeInfo.Mcpu)
		ext, ok := scope.RootOp.(*external.External)
		require.True(t, ok)
		require.False(t, ext.Es.Extern.Parallel)
		require.Equal(t, "warehouse/sales", ext.Es.Extern.Filepath)
		require.Len(t, ext.Es.FileOffsetTotal, len(ext.Es.FileList))
		for _, off := range ext.Es.FileOffsetTotal {
			require.Equal(t, []int64{0, -1}, off.Offset)
		}
		totalFiles += len(ext.Es.FileList)
	}
	require.Equal(t, len(fileList), totalFiles)
}

func TestCompileExternScanParquetLoadFileFanout(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 2}, {Addr: "cn2:6001", Mcpu: 2}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}

	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.S3,
			Filepath: "warehouse/load/*.parquet",
			Format:   tree.PARQUET,
			Tail:     &tree.TailParameter{},
		},
		ExParam: tree.ExParam{
			ExternType: int32(plan.ExternType_LOAD),
			Parallel:   true,
		},
	}
	n := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_LOAD),
			TbColToDataCol: map[string]int32{},
		},
	}
	fileList := []string{
		"warehouse/load/part-000.parquet",
		"warehouse/load/part-001.parquet",
		"warehouse/load/part-002.parquet",
	}
	fileSize := []int64{30, 10, 20}

	ss, err := testCompile.compileExternScanParquetLoadFileFanout(n, param, fileList, fileSize, true)
	require.NoError(t, err)
	require.Len(t, ss, 3)
	require.Equal(t, "warehouse/load/*.parquet", param.Filepath)
	require.True(t, param.Parallel)
	require.False(t, param.HivePartitioning)

	totalFiles := 0
	for _, scope := range ss {
		require.NoError(t, checkScopeWithExpectedList(scope, []vm.OpType{vm.External}))
		require.Equal(t, 1, scope.NodeInfo.Mcpu)
		ext, ok := scope.RootOp.(*external.External)
		require.True(t, ok)
		require.False(t, ext.Es.Extern.Parallel)
		require.False(t, ext.Es.Extern.HivePartitioning)
		require.Equal(t, "warehouse/load/*.parquet", ext.Es.Extern.Filepath)
		require.Len(t, ext.Es.FileOffsetTotal, len(ext.Es.FileList))
		for _, off := range ext.Es.FileOffsetTotal {
			require.Equal(t, []int64{0, -1}, off.Offset)
		}
		totalFiles += len(ext.Es.FileList)
	}
	require.Equal(t, len(fileList), totalFiles)
}

func TestSplitParquetRowGroupShardsBalancesAndReindexesFiles(t *testing.T) {
	fileList := []string{"warehouse/load/part-000.parquet", "warehouse/load/part-001.parquet"}
	fileSize := []int64{190, 30}
	rowGroups := []parquetRowGroupMeta{
		{fileIndex: 0, rowGroupIndex: 0, numRows: 10, bytes: 100},
		{fileIndex: 0, rowGroupIndex: 1, numRows: 9, bytes: 90},
		{fileIndex: 1, rowGroupIndex: 0, numRows: 1, bytes: 10},
		{fileIndex: 1, rowGroupIndex: 1, numRows: 2, bytes: 20},
	}
	nodes := engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}, {Addr: "cn2:6001", Mcpu: 1}}

	shards, err := splitParquetRowGroupShards(fileList, fileSize, rowGroups, nodes)
	require.NoError(t, err)
	require.Len(t, shards, 2)

	loads := make(map[string]int64)
	seen := make(map[string]bool)
	for _, shard := range shards {
		require.NotEmpty(t, shard.fileList)
		require.Len(t, shard.fileList, len(shard.fileSize))
		require.Nil(t, shard.originalToLocal)
		for _, rowGroupShard := range shard.rowGroupShards {
			require.GreaterOrEqual(t, rowGroupShard.FileIndex, int32(0))
			require.Less(t, int(rowGroupShard.FileIndex), len(shard.fileList))
			file := shard.fileList[rowGroupShard.FileIndex]
			for rowGroupIdx := rowGroupShard.RowGroupStart; rowGroupIdx < rowGroupShard.RowGroupEnd; rowGroupIdx++ {
				seen[fmt.Sprintf("%s:%d", file, rowGroupIdx)] = true
			}
			loads[shard.node.Addr] += rowGroupShard.Bytes
		}
	}
	require.Equal(t, map[string]bool{
		"warehouse/load/part-000.parquet:0": true,
		"warehouse/load/part-000.parquet:1": true,
		"warehouse/load/part-001.parquet:0": true,
		"warehouse/load/part-001.parquet:1": true,
	}, seen)
	require.Equal(t, int64(110), loads["cn1:6001"])
	require.Equal(t, int64(110), loads["cn2:6001"])
}

func TestCompileExternScanParquetRowGroupFanout(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 2}, {Addr: "cn2:6001", Mcpu: 2}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}

	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.S3,
			Filepath: "warehouse/load/big.parquet",
			Format:   tree.PARQUET,
			Tail:     &tree.TailParameter{},
		},
		ExParam: tree.ExParam{
			ExternType: int32(plan.ExternType_LOAD),
			Parallel:   true,
		},
	}
	n := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_LOAD),
			TbColToDataCol: map[string]int32{},
		},
	}
	fileList := []string{"warehouse/load/big.parquet"}
	fileSize := []int64{220}
	rowGroups := []parquetRowGroupMeta{
		{fileIndex: 0, rowGroupIndex: 0, numRows: 10, bytes: 100},
		{fileIndex: 0, rowGroupIndex: 1, numRows: 9, bytes: 90},
		{fileIndex: 0, rowGroupIndex: 2, numRows: 2, bytes: 20},
		{fileIndex: 0, rowGroupIndex: 3, numRows: 1, bytes: 10},
	}

	ss, err := testCompile.compileExternScanParquetRowGroupFanout(n, param, fileList, fileSize, rowGroups, true)
	require.NoError(t, err)
	require.Len(t, ss, 4)
	require.True(t, param.Parallel)

	seen := make(map[int32]bool)
	var totalRows int64
	for _, scope := range ss {
		require.NoError(t, checkScopeWithExpectedList(scope, []vm.OpType{vm.External}))
		require.Equal(t, 1, scope.NodeInfo.Mcpu)
		require.True(t, scope.IsLoad)
		ext, ok := scope.RootOp.(*external.External)
		require.True(t, ok)
		require.False(t, ext.Es.Extern.Parallel)
		require.Equal(t, []string{"warehouse/load/big.parquet"}, ext.Es.FileList)
		require.Equal(t, []int64{int64(220)}, ext.Es.FileSize)
		require.Equal(t, []*pipeline.FileOffset{{Offset: []int64{0, -1}}}, ext.Es.FileOffsetTotal)
		require.NotEmpty(t, ext.Es.ParquetRowGroupShards)
		for _, shard := range ext.Es.ParquetRowGroupShards {
			require.Equal(t, int32(0), shard.FileIndex)
			for rowGroupIdx := shard.RowGroupStart; rowGroupIdx < shard.RowGroupEnd; rowGroupIdx++ {
				seen[rowGroupIdx] = true
			}
			totalRows += shard.NumRows
		}
	}
	require.Equal(t, map[int32]bool{0: true, 1: true, 2: true, 3: true}, seen)
	require.Equal(t, int64(22), totalRows)
}

func TestReadLoadParquetRowGroupMetadataLocalFile(t *testing.T) {
	testCompile := NewMockCompile(t)
	data := writeCompileInt32ParquetWithRowGroups(t, []int32{0, 1, 2, 3, 4, 5}, 2)
	filePath := filepath.Join(t.TempDir(), "rg.parquet")
	require.NoError(t, os.WriteFile(filePath, data, 0o600))

	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE,
			Filepath: filePath,
			Format:   tree.PARQUET,
		},
		ExParam: tree.ExParam{
			Ctx: context.Background(),
		},
	}

	metas, stats, err := testCompile.readLoadParquetRowGroupMetadata(
		param, []string{filePath}, []int64{int64(len(data))})
	require.NoError(t, err)
	require.Len(t, metas, 3)
	t.Logf("parquet footer metadata stats: files=%d row_groups=%d rows=%d bytes=%d read_calls=%d read_bytes=%d duration=%s",
		stats.Files, stats.RowGroups, stats.Rows, stats.Bytes, stats.ReadCalls, stats.ReadBytes, stats.Duration)
	require.Equal(t, parquetFooterStats{
		Files:     1,
		RowGroups: 3,
		Rows:      6,
		Bytes:     int64(len(data)),
		ReadCalls: stats.ReadCalls,
		ReadBytes: stats.ReadBytes,
		Duration:  stats.Duration,
	}, stats)
	require.Positive(t, stats.ReadCalls)
	require.Positive(t, stats.ReadBytes)
	require.Positive(t, stats.Duration)
	for i, meta := range metas {
		require.Equal(t, int32(0), meta.fileIndex)
		require.Equal(t, int32(i), meta.rowGroupIndex)
		require.Equal(t, int64(2), meta.numRows)
		require.Positive(t, meta.bytes)
	}
}

func TestCompileExternScanParquetLoadUsesRowGroupMetadata(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.addr = "cn1:6001"
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	testCompile.proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		if varName == "sql_mode" {
			return "", nil
		}
		return nil, nil
	})

	data := writeCompileInt32ParquetWithRowGroups(t, []int32{0, 1, 2, 3, 4, 5}, 2)
	filePath := filepath.Join(t.TempDir(), "rg.parquet")
	require.NoError(t, os.WriteFile(filePath, data, 0o600))

	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE,
			Filepath: filePath,
			Format:   tree.PARQUET,
			FileSize: int64(len(data)),
			Tail:     &tree.TailParameter{},
		},
		ExParam: tree.ExParam{
			ExternType: int32(plan.ExternType_LOAD),
			Parallel:   true,
		},
	}
	createSQL, err := json.Marshal(param)
	require.NoError(t, err)
	node := &plan.Node{
		Stats: &plan.Stats{Cost: float64(len(data)), Rowsize: 1},
		TableDef: &plan.TableDef{
			Createsql: string(createSQL),
		},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_LOAD),
			TbColToDataCol: map[string]int32{},
		},
	}

	ss, err := testCompile.compileExternScan(node)
	require.NoError(t, err)
	require.Len(t, ss, 3)

	seen := make(map[int32]bool)
	for _, scope := range ss {
		require.NoError(t, checkScopeWithExpectedList(scope, []vm.OpType{vm.External}))
		ext := scope.RootOp.(*external.External)
		require.False(t, ext.Es.Extern.Parallel)
		require.Equal(t, []string{filePath}, ext.Es.FileList)
		require.Equal(t, []*pipeline.FileOffset{{Offset: []int64{0, -1}}}, ext.Es.FileOffsetTotal)
		require.NotEmpty(t, ext.Es.ParquetRowGroupShards)
		for _, shard := range ext.Es.ParquetRowGroupShards {
			require.Equal(t, int32(0), shard.FileIndex)
			for rowGroupIdx := shard.RowGroupStart; rowGroupIdx < shard.RowGroupEnd; rowGroupIdx++ {
				seen[rowGroupIdx] = true
			}
		}
	}
	require.Equal(t, map[int32]bool{0: true, 1: true, 2: true}, seen)
}

func TestCompileExternScanParquetLoadUsesFileFanoutMainPath(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.addr = "cn1:6001"
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	testCompile.proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		if varName == "sql_mode" {
			return "", nil
		}
		return nil, nil
	})

	dir := t.TempDir()
	data0 := writeCompileInt32ParquetWithRowGroups(t, []int32{0, 1}, 10)
	data1 := writeCompileInt32ParquetWithRowGroups(t, []int32{2, 3}, 10)
	file0 := filepath.Join(dir, "part-0.parquet")
	file1 := filepath.Join(dir, "part-1.parquet")
	require.NoError(t, os.WriteFile(file0, data0, 0o600))
	require.NoError(t, os.WriteFile(file1, data1, 0o600))
	pattern := filepath.Join(dir, "part-*.parquet")

	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE,
			Filepath: pattern,
			Format:   tree.PARQUET,
			FileSize: int64(len(data0) + len(data1)),
			Tail:     &tree.TailParameter{},
		},
		ExParam: tree.ExParam{
			ExternType: int32(plan.ExternType_LOAD),
			Parallel:   true,
		},
	}
	createSQL, err := json.Marshal(param)
	require.NoError(t, err)
	node := &plan.Node{
		Stats: &plan.Stats{Cost: float64(len(data0) + len(data1)), Rowsize: 1},
		TableDef: &plan.TableDef{
			Createsql: string(createSQL),
		},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_LOAD),
			TbColToDataCol: map[string]int32{},
		},
	}

	ss, err := testCompile.compileExternScan(node)
	require.NoError(t, err)
	require.Len(t, ss, 2)

	seen := make(map[string]bool)
	for _, scope := range ss {
		require.NoError(t, checkScopeWithExpectedList(scope, []vm.OpType{vm.External}))
		ext := scope.RootOp.(*external.External)
		require.False(t, ext.Es.Extern.Parallel)
		require.Empty(t, ext.Es.ParquetRowGroupShards)
		require.Len(t, ext.Es.FileList, 1)
		require.Len(t, ext.Es.FileOffsetTotal, 1)
		require.Equal(t, []int64{0, -1}, ext.Es.FileOffsetTotal[0].Offset)
		seen[ext.Es.FileList[0]] = true
	}
	require.Equal(t, map[string]bool{file0: true, file1: true}, seen)
}

func writeCompileInt32ParquetWithRowGroups(t *testing.T, values []int32, rowsPerGroup int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(parquet.Int32Type)})
	w := parquet.NewWriter(&buf, schema, parquet.MaxRowsPerRowGroup(rowsPerGroup))
	rows := make([]parquet.Row, len(values))
	for i, value := range values {
		rows[i] = parquet.Row{parquet.Int32Value(value).Level(0, 0, 0)}
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, f.RowGroups(), (len(values)+int(rowsPerGroup)-1)/int(rowsPerGroup))
	return buf.Bytes()
}

func TestCompileBuildSideForBroadcastJoinSkipsEmptyCN(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 2}, {Addr: "cn2:6001", Mcpu: 2}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}

	node := &plan.Node{
		Stats: &plan.Stats{
			HashmapStats: &plan.HashMapStats{},
		},
	}
	buildScope := generateScopeWithRootOperator(testCompile.proc, []vm.OpType{vm.TableScan})
	buildScope.NodeInfo = engine.Node{Addr: "cn1:6001", Mcpu: 1}
	rs := []*Scope{
		generateScopeWithRootOperator(testCompile.proc, []vm.OpType{vm.HashJoin}),
		generateScopeWithRootOperator(testCompile.proc, []vm.OpType{vm.HashJoin}),
		generateScopeWithRootOperator(testCompile.proc, []vm.OpType{vm.HashJoin}),
	}
	for _, scope := range rs {
		scope.NodeInfo = engine.Node{Addr: "cn1:6001", Mcpu: 1}
	}

	out := testCompile.compileBuildSideForBroadcastJoin(node, rs, []*Scope{buildScope})

	require.Same(t, rs[0], out[0])
	require.Len(t, rs[0].PreScopes, 2)
	require.Empty(t, rs[1].PreScopes)
	require.Empty(t, rs[2].PreScopes)
	require.Same(t, buildScope, rs[0].PreScopes[0])
	require.NoError(t, checkScopeWithExpectedList(rs[0].PreScopes[1], []vm.OpType{vm.Merge, vm.HashBuild}))
	require.NoError(t, checkScopeWithExpectedList(buildScope, []vm.OpType{vm.TableScan, vm.Dispatch}))
	dispatchOp, ok := buildScope.RootOp.(*dispatch.Dispatch)
	require.True(t, ok)
	require.Len(t, dispatchOp.LocalRegs, 1)
	require.Empty(t, dispatchOp.RemoteRegs)
}

func TestCompileBuildSideForBroadcastJoinGroupsDuplicateCN(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 2}, {Addr: "cn2:6001", Mcpu: 2}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}

	node := &plan.Node{
		Stats: &plan.Stats{
			HashmapStats: &plan.HashMapStats{},
		},
	}
	buildScope := generateScopeWithRootOperator(testCompile.proc, []vm.OpType{vm.TableScan})
	buildScope.NodeInfo = engine.Node{Addr: "cn1:6001", Mcpu: 1}
	rs := []*Scope{
		generateScopeWithRootOperator(testCompile.proc, []vm.OpType{vm.HashJoin}),
		generateScopeWithRootOperator(testCompile.proc, []vm.OpType{vm.HashJoin}),
	}
	for _, scope := range rs {
		scope.NodeInfo = engine.Node{Addr: "cn1:6001", Mcpu: 1}
	}

	testCompile.compileBuildSideForBroadcastJoin(node, rs, []*Scope{buildScope})

	require.Len(t, rs[0].PreScopes, 2)
	require.Empty(t, rs[1].PreScopes)
	require.Same(t, buildScope, rs[0].PreScopes[0])
	require.NoError(t, checkScopeWithExpectedList(rs[0].PreScopes[1], []vm.OpType{vm.Merge, vm.HashBuild}))
	dispatchOp, ok := buildScope.RootOp.(*dispatch.Dispatch)
	require.True(t, ok)
	require.Len(t, dispatchOp.LocalRegs, 1)
	require.Empty(t, dispatchOp.RemoteRegs)
}

func generateScopeWithRootOperator(proc *process.Process, operatorList []vm.OpType) *Scope {
	simpleFakeArgument := func(id vm.OpType) vm.Operator {
		switch id {
		case vm.Projection:
			return projection.NewArgument()
		case vm.Limit:
			return limit.NewArgument()
		case vm.Connector:
			return connector.NewArgument().WithReg(proc.Reg.MergeReceivers[0])
		case vm.Filter:
			return filter.NewArgument()
		case vm.Dispatch:
			return dispatch.NewArgument()
		case vm.Shuffle:
			return shuffle.NewArgument()
		case vm.TableScan:
			return table_scan.NewArgument()
		case vm.HashJoin:
			arg := hashjoin.NewArgument()
			arg.EqConds = [][]*plan.Expr{nil, nil}
			return arg
		case vm.Merge:
			return merge.NewArgument()
		default:
			panic("unsupported for ut.")
		}
	}

	ret := &Scope{
		Proc: proc,
	}

	for i := 0; i < len(operatorList); i++ {
		ret.setRootOperator(simpleFakeArgument(operatorList[i]))
	}
	return ret
}

func checkScopeWithExpectedList(s *Scope, requiredOperator []vm.OpType) error {
	resultOperators := getReverseList(s.RootOp)

	if len(resultOperators) != len(requiredOperator) {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"required %d operators but get %d", len(requiredOperator), len(resultOperators)))
	}

	for i, expected := range requiredOperator {
		if expected != resultOperators[i] {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"the %dth operator need %d but get %d",
				i+1, expected, resultOperators[i]))
		}
	}

	return nil
}

func getReverseList(rootOp vm.Operator) []vm.OpType {
	return getReverseList2(rootOp, nil)
}

func getReverseList2(rootOp vm.Operator, stack []vm.OpType) []vm.OpType {
	if rootOp == nil {
		return stack
	}
	base := rootOp.GetOperatorBase()
	if base == nil {
		panic("unexpected to get an empty base.")
	}

	if len(base.Children) > 0 {
		stack = getReverseList2(base.GetChildren(0), stack)
	}

	stack = append(stack, rootOp.OpType())
	return stack
}

type fakeStreamSender2 struct {
	morpc.Stream
	number int
}

func (s *fakeStreamSender2) Close(_ bool) error {
	s.number++
	return nil
}

func TestNotifyMessageClean(t *testing.T) {
	proc := testutil.NewProcess(t)

	ff := &fakeStreamSender2{
		number: 0,
	}
	sender := &messageSenderOnClient{
		streamSender: ff,
		safeToClose:  true,
	}
	// no matter error happens or not, clean method should close the sender.
	n1 := notifyMessageResult{
		sender: sender,
		err:    moerr.NewInternalErrorNoCtx("there is an error."),
	}
	n2 := notifyMessageResult{
		sender: sender,
		err:    nil,
	}

	n1.clean(proc)
	require.Equal(t, 1, ff.number)

	n2.clean(proc)
	require.Equal(t, 2, ff.number)
}

func TestSuppressRemoteRunCancelError(t *testing.T) {
	t.Run("suppress query interrupted after proc cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, suppressRemoteRunCancelError(ctx, moerr.NewQueryInterrupted(ctx)))
	})

	t.Run("keep rpc timeout after proc cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := suppressRemoteRunCancelError(ctx, moerr.NewRPCTimeout(ctx))
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrRPCTimeout))
	})

	t.Run("keep query interrupted while proc still active", func(t *testing.T) {
		ctx := context.Background()
		err := suppressRemoteRunCancelError(ctx, moerr.NewQueryInterrupted(ctx))
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted))
	})
}

func TestScopeHoldAnyCannotRemoteOperator(t *testing.T) {
	s0 := &Scope{
		RootOp: &dispatch.Dispatch{RecCTE: false},
	}
	s1 := &Scope{
		RootOp: &dispatch.Dispatch{RecCTE: true},
	}
	s2 := &Scope{
		RootOp: &dispatch.Dispatch{RecCTE: false},
	}
	s0.PreScopes = []*Scope{s1, s2}

	require.NotNil(t, s1.holdAnyCannotRemoteOperator())
	require.NotNil(t, s0.holdAnyCannotRemoteOperator())
	require.Nil(t, s2.holdAnyCannotRemoteOperator())
}

func TestCleanPipelineWitchStartFail(t *testing.T) {
	s := &Scope{
		Proc: testutil.NewProcess(t),
	}
	s.Proc.BuildPipelineContext(context.Background())
	op := connector.NewArgument()
	op.Reg = &process.WaitRegister{
		Ch2: make(chan process.PipelineSignal, 1),
	}
	s.RootOp = op

	cleanPipelineWitchStartFail(s, moerr.NewInternalErrorNoCtx("test cleanPipelineWitchStartFail"), false)

	require.Equal(t, 1, len(op.Reg.Ch2))
	signal := <-op.Reg.Ch2
	_, err := signal.Action()
	require.Error(t, err)
}

func TestScopeGetRelDataError(t *testing.T) {
	// Create a new scope
	s := newScope(Normal)
	s.Proc = testutil.NewProcess(t)
	s.NodeInfo = engine.Node{
		Mcpu: 1,
	}
	s.DataSource = &Source{
		node: &plan.Node{
			Stats: &plan.Stats{},
			ObjRef: &plan.ObjectRef{
				SchemaName: "test_db",
			},
			TableDef: &plan.TableDef{
				Name: "test_table",
			},
		},
	}

	// Create a mock compile with engine
	e, _, _ := testengine.New(defines.AttachAccountId(context.Background(), catalog.System_Account))
	c := NewMockCompile(t)
	c.proc = s.Proc
	c.e = e

	// Test case: error when expanding ranges
	err := s.getRelData(c, nil)
	require.Error(t, err)
}

// mockRelation is a mock Relation that captures the FilterHint passed to BuildReaders
type mockRelationForBloomFilter struct {
	engine.Relation
	capturedHint engine.FilterHint
}

func (m *mockRelationForBloomFilter) BuildReaders(
	ctx context.Context,
	proc any,
	expr *plan.Expr,
	relData engine.RelData,
	num int,
	txnOffset int,
	orderBy bool,
	policy engine.TombstoneApplyPolicy,
	filterHint engine.FilterHint,
) ([]engine.Reader, error) {
	m.capturedHint = filterHint
	return []engine.Reader{}, nil
}

type mockReaderForParallelOrderBy struct {
	orderByCalls int
	orderBy      []*plan.OrderBySpec
}

func (m *mockReaderForParallelOrderBy) Close() error {
	return nil
}

func (m *mockReaderForParallelOrderBy) Read(context.Context, []string, *plan.Expr, *mpool.MPool, *batch.Batch) (bool, error) {
	return true, nil
}

func (m *mockReaderForParallelOrderBy) SetOrderBy(orderBy []*plan.OrderBySpec) {
	m.orderByCalls++
	m.orderBy = orderBy
}

func (m *mockReaderForParallelOrderBy) GetOrderBy() []*plan.OrderBySpec {
	return m.orderBy
}

func (m *mockReaderForParallelOrderBy) SetIndexParam(*plan.IndexReaderParam) {}

func (m *mockReaderForParallelOrderBy) SetFilterZM(objectio.ZoneMap) {}

type mockRelationForParallelOrderBy struct {
	engine.Relation
	readers []engine.Reader
}

func (m *mockRelationForParallelOrderBy) BuildReaders(
	context.Context,
	any,
	*plan.Expr,
	engine.RelData,
	int,
	int,
	bool,
	engine.TombstoneApplyPolicy,
	engine.FilterHint,
) ([]engine.Reader, error) {
	return m.readers, nil
}

func TestBuildReadersBloomFilterHint(t *testing.T) {
	t.Run("BloomFilter set when node is IVFFLAT Entries and context has bloom filter", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expectedBloomFilter := []byte{1, 2, 3, 4, 5}
		ctx := context.WithValue(proc.Ctx, defines.IvfBloomFilter{}, expectedBloomFilter)
		proc.Ctx = ctx

		mockRel := &mockRelationForBloomFilter{}
		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel: mockRel,
				node: &plan.Node{
					TableDef: &plan.TableDef{
						TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
					},
				},
				FilterExpr: nil,
			},
			NodeInfo: engine.Node{
				Mcpu: 1,
			},
			TxnOffset: 0,
		}

		c := NewMockCompile(t)
		c.proc = proc
		// Use MakeFalseExpr to make emptyScan = true, skipping getRelData
		s.DataSource.FilterList = []*plan.Expr{plan2.MakeFalseExpr()}
		s.DataSource.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{}

		readers, err := s.buildReaders(c)
		require.NoError(t, err)
		require.NotNil(t, readers)
		require.Equal(t, expectedBloomFilter, mockRel.capturedHint.BloomFilter)
	})

	t.Run("BloomFilter not set when node is nil", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expectedBloomFilter := []byte{1, 2, 3, 4, 5}
		ctx := context.WithValue(proc.Ctx, defines.IvfBloomFilter{}, expectedBloomFilter)
		proc.Ctx = ctx

		mockRel := &mockRelationForBloomFilter{}
		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel:                mockRel,
				node:               nil, // node is nil
				FilterExpr:         nil,
				FilterList:         []*plan.Expr{},
				RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{},
			},
			NodeInfo: engine.Node{
				Mcpu: 1,
			},
			TxnOffset: 0,
		}

		c := NewMockCompile(t)
		c.proc = proc
		s.DataSource.FilterList = []*plan.Expr{plan2.MakeFalseExpr()}

		readers, err := s.buildReaders(c)
		require.NoError(t, err)
		require.NotNil(t, readers)
		require.Nil(t, mockRel.capturedHint.BloomFilter)
	})

	t.Run("BloomFilter not set when TableDef is nil", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expectedBloomFilter := []byte{1, 2, 3, 4, 5}
		ctx := context.WithValue(proc.Ctx, defines.IvfBloomFilter{}, expectedBloomFilter)
		proc.Ctx = ctx

		mockRel := &mockRelationForBloomFilter{}
		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel: mockRel,
				node: &plan.Node{
					TableDef: nil, // TableDef is nil
				},
				FilterExpr:         nil,
				FilterList:         []*plan.Expr{},
				RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{},
			},
			NodeInfo: engine.Node{
				Mcpu: 1,
			},
			TxnOffset: 0,
		}

		c := NewMockCompile(t)
		c.proc = proc
		s.DataSource.FilterList = []*plan.Expr{plan2.MakeFalseExpr()}

		readers, err := s.buildReaders(c)
		require.NoError(t, err)
		require.NotNil(t, readers)
		require.Nil(t, mockRel.capturedHint.BloomFilter)
	})

	t.Run("BloomFilter not set when TableType is not IVFFLAT Entries", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expectedBloomFilter := []byte{1, 2, 3, 4, 5}
		ctx := context.WithValue(proc.Ctx, defines.IvfBloomFilter{}, expectedBloomFilter)
		proc.Ctx = ctx

		mockRel := &mockRelationForBloomFilter{}
		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel: mockRel,
				node: &plan.Node{
					TableDef: &plan.TableDef{
						TableType: catalog.SystemSI_IVFFLAT_TblType_Metadata, // different type
					},
				},
				FilterExpr:         nil,
				FilterList:         []*plan.Expr{},
				RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{},
			},
			NodeInfo: engine.Node{
				Mcpu: 1,
			},
			TxnOffset: 0,
		}

		c := NewMockCompile(t)
		c.proc = proc
		s.DataSource.FilterList = []*plan.Expr{plan2.MakeFalseExpr()}

		readers, err := s.buildReaders(c)
		require.NoError(t, err)
		require.NotNil(t, readers)
		require.Nil(t, mockRel.capturedHint.BloomFilter)
	})

	t.Run("BloomFilter not set when context has no IvfBloomFilter", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		// No IvfBloomFilter in context

		mockRel := &mockRelationForBloomFilter{}
		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel: mockRel,
				node: &plan.Node{
					TableDef: &plan.TableDef{
						TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
					},
				},
				FilterExpr:         nil,
				FilterList:         []*plan.Expr{},
				RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{},
			},
			NodeInfo: engine.Node{
				Mcpu: 1,
			},
			TxnOffset: 0,
		}

		c := NewMockCompile(t)
		c.proc = proc
		s.DataSource.FilterList = []*plan.Expr{plan2.MakeFalseExpr()}

		readers, err := s.buildReaders(c)
		require.NoError(t, err)
		require.NotNil(t, readers)
		require.Nil(t, mockRel.capturedHint.BloomFilter)
	})

	t.Run("BloomFilter not set when context value is not []byte", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		ctx := context.WithValue(proc.Ctx, defines.IvfBloomFilter{}, "not a byte slice")
		proc.Ctx = ctx

		mockRel := &mockRelationForBloomFilter{}
		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel: mockRel,
				node: &plan.Node{
					TableDef: &plan.TableDef{
						TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
					},
				},
				FilterExpr:         nil,
				FilterList:         []*plan.Expr{},
				RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{},
			},
			NodeInfo: engine.Node{
				Mcpu: 1,
			},
			TxnOffset: 0,
		}

		c := NewMockCompile(t)
		c.proc = proc
		s.DataSource.FilterList = []*plan.Expr{plan2.MakeFalseExpr()}

		readers, err := s.buildReaders(c)
		require.NoError(t, err)
		require.NotNil(t, readers)
		require.Nil(t, mockRel.capturedHint.BloomFilter)
	})

	t.Run("BloomFilter not set when context value is empty []byte", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		ctx := context.WithValue(proc.Ctx, defines.IvfBloomFilter{}, []byte{}) // empty byte slice
		proc.Ctx = ctx

		mockRel := &mockRelationForBloomFilter{}
		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel: mockRel,
				node: &plan.Node{
					TableDef: &plan.TableDef{
						TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
					},
				},
				FilterExpr:         nil,
				FilterList:         []*plan.Expr{},
				RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{},
			},
			NodeInfo: engine.Node{
				Mcpu: 1,
			},
			TxnOffset: 0,
		}

		c := NewMockCompile(t)
		c.proc = proc
		s.DataSource.FilterList = []*plan.Expr{plan2.MakeFalseExpr()}

		readers, err := s.buildReaders(c)
		require.NoError(t, err)
		require.NotNil(t, readers)
		require.Nil(t, mockRel.capturedHint.BloomFilter)
	})
}

func TestBuildScanParallelRunSetsOrderByOnParallelReaders(t *testing.T) {
	c := NewMockCompile(t)
	scope := generateScopeWithRootOperator(c.proc, []vm.OpType{vm.Projection})

	orderBy := []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC}}
	reader1 := &mockReaderForParallelOrderBy{}
	reader2 := &mockReaderForParallelOrderBy{}

	scope.DataSource = &Source{
		Rel:                &mockRelationForParallelOrderBy{readers: []engine.Reader{reader1, reader2}},
		FilterList:         []*plan.Expr{plan2.MakeFalseExpr()},
		FilterExpr:         nil,
		OrderBy:            orderBy,
		RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{},
	}
	scope.NodeInfo = engine.Node{Mcpu: 2}

	mergeScope, err := buildScanParallelRun(scope, c)
	require.NoError(t, err)
	require.NotNil(t, mergeScope)
	require.Len(t, mergeScope.PreScopes, 2)

	for _, reader := range []*mockReaderForParallelOrderBy{reader1, reader2} {
		require.Equal(t, 1, reader.orderByCalls)
		require.Equal(t, orderBy, reader.orderBy)
	}
}
