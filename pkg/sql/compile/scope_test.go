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
	"path"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/right"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightsemi"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/golang/mock/gomock"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/stretchr/testify/require"

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
	testCompile := NewMockCompile()
	var reg process.WaitRegister
	testCompile.proc.Reg.MergeReceivers = []*process.WaitRegister{&reg}

	// join->Shuffle->Dispatch
	s := generateScopeWithRootOperator(
		testCompile.proc,
		[]vm.OpType{vm.Join, vm.Shuffle, vm.Dispatch})
	s.IsEnd = true
	//join->connector
	s1 := generateScopeWithRootOperator(
		testCompile.proc,
		[]vm.OpType{vm.Join, vm.Connector})
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
		proc := testutil.NewProcess()
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

func TestNewParallelScope(t *testing.T) {
	// function `newParallelScope` will dispatch one scope's work into n scopes.
	testCompile := NewMockCompile()

	var reg process.WaitRegister
	testCompile.proc.Reg.MergeReceivers = []*process.WaitRegister{&reg}

	// 1. test (rightSemi -> projection -> limit -> connector.)
	{
		scopeToParallel := generateScopeWithRootOperator(
			testCompile.proc,
			[]vm.OpType{vm.RightSemi, vm.Projection, vm.Limit, vm.Connector})

		scopeToParallel.NodeInfo.Mcpu = 4
		_, ss := newParallelScope(scopeToParallel)
		require.NoError(t, checkScopeWithExpectedList(ss[0], []vm.OpType{vm.RightSemi, vm.Projection, vm.Limit, vm.Connector}))
		require.NoError(t, checkScopeWithExpectedList(ss[1], []vm.OpType{vm.RightSemi, vm.Projection, vm.Limit, vm.Connector}))
		require.NoError(t, checkScopeWithExpectedList(ss[2], []vm.OpType{vm.RightSemi, vm.Projection, vm.Limit, vm.Connector}))
		require.NoError(t, checkScopeWithExpectedList(ss[3], []vm.OpType{vm.RightSemi, vm.Projection, vm.Limit, vm.Connector}))
	}

	// 2. test (right -> filter -> projection -> connector.)
	{
		scopeToParallel := generateScopeWithRootOperator(
			testCompile.proc,
			[]vm.OpType{vm.Right, vm.Filter, vm.Projection, vm.Connector})

		scopeToParallel.NodeInfo.Mcpu = 4

		_, ss := newParallelScope(scopeToParallel)
		require.NoError(t, checkScopeWithExpectedList(ss[0], []vm.OpType{vm.Right, vm.Filter, vm.Projection, vm.Connector}))
		require.NoError(t, checkScopeWithExpectedList(ss[1], []vm.OpType{vm.Right, vm.Filter, vm.Projection, vm.Connector}))
		require.NoError(t, checkScopeWithExpectedList(ss[2], []vm.OpType{vm.Right, vm.Filter, vm.Projection, vm.Connector}))
		require.NoError(t, checkScopeWithExpectedList(ss[3], []vm.OpType{vm.Right, vm.Filter, vm.Projection, vm.Connector}))
	}

	// 3. test (rightanti -> shuffle  -> dispatch.)
	{
		scopeToParallel := generateScopeWithRootOperator(
			testCompile.proc,
			[]vm.OpType{vm.RightAnti, vm.Shuffle, vm.Dispatch})

		scopeToParallel.NodeInfo.Mcpu = 3

		_, ss := newParallelScope(scopeToParallel)
		require.NoError(t, checkScopeWithExpectedList(ss[0], []vm.OpType{vm.RightAnti, vm.Shuffle, vm.Dispatch}))
		require.NoError(t, checkScopeWithExpectedList(ss[1], []vm.OpType{vm.RightAnti, vm.Shuffle, vm.Dispatch}))
		require.NoError(t, checkScopeWithExpectedList(ss[2], []vm.OpType{vm.RightAnti, vm.Shuffle, vm.Dispatch}))
	}
}

func TestCompileExternValueScan(t *testing.T) {
	testCompile := NewMockCompile()
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
	testCompile := NewMockCompile()
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
	testCompile := NewMockCompile()
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
	filePath := fmt.Sprintf("%s/../../../test/distributed/resources/load_data/parallel_1.txt.gz", GetFilePath())
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
		case vm.Join:
			arg := join.NewArgument()
			arg.Conditions = [][]*plan.Expr{nil, nil}
			return arg
		case vm.Shuffle:
			return shuffle.NewArgument()
		case vm.TableScan:
			return table_scan.NewArgument()
		case vm.RightSemi:
			return rightsemi.NewArgument()
		case vm.RightAnti:
			return rightanti.NewArgument()
		case vm.Right:
			return right.NewArgument()
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

func TestRemoveStringBetween(t *testing.T) {
	cases := []struct {
		input, output string
	}{
		{
			input:  "/* comment */ replace into t1 values (1);",
			output: " replace into t1 values (1);",
		},
		{
			input:  "/* comment */ replace /* replace */ into t1 values (1);",
			output: " replace  into t1 values (1);",
		},
	}

	for _, c := range cases {
		require.Equal(t, c.output, removeStringBetween(c.input, "/*", "*/"))
	}
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
	proc := testutil.NewProcess()

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
		Proc: testutil.NewProcess(),
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
	s.Proc = testutil.NewProcess()
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
	c := NewMockCompile()
	c.proc = s.Proc
	c.e = e

	// Test case: error when expanding ranges
	err := s.getRelData(c, nil)
	require.Error(t, err)
}
