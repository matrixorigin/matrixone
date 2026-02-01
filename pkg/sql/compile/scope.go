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
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbpipeline "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

func newScope(magic magicType) *Scope {
	s := reuse.Alloc[Scope](nil)
	s.Magic = magic
	return s
}

func ReleaseScopes(ss []*Scope) {
	for i := range ss {
		ss[i].release()
	}
}

func (s *Scope) withPlan(pn *plan.Plan) *Scope {
	s.Plan = pn
	return s
}

func (s *Scope) release() {
	if s == nil {
		return
	}
	for i := range s.PreScopes {
		s.PreScopes[i].release()
	}
	vm.HandleAllOp(s.RootOp, func(parentOp vm.Operator, op vm.Operator) error {
		op.Release()
		if parentOp != nil {
			parentOp.GetOperatorBase().SetChild(nil, 0)
		}
		return nil
	})

	reuse.Free[Scope](s, nil)
}

func (s *Scope) Reset(c *Compile) error {
	err := s.resetForReuse(c)
	if err != nil {
		return err
	}
	for _, scope := range s.PreScopes {
		if err = scope.Reset(c); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scope) resetForReuse(c *Compile) (err error) {
	if err = vm.HandleAllOp(s.RootOp, func(parentOp vm.Operator, op vm.Operator) error {
		if op.OpType() == vm.Output {
			op.(*output.Output).Func = c.fill
		}
		return nil
	}); err != nil {
		return err
	}

	if s.DataSource != nil && !s.DataSource.isConst {
		s.DataSource.R = nil
	}

	if s.ScopeAnalyzer != nil {
		s.ScopeAnalyzer.Reset()
	}

	// Reset StarCount cache so next run re-executes StarCount with new snapshot.
	s.StarCountOnly = false
	if s.StarCountMergeGroup != nil {
		s.StarCountMergeGroup.PartialResults = nil
	}

	return nil
}

func (s *Scope) initDataSource(c *Compile) (err error) {
	if s.DataSource == nil || s.DataSource.isConst {
		return nil
	}

	return c.compileTableScanDataSource(s)
}

// Run read data from storage engine and run the instructions of scope.
// Note: The prepare time for executing the `scope`.`Run()` method is very short, and no statistics are done
func (s *Scope) Run(c *Compile) (err error) {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	var p *pipeline.Pipeline
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(s.Proc.Ctx, e)
			c.proc.Error(c.proc.Ctx, "panic in scope run",
				zap.String("sql", commonutil.Abbreviate(c.sql, 500)),
				zap.String("error", err.Error()))
		}
		if p != nil {
			p.Cleanup(s.Proc, err != nil, c.isPrepare, err)
		}
	}()

	if s.RootOp == nil {
		//s.ScopeAnalyzer.Stop()
		// it's a fake scope
		return nil
	}

	if s.DataSource == nil {
		s.ScopeAnalyzer.Stop()
		p = pipeline.NewMerge(s.RootOp)
		_, err = p.Run(s.Proc)
	} else {
		id := uint64(0)
		if s.DataSource.TableDef != nil {
			id = s.DataSource.TableDef.TblId
		}
		p = pipeline.New(id, s.DataSource.Attributes, s.RootOp)
		if s.DataSource.isConst {
			s.ScopeAnalyzer.Stop()
			_, err = p.Run(s.Proc)
		} else {
			if s.DataSource.R == nil {
				s.NodeInfo.Data = readutil.BuildEmptyRelData()
				stats := statistic.StatsInfoFromContext(c.proc.GetTopContext())

				buildStart := time.Now()
				readers, err := s.buildReaders(c)
				stats.AddBuildReaderTimeConsumption(time.Since(buildStart))
				if err != nil {
					return err
				}

				s.DataSource.R = readers[0]
				s.DataSource.R.SetOrderBy(s.DataSource.OrderBy)
				s.DataSource.R.SetIndexParam(s.DataSource.IndexReaderParam)
			}

			var tag int32
			if len(s.DataSource.RecvMsgList) > 0 {
				tag = s.DataSource.RecvMsgList[0].MsgTag
			}
			s.ScopeAnalyzer.Stop()
			_, err = p.RunWithReader(s.DataSource.R, tag, s.Proc)
		}
	}
	select {
	case <-s.Proc.Ctx.Done():
		err = nil
	default:
	}
	return err
}

func (s *Scope) FreeOperator(c *Compile) {
	for _, scope := range s.PreScopes {
		scope.FreeOperator(c)
	}

	vm.HandleAllOp(s.RootOp, func(aprentOp vm.Operator, op vm.Operator) error {
		op.Free(c.proc, false, nil)
		return nil
	})
}

func (s *Scope) InitAllDataSource(c *Compile) error {
	err := s.initDataSource(c)
	if err != nil {
		return err
	}
	for _, scope := range s.PreScopes {
		err := scope.InitAllDataSource(c)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scope) SetOperatorInfoRecursively(cb func() int32) {
	vm.HandleAllOp(s.RootOp, func(parentOp vm.Operator, op vm.Operator) error {
		opBase := op.GetOperatorBase()
		opBase.SetCnAddr(s.NodeInfo.Addr)
		opBase.SetOperatorID(cb())
		opBase.SetParalleID(0)
		opBase.SetMaxParallel(1)
		return nil
	})

	for _, scope := range s.PreScopes {
		scope.SetOperatorInfoRecursively(cb)
	}
}

// MergeRun
// case 1 :
//
//	specific run for Tp query without merge operator.
//
// case 2 :
//
//	normal merge run.
//	1. start n goroutines from pool to run the pre-scope.
//	2. send notify message to remote node for its data producer.
//	3. run itself.
//	4. listen to all running pipelines, once any error occurs, stop the NormalMergeRun asap.
func (s *Scope) MergeRun(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	// specific case.
	if c.IsTpQuery() && !c.hasMergeOp {
		for i := len(s.PreScopes) - 1; i >= 0; i-- {
			err := s.PreScopes[i].MergeRun(c)
			if err != nil {
				return err
			}
		}
		return s.ParallelRun(c)
	}

	// Merge Run normally.
	var wg sync.WaitGroup
	preScopeResultReceiveChan := make(chan error, len(s.PreScopes))

	// step 1.
	for i := range s.PreScopes {
		wg.Add(1)

		scope := s.PreScopes[i]

		submitPreScope := ants.Submit(
			func() {
				defer wg.Done()

				switch scope.Magic {
				case Normal:
					preScopeResultReceiveChan <- scope.Run(c)
				case Merge, MergeInsert:
					preScopeResultReceiveChan <- scope.MergeRun(c)
				case Remote:
					preScopeResultReceiveChan <- scope.RemoteRun(c)
				default:
					err := moerr.NewInternalErrorf(c.proc.Ctx, "unexpected scope Magic %d", scope.Magic)
					cleanPipelineWitchStartFail(scope, err, c.isPrepare)
					preScopeResultReceiveChan <- err
				}
			})

		// build routine failed.
		if submitPreScope != nil {
			wg.Done() // this is necessary, because the submitPreScope may panic.
			cleanPipelineWitchStartFail(scope, submitPreScope, c.isPrepare)
			preScopeResultReceiveChan <- submitPreScope
		}
	}

	// step 2.
	var notifyMessageResultReceiveChan chan notifyMessageResult
	if len(s.RemoteReceivRegInfos) > 0 {
		notifyMessageResultReceiveChan = make(chan notifyMessageResult, len(s.RemoteReceivRegInfos))
		s.sendNotifyMessage(&wg, notifyMessageResultReceiveChan)
	}

	// step 3.
	defer func() {
		// should wait all the notify-message-routine and preScopes done.
		wg.Wait()

		// not necessary, but we still clean the preScope error channel here.
		for len(preScopeResultReceiveChan) > 0 {
			<-preScopeResultReceiveChan
		}

		// clean the notifyMessageResultReceiveChan to make sure all the rpc-sender can be closed.
		for len(notifyMessageResultReceiveChan) > 0 {
			result := <-notifyMessageResultReceiveChan
			result.clean(s.Proc)
		}
	}()

	preScopeCount := len(s.PreScopes)
	remoteScopeCount := len(s.RemoteReceivRegInfos)
	//after parallelRun, prescope count may change. we need to save this before parallelRun

	err := s.ParallelRun(c)
	if err != nil {
		return err
	}

	// receive and check error from pre-scopes and remote scopes.
	if remoteScopeCount == 0 {
		for i := 0; i < preScopeCount; i++ {
			if err = <-preScopeResultReceiveChan; err != nil {
				return err
			}
		}
		return nil
	}

	for {
		select {
		case err := <-preScopeResultReceiveChan:
			if err != nil {
				return err
			}
			preScopeCount--

		case result := <-notifyMessageResultReceiveChan:
			result.clean(s.Proc)
			if result.err != nil {
				return result.err
			}
			remoteScopeCount--
		}

		if preScopeCount == 0 && remoteScopeCount == 0 {
			return nil
		}
	}
}

// cleanPipelineWitchStartFail is used to clean up the pipelines that has failed to start due to a certain reasons.
func cleanPipelineWitchStartFail(sp *Scope, fail error, isPrepare bool) {
	p := pipeline.New(0, nil, sp.RootOp)
	p.Cleanup(sp.Proc, true, isPrepare, fail)
}

// RemoteRun send the scope to a remote node for execution.
func (s *Scope) RemoteRun(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	if s.ipAddrMatch(c.addr) {
		return s.MergeRun(c)
	}
	if err := s.holdAnyCannotRemoteOperator(); err != nil {
		return err
	}

	// In fact, it's not a safe way to convert this pipeline to run at local.
	//
	// Just image this case,
	// pipelineA holds an operator `dispatch d1`, d1 want to send data to pipelineB at node1.
	// for this operator `d1`, it takes a remote-receiver (the pipelineB), and it will call a rpc for sending.
	// but now, the pipelineB is not a remote-receiver but in local, the rpc will fail and the B cannot receive anything.
	// This will cause a hung.
	//
	// we should avoid to generate this format pipeline, or do a suitable conversion for the dispatch operator,
	// or refactor the dispatch operator for no need to know a receiver is local or remote.
	if !checkPipelineStandaloneExecutableAtRemote(s) {
		return s.MergeRun(c)
	}

	runtime.ServiceRuntime(s.Proc.GetService()).Logger().
		Debug("remote run pipeline",
			zap.String("local-address", c.addr),
			zap.String("remote-address", s.NodeInfo.Addr))

	p := pipeline.New(0, nil, s.RootOp)
	sender, err := s.remoteRun(c)

	runErr := err
	if s.Proc.Ctx.Err() != nil {
		runErr = nil
	}
	// this clean-up action shouldn't be called before context check.
	// because the clean-up action will cancel the context, and error will be suppressed.
	p.CleanRootOperator(s.Proc, err != nil, c.isPrepare, err)

	// sender should be closed after cleanup (tell the children-pipeline that query was done).
	if sender != nil {
		sender.close()
	}
	return runErr
}

// ParallelRun run a pipeline in parallel.
func (s *Scope) ParallelRun(c *Compile) (err error) {
	var parallelScope *Scope

	// Warning: It is possible that an error occurs before the pipeline has executed prepare, triggering
	// defer `pipeline.Cleanup()`, and execute `reset()` and `free()`. If the operator analyzer is not
	// instantiated and there is a statistical operation in reset, a null pointer will occur
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(s.Proc.Ctx, e)
			c.proc.Error(c.proc.Ctx, "panic in scope run",
				zap.String("sql", commonutil.Abbreviate(c.sql, 500)),
				zap.String("error", err.Error()))
		}

		// if codes run here, it means some error happens during build the parallel scope.
		// we should do clean work for source-scope to avoid receiver hung.
		if parallelScope == nil {
			pipeline.NewMerge(s.RootOp).Cleanup(s.Proc, true, c.isPrepare, err)
		}
	}()

	switch {
	// probability 1: it's a JOIN pipeline.
	//case s.IsJoin:
	//parallelScope, err = buildJoinParallelRun(s, c)
	//fmt.Println(DebugShowScopes([]*Scope{parallelScope}))

	// probability 2: it's a LOAD pipeline.
	case s.IsLoad:
		parallelScope, err = buildLoadParallelRun(s, c)

	// probability 3: it's a SCAN pipeline.
	case s.isTableScan():
		parallelScope, err = buildScanParallelRun(s, c)
		//fmt.Println("after scan parallel run", DebugShowScopes([]*Scope{parallelScope}, OldLevel))

	// probability 3: src op is tablefunction
	case s.IsTbFunc:
		parallelScope, err = buildLoadParallelRun(s, c)

	// others.
	default:
		parallelScope, err = s, nil
	}

	if err != nil {
		return err
	}

	if parallelScope == s {
		//s.ScopeAnalyzer.Stop()
		return parallelScope.Run(c)
	}

	s.ScopeAnalyzer.Stop()
	setContextForParallelScope(parallelScope, s.Proc.Ctx, s.Proc.Cancel)
	err = parallelScope.MergeRun(c)
	return err
}

// buildLoadParallelRun deal one case of scope.ParallelRun.
// this function will create a pipeline to load in parallel.
func buildLoadParallelRun(s *Scope, c *Compile) (*Scope, error) {
	ms, ss := newParallelScope(s)
	for i := range ss {
		ss[i].DataSource = &Source{
			isConst: true,
		}
		if err := ss[i].initDataSource(c); err != nil {
			ReleaseScopes(ss)
			return nil, err
		}
	}
	return ms, nil
}

// buildScanParallelRun deal one case of scope.ParallelRun.
// this function will create a pipeline which will get data from n scan-pipeline and output it as a while to the outside.
// return true if this was just one scan but not mergeScan.
func buildScanParallelRun(s *Scope, c *Compile) (*Scope, error) {
	// unexpected case.
	if s.IsRemote && len(s.DataSource.OrderBy) > 0 {
		return nil, moerr.NewInternalError(c.proc.Ctx, "ordered scan cannot run in remote.")
	}

	stats := statistic.StatsInfoFromContext(c.proc.GetTopContext())
	buildStart := time.Now()
	defer func() {
		stats.AddBuildReaderTimeConsumption(time.Since(buildStart))
	}()
	readers, err := s.buildReaders(c)
	if err != nil {
		return nil, err
	}
	// only one scan reader, it can just run without any merge.
	if s.NodeInfo.Mcpu == 1 {
		s.DataSource.R = readers[0]
		s.DataSource.R.SetOrderBy(s.DataSource.OrderBy)
		s.DataSource.R.SetIndexParam(s.DataSource.IndexReaderParam)
		return s, nil
	}

	ms, ss := newParallelScope(s)
	for i := range ss {
		recvMsgList := slices.Clone(s.DataSource.RecvMsgList)
		for j := range recvMsgList {
			if recvMsgList[j].MsgType == int32(message.MsgTopValue) {
				recvMsgList[j].MsgTag += int32(i) << 16
			}
		}

		readers[i].SetIndexParam(s.DataSource.IndexReaderParam)

		ss[i].DataSource = &Source{
			R:            readers[i],
			SchemaName:   s.DataSource.SchemaName,
			RelationName: s.DataSource.RelationName,
			Attributes:   s.DataSource.Attributes,
			AccountId:    s.DataSource.AccountId,
			node:         s.DataSource.node,
			RecvMsgList:  recvMsgList,
		}
	}

	return ms, nil
}

func (s *Scope) getRelData(c *Compile, blockExprList []*plan.Expr) error {
	rel, db, ctx, err := c.handleDbRelContext(s.DataSource.node, s.IsRemote)
	if err != nil {
		return err
	}

	if s.NodeInfo.CNCNT == 1 {
		rsp := &engine.RangesShuffleParam{
			Node:  s.DataSource.node,
			CNCNT: s.NodeInfo.CNCNT,
			CNIDX: s.NodeInfo.CNIDX,
			Init:  false,
		}
		s.NodeInfo.Data, err = c.expandRanges(
			s.DataSource.node,
			rel,
			db,
			ctx,
			blockExprList,
			engine.Policy_CollectAllData,
			rsp)
		if err != nil {
			return err
		}
		if _, ok := s.NodeInfo.Data.(*disttae.CombinedRelData); !ok {
			err = s.aggOptimize(c, rel, ctx)
			if err != nil {
				return err
			}
		}
		return nil
	}

	//need to shuffle blocks when cncnt>1
	rsp := &engine.RangesShuffleParam{
		Node:  s.DataSource.node,
		CNCNT: s.NodeInfo.CNCNT,
		CNIDX: s.NodeInfo.CNIDX,
		Init:  false,
	}
	if !s.IsRemote { // this is local CN
		rsp.IsLocalCN = true
	}

	policyForLocal := engine.DataCollectPolicy(engine.Policy_CollectAllData)
	policyForRemote := engine.DataCollectPolicy(engine.Policy_CollectCommittedPersistedData)

	// local
	if !s.IsRemote {
		s.NodeInfo.Data, err = c.expandRanges(
			s.DataSource.node,
			rel,
			db,
			ctx,
			blockExprList,
			policyForLocal,
			rsp,
		)
		return err
	}

	// remote
	var commited engine.RelData
	commited, err = c.expandRanges(
		s.DataSource.node,
		rel,
		db,
		ctx,
		blockExprList,
		policyForRemote,
		rsp,
	)

	if err == nil {
		tombstones := s.NodeInfo.Data.GetTombstones()
		commited.AttachTombstones(tombstones)
		s.NodeInfo.Data = commited
	}

	return err
}

func (s *Scope) waitForRuntimeFilters(c *Compile) ([]*plan.Expr, bool, error) {
	var runtimeInExprList []*plan.Expr

	if len(s.DataSource.RuntimeFilterSpecs) > 0 {
		for _, spec := range s.DataSource.RuntimeFilterSpecs {
			msgReceiver := message.NewMessageReceiver([]int32{spec.Tag}, message.AddrBroadCastOnCurrentCN(), c.proc.GetMessageBoard())
			msgs, ctxDone, err := msgReceiver.ReceiveMessage(true, s.Proc.Ctx)
			if err != nil {
				return nil, false, err
			}
			if ctxDone {
				return nil, false, nil
			}
			for i := range msgs {
				msg, ok := msgs[i].(message.RuntimeFilterMessage)
				if !ok {
					panic("expect runtime filter message, receive unknown message!")
				}
				switch msg.Typ {
				case message.RuntimeFilter_PASS:
					continue
				case message.RuntimeFilter_DROP:
					return nil, true, nil
				case message.RuntimeFilter_IN:
					inExpr := plan2.MakeInExpr(c.proc.Ctx, spec.Expr, msg.Card, msg.Data, spec.MatchPrefix)
					runtimeInExprList = append(runtimeInExprList, inExpr)

					// TODO: implement BETWEEN expression
				}
			}
		}
	}

	return runtimeInExprList, false, nil
}

func (s *Scope) handleRuntimeFilters(c *Compile, runtimeInExprList []*plan.Expr) ([]*plan.Expr, error) {
	var nonPkFilters, pkFilters []*plan.Expr

	rfSpecs := s.DataSource.RuntimeFilterSpecs
	for i := range runtimeInExprList {
		fn := runtimeInExprList[i].GetF()
		col := fn.Args[0].GetCol()
		if col == nil {
			panic("only support col in runtime filter's left child!")
		}
		if rfSpecs[i].NotOnPk {
			nonPkFilters = append(nonPkFilters, runtimeInExprList[i])
		} else {
			pkFilters = append(pkFilters, runtimeInExprList[i])
		}
	}

	// reset filter
	if len(nonPkFilters) > 0 {
		// put expr in filter instruction
		op := vm.GetLeafOp(s.RootOp)
		if _, ok := op.(*table_scan.TableScan); ok {
			op = vm.GetLeafOpParent(nil, s.RootOp)
		}
		arg, ok := op.(*filter.Filter)
		if !ok {
			panic("missing instruction for runtime filter!")
		}
		arg.RuntimeFilterExprs = nonPkFilters
	}

	// reset datasource
	if len(pkFilters) > 0 {
		if s.DataSource.FilterExpr != nil {
			pkFilters = append(pkFilters, s.DataSource.FilterExpr)
		}
		s.DataSource.FilterExpr = colexec.RewriteFilterExprList(pkFilters)
	}

	for _, e := range s.DataSource.BlockFilterList {
		err := plan2.EvalFoldExpr(s.Proc, e, &c.filterExprExes)
		if err != nil {
			return nil, err
		}
	}

	return append(runtimeInExprList, s.DataSource.BlockFilterList...), nil
}

func (s *Scope) isTableScan() bool {
	if s == nil {
		return false
	}
	_, isTableScan := vm.GetLeafOp(s.RootOp).(*table_scan.TableScan)
	return isTableScan
}

func newParallelScope(s *Scope) (*Scope, []*Scope) {
	if s.NodeInfo.Mcpu == 1 {
		return s, nil
	}

	if op, ok := s.RootOp.(*dispatch.Dispatch); ok {
		if len(op.RemoteRegs) > 0 {
			panic("pipeline end with dispatch should have been merged in multi CN!")
		}
	}

	// fake scope is used to merge parallel scopes, and do nothing itself
	rs := newScope(Normal)
	rs.Proc = s.Proc.NewContextChildProc(0)

	parallelScopes := make([]*Scope, s.NodeInfo.Mcpu)
	for i := 0; i < s.NodeInfo.Mcpu; i++ {
		parallelScopes[i] = newScope(Normal)
		parallelScopes[i].NodeInfo = s.NodeInfo
		parallelScopes[i].NodeInfo.Mcpu = 1
		parallelScopes[i].Proc = rs.Proc.NewContextChildProc(0)
		parallelScopes[i].TxnOffset = s.TxnOffset
		parallelScopes[i].setRootOperator(dupOperatorRecursively(s.RootOp, i, s.NodeInfo.Mcpu))
	}

	rs.PreScopes = parallelScopes
	s.PreScopes = append(s.PreScopes, rs)

	// after parallelScope
	// s(fake)
	//   |_ rs(fake)
	//   |     |_ parallelScpes
	//   |
	//   |_ prescopes
	return rs, parallelScopes
}

func (s *Scope) doSetRootOperator(op vm.Operator) {
	if s.RootOp != nil {
		op.AppendChild(s.RootOp)
	}
	s.RootOp = op
}

func (s *Scope) setRootOperator(op vm.Operator) {
	if !s.IsEnd {
		s.doSetRootOperator(op)
	}
}

// the result of sendNotifyMessage routine.
// we set sender here because we need to close the sender after canceling the context
// to avoid misreport error (it was possible if there are more than one stream between two compute nodes).
type notifyMessageResult struct {
	sender *messageSenderOnClient
	err    error
}

// clean do final work for a notifyMessageResult.
func (r *notifyMessageResult) clean(proc *process.Process) {
	if r.sender != nil {
		r.sender.close()
	}
	if r.err != nil {
		proc.Infof(proc.Ctx, "send notify message failed : %s", r.err)
	}
}

// sendNotifyMessage create n routines to notify the remote nodes where their receivers are.
// and keep receiving the data until the query was done or data is ended.
func (s *Scope) sendNotifyMessage(wg *sync.WaitGroup, resultChan chan notifyMessageResult) {
	// if context has done, it means the user or other part of the pipeline stops this query.
	closeWithError := func(err error, reg *process.WaitRegister, sender *messageSenderOnClient) {
		reg.Ch2 <- process.NewPipelineSignalToDirectly(nil, err, s.Proc.Mp())

		select {
		case <-s.Proc.Ctx.Done():
			resultChan <- notifyMessageResult{err: nil, sender: sender}
		default:
			resultChan <- notifyMessageResult{err: err, sender: sender}
		}
		wg.Done()
	}

	// start N goroutines to send notifications to remote nodes.
	// to notify the remote dispatch executor where its remote receivers are.
	// dispatch operator will use this stream connection to send data back.
	//
	// function `cnMessageHandle` at file `remoterunServer.go` will handle the notification.
	for i := range s.RemoteReceivRegInfos {
		wg.Add(1)

		op := &s.RemoteReceivRegInfos[i]
		fromAddr := op.FromAddr
		receiverIdx := op.Idx
		uuid := op.Uuid[:]

		errSubmit := ants.Submit(
			func() {

				sender, err := newMessageSenderOnClient(
					s.Proc.Ctx,
					s.Proc.GetService(),
					fromAddr,
					s.Proc.Mp(),
					nil,
				)
				if err != nil {
					closeWithError(err, s.Proc.Reg.MergeReceivers[receiverIdx], nil)
					return
				}

				message := cnclient.AcquireMessage()
				message.SetID(sender.streamSender.ID())
				message.SetMessageType(pbpipeline.Method_PrepareDoneNotifyMessage)
				message.NeedNotReply = false
				message.Uuid = uuid

				if errSend := sender.streamSender.Send(sender.ctx, message); errSend != nil {
					closeWithError(errSend, s.Proc.Reg.MergeReceivers[receiverIdx], sender)
					return
				}
				sender.safeToClose = false
				sender.alreadyClose = false

				err = receiveMsgAndForward(sender, s.Proc.Reg.MergeReceivers[receiverIdx].Ch2)
				closeWithError(err, s.Proc.Reg.MergeReceivers[receiverIdx], sender)
			},
		)

		if errSubmit != nil {
			closeWithError(errSubmit, s.Proc.Reg.MergeReceivers[receiverIdx], nil)
		}
	}
}

func receiveMsgAndForward(sender *messageSenderOnClient, forwardCh chan process.PipelineSignal) error {
	for {
		bat, end, err := sender.receiveBatch()
		if err != nil || end || bat == nil {
			return err
		}

		forwardCh <- process.NewPipelineSignalToDirectly(bat, nil, sender.mp)
	}
}

func (s *Scope) replace(c *Compile) error {
	dbName := s.Plan.GetQuery().Nodes[0].ReplaceCtx.TableDef.DbName
	tblName := s.Plan.GetQuery().Nodes[0].ReplaceCtx.TableDef.Name
	deleteCond := s.Plan.GetQuery().Nodes[0].ReplaceCtx.DeleteCond
	rewriteFromOnDuplicateKey := s.Plan.GetQuery().Nodes[0].ReplaceCtx.RewriteFromOnDuplicateKey

	delAffectedRows := uint64(0)
	if deleteCond != "" {
		result, err := c.runSqlWithResult(fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s", dbName, tblName, deleteCond), NoAccountId)
		if err != nil {
			return err
		}
		delAffectedRows = result.AffectedRows
	}
	var sql string
	if rewriteFromOnDuplicateKey {
		idx := strings.Index(strings.ToLower(c.sql), "on duplicate key update")
		sql = c.sql[:idx]
	} else {
		removed := removeStringBetween(c.sql, "/*", "*/")
		sql = "insert " + strings.TrimSpace(removed)[7:]
	}
	result, err := c.runSqlWithResult(sql, NoAccountId)
	if err != nil {
		return err
	}
	c.addAffectedRows(result.AffectedRows + delAffectedRows)
	return nil
}

func removeStringBetween(s, start, end string) string {
	startIndex := strings.Index(s, start)
	for startIndex != -1 {
		endIndex := strings.Index(s, end)
		if endIndex == -1 || startIndex > endIndex {
			return s
		}

		s = s[:startIndex] + s[endIndex+len(end):]
		startIndex = strings.Index(s, start)
	}
	return s
}

// defaultStarCountTombstoneThreshold: when estimated tombstone rows exceed this, skip StarCount
// and use per-block aggOptimize (only scan blocks with tombstones). Avoids CollectTombstoneStats
// Merge path taking seconds for large tombstone sets.
const defaultStarCountTombstoneThreshold = 5000000

// isSingleStarCountNoFilterNoGroupBy returns true if node has exactly one agg and it is starcount, with no filter and no groupby.
func isSingleStarCountNoFilterNoGroupBy(node *plan.Node) bool {
	if node == nil || len(node.AggList) != 1 || len(node.FilterList) != 0 || len(node.GroupBy) != 0 {
		return false
	}
	agg, ok := node.AggList[0].Expr.(*plan.Expr_F)
	return ok && agg.F.Func.ObjName == "starcount"
}

func (s *Scope) aggOptimize(c *Compile, rel engine.Relation, ctx context.Context) error {
	node := s.DataSource.node
	if node != nil && len(node.AggList) > 0 {
		// Fast path: single starcount without filter/groupby — only call rel.StarCount(), no scan.
		if isSingleStarCountNoFilterNoGroupBy(node) {
			estimatedTombstoneRows, err := rel.EstimateCommittedTombstoneCount(ctx)
			if err != nil {
				return err
			}
			metricv2.StarcountEstimateTombstoneRowsHistogram.Observe(float64(estimatedTombstoneRows))

			if estimatedTombstoneRows > defaultStarCountTombstoneThreshold {
				metricv2.StarcountPathPerBlockCounter.Inc()
				// Fall through to per-block aggOptimize (only scan blocks with tombstones)
			} else {
				metricv2.StarcountPathFastCounter.Inc()
				fastStart := time.Now()
				totalRows, err := rel.StarCount(ctx)
				if err != nil {
					return err
				}
				metricv2.StarcountDurationHistogram.Observe(time.Since(fastStart).Seconds())
				metricv2.StarcountResultRowsHistogram.Observe(float64(totalRows))

				newRelData := s.NodeInfo.Data.BuildEmptyRelData(0)
				s.NodeInfo.Data = newRelData
				partialResults := []any{int64(totalRows)}
				partialResultTypes := []types.T{types.T_int64}
				mergeGroup := findMergeGroup(s.RootOp)
				if mergeGroup != nil {
					mergeGroup.PartialResults = partialResults
					mergeGroup.PartialResultTypes = partialResultTypes
					s.StarCountMergeGroup = mergeGroup
				} else {
					panic("can't find merge group operator for agg optimize!")
				}
				s.StarCountOnly = true
				return nil
			}
		}

		partialResults, partialResultTypes, columnMap := checkAggOptimize(node)
		if partialResults != nil && s.NodeInfo.Data.DataCnt() > 1 {
			//append first empty block
			newRelData := s.NodeInfo.Data.BuildEmptyRelData(1)
			newRelData.AppendBlockInfo(&objectio.EmptyBlockInfo)
			//For each blockinfo in relData, if blk has no tombstones, then compute the agg result,
			//otherwise put it into newRelData.
			var (
				hasTombstone bool
				err2         error
			)

			fs, err := colexec.GetSharedFSFromProc(c.proc)
			if err != nil {
				return err
			}

			tombstones, err := collectTombstones(c, node, rel, engine.Policy_CollectAllTombstones)
			if err != nil {
				return err
			}
			if err = engine.ForRangeBlockInfo(1, s.NodeInfo.Data.DataCnt(), s.NodeInfo.Data, func(blk *objectio.BlockInfo) (bool, error) {
				if hasTombstone, err2 = tombstones.HasBlockTombstone(
					ctx, &blk.BlockID, fs,
				); err2 != nil {
					return false, err2
				} else if blk.IsAppendable() || hasTombstone {
					newRelData.AppendBlockInfo(blk)
					return true, nil
				}
				if c.evalAggOptimize(node, blk, partialResults, partialResultTypes, columnMap) != nil {
					partialResults = nil
					return false, nil
				}
				return true, nil
			}); err != nil {
				return err
			}

			if partialResults != nil {
				s.NodeInfo.Data = newRelData
				//find the last mergegroup
				mergeGroup := findMergeGroup(s.RootOp)
				if mergeGroup != nil {
					mergeGroup.PartialResults = partialResults
					mergeGroup.PartialResultTypes = partialResultTypes
				} else {
					panic("can't find merge group operator for agg optimize!")
				}
			}
		}
	}
	return nil
}

// find scan->group->mergegroup
func findMergeGroup(op vm.Operator) *group.MergeGroup {
	if op == nil {
		return nil
	}
	base := op.GetOperatorBase()
	if base == nil || base.NumChildren() == 0 {
		return nil
	}
	if mergeGroup, ok := op.(*group.MergeGroup); ok {
		child := base.GetChildren(0)
		if _, ok = child.(*group.Group); ok {
			childBase := child.GetOperatorBase()
			if childBase != nil && childBase.NumChildren() > 0 {
				child = childBase.GetChildren(0)
				if _, ok = child.(*table_scan.TableScan); ok {
					return mergeGroup
				}
			}
		}
	}
	return findMergeGroup(base.GetChildren(0))
}

func (s *Scope) buildReaders(c *Compile) (readers []engine.Reader, err error) {
	// StarCount-only path: aggOptimize already called rel.StarCount() and set PartialResults.
	// Return EmptyReaders so no data flows; MergeGroup will use PartialResults only.
	if s.StarCountOnly {
		readers = make([]engine.Reader, s.NodeInfo.Mcpu)
		for i := range readers {
			readers[i] = new(readutil.EmptyReader)
		}
		return readers, nil
	}

	// receive runtime filter and optimize the datasource.
	var runtimeFilterList, blockFilterList []*plan.Expr
	var emptyScan bool
	runtimeFilterList, emptyScan, err = s.waitForRuntimeFilters(c)
	if err != nil {
		return
	}
	for i := range s.DataSource.FilterList {
		if plan2.IsFalseExpr(s.DataSource.FilterList[i]) {
			emptyScan = true
			break
		}
	}
	if !emptyScan {
		blockFilterList, err = s.handleRuntimeFilters(c, runtimeFilterList)
		if err != nil {
			return
		}
		err = s.getRelData(c, blockFilterList)
		if err != nil {
			return
		}
	}

	switch {
	// If this was a remote-run pipeline. Reader should be generated from Engine.
	case s.IsRemote:
		// this cannot use c.proc.Ctx directly, please refer to `default case`.
		ctx := c.proc.Ctx
		if util.TableIsClusterTable(s.DataSource.TableDef.GetTableType()) {
			ctx = defines.AttachAccountId(ctx, catalog.System_Account)
		}
		if s.DataSource.AccountId != nil {
			ctx = defines.AttachAccountId(ctx, uint32(s.DataSource.AccountId.GetTenantId()))
		}

		readers, err = c.e.BuildBlockReaders(
			ctx,
			c.proc,
			s.DataSource.Timestamp,
			s.DataSource.FilterExpr,
			s.DataSource.TableDef,
			s.NodeInfo.Data,
			s.NodeInfo.Mcpu)
		if err != nil {
			return
		}
	// Reader can be generated from local relation.
	case s.DataSource.Rel != nil:
		ctx := c.proc.Ctx
		stats := statistic.StatsInfoFromContext(ctx)
		crs := new(perfcounter.CounterSet)
		newCtx := perfcounter.AttachS3RequestKey(ctx, crs)

		hint := engine.FilterHint{}
		// Pass runtime BloomFilter to reader via FilterHint (only for ivf entries table).
		if n := s.DataSource.node; n != nil && n.TableDef != nil &&
			n.TableDef.TableType == catalog.SystemSI_IVFFLAT_TblType_Entries {
			if len(s.DataSource.BloomFilter) > 0 {
				hint.BloomFilter = s.DataSource.BloomFilter
			} else if bfVal := c.proc.Ctx.Value(defines.IvfBloomFilter{}); bfVal != nil {
				if bf, ok := bfVal.([]byte); ok && len(bf) > 0 {
					hint.BloomFilter = bf
				}
			}
		}

		readers, err = s.DataSource.Rel.BuildReaders(
			newCtx,
			c.proc,
			s.DataSource.FilterExpr,
			s.NodeInfo.Data,
			s.NodeInfo.Mcpu,
			s.TxnOffset,
			len(s.DataSource.OrderBy) > 0,
			engine.Policy_CheckAll,
			hint,
		)

		stats.AddScopePrepareS3Request(statistic.S3Request{
			List:      crs.FileService.S3.List.Load(),
			Head:      crs.FileService.S3.Head.Load(),
			Put:       crs.FileService.S3.Put.Load(),
			Get:       crs.FileService.S3.Get.Load(),
			Delete:    crs.FileService.S3.Delete.Load(),
			DeleteMul: crs.FileService.S3.DeleteMulti.Load(),
		})

		if err != nil {
			return
		}

	// Should get relation first to generate Reader.
	// FIXME:: s.NodeInfo.Rel == nil, partition table? -- this is an old comment, I just do a copy here.
	default:
		// This cannot modify the c.proc.Ctx here, but I don't know why.
		// Maybe there are some account related things stores in the context (using the context.WithValue),
		// and modify action will change the account.
		ctx := c.proc.Ctx

		if util.TableIsClusterTable(s.DataSource.TableDef.GetTableType()) {
			ctx = defines.AttachAccountId(ctx, catalog.System_Account)
		}

		// todo:
		//  these following codes were very likely to `compile.go:compileTableScanDataSource `.
		//  I kept the old codes here without any modify. I don't know if there is one `GetRelation(txn, scanNode, scheme, table)`
		{
			n := s.DataSource.node
			if n.ScanSnapshot != nil && n.ScanSnapshot.TS != nil {
				if !n.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
					n.ScanSnapshot.TS.Less(c.proc.GetTxnOperator().Txn().SnapshotTS) {
					if c.proc.GetCloneTxnOperator() == nil {
						txnOp := c.proc.GetTxnOperator().CloneSnapshotOp(*n.ScanSnapshot.TS)
						c.proc.SetCloneTxnOperator(txnOp)
					}

					if n.ScanSnapshot.Tenant != nil {
						ctx = context.WithValue(ctx, defines.TenantIDKey{}, n.ScanSnapshot.Tenant.TenantID)
					}
				}
			}
		}

		var mainRds []engine.Reader

		stats := statistic.StatsInfoFromContext(ctx)
		crs := new(perfcounter.CounterSet)
		newCtx := perfcounter.AttachS3RequestKey(ctx, crs)

		hint := engine.FilterHint{}
		if n := s.DataSource.node; n != nil && n.TableDef != nil &&
			n.TableDef.TableType == catalog.SystemSI_IVFFLAT_TblType_Entries {
			if len(s.DataSource.BloomFilter) > 0 {
				hint.BloomFilter = s.DataSource.BloomFilter
			} else if bfVal := c.proc.Ctx.Value(defines.IvfBloomFilter{}); bfVal != nil {
				if bf, ok := bfVal.([]byte); ok && len(bf) > 0 {
					hint.BloomFilter = bf
				}
			}
		}

		mainRds, err = s.DataSource.Rel.BuildReaders(
			newCtx,
			c.proc,
			s.DataSource.FilterExpr,
			s.NodeInfo.Data,
			s.NodeInfo.Mcpu,
			s.TxnOffset,
			len(s.DataSource.OrderBy) > 0,
			engine.Policy_CheckAll,
			hint,
		)
		if err != nil {
			return
		}
		readers = append(readers, mainRds...)

		stats.AddScopePrepareS3Request(statistic.S3Request{
			List:      crs.FileService.S3.List.Load(),
			Head:      crs.FileService.S3.Head.Load(),
			Put:       crs.FileService.S3.Put.Load(),
			Get:       crs.FileService.S3.Get.Load(),
			Delete:    crs.FileService.S3.Delete.Load(),
			DeleteMul: crs.FileService.S3.DeleteMulti.Load(),
		})

	}
	// just for quick GC.
	s.NodeInfo.Data = nil

	//for partition table.
	if len(readers) != s.NodeInfo.Mcpu {
		newReaders := make([]engine.Reader, 0, s.NodeInfo.Mcpu)
		step := len(readers) / s.NodeInfo.Mcpu
		for i := 0; i < len(readers); i += step {
			newReaders = append(newReaders, readutil.NewMergeReader(readers[i:i+step]))
		}
		readers = newReaders
	}
	return
}

func (s Scope) TypeName() string {
	return "compile.Scope"
}
