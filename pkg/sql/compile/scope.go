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
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbpipeline "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
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
		s.DataSource.Rel = nil
		s.DataSource.R = nil
	}
	return nil
}

func (s *Scope) initDataSource(c *Compile) (err error) {
	if s.DataSource == nil || s.DataSource.isConst {
		return nil
	}

	if s.DataSource.Rel != nil {
		return nil
	}
	return c.compileTableScanDataSource(s)
}

// Run read data from storage engine and run the instructions of scope.
func (s *Scope) Run(c *Compile) (err error) {
	var p *pipeline.Pipeline
	defer func() {

		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(s.Proc.Ctx, e)
			c.proc.Error(c.proc.Ctx, "panic in scope run",
				zap.String("sql", c.sql),
				zap.String("error", err.Error()))
		}
		if p != nil {
			p.Cleanup(s.Proc, err != nil, c.isPrepare, err)
		}
	}()

	if s.RootOp == nil {
		// it's a fake scope
		return nil
	}

	if s.DataSource == nil {
		p = pipeline.NewMerge(s.RootOp)
		_, err = p.MergeRun(s.Proc)
	} else {
		id := uint64(0)
		if s.DataSource.TableDef != nil {
			id = s.DataSource.TableDef.TblId
		}
		p = pipeline.New(id, s.DataSource.Attributes, s.RootOp)
		if s.DataSource.isConst {
			_, err = p.ConstRun(s.Proc)
		} else {
			if s.DataSource.R == nil {
				s.NodeInfo.Data = engine.BuildEmptyRelData()
				stats := statistic.StatsInfoFromContext(c.proc.GetTopContext())

				buildStart := time.Now()
				readers, err := s.buildReaders(c)
				stats.AddBuidReaderTimeConsumption(time.Since(buildStart))
				if err != nil {
					return err
				}

				s.DataSource.R = readers[0]
				s.DataSource.R.SetOrderBy(s.DataSource.OrderBy)
			}

			var tag int32
			if s.DataSource.node != nil && len(s.DataSource.node.RecvMsgList) > 0 {
				tag = s.DataSource.node.RecvMsgList[0].MsgTag
			}
			_, err = p.Run(s.DataSource.R, tag, s.Proc)
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

// MergeRun range and run the scope's pre-scopes by go-routine, and finally run itself to do merge work.
func (s *Scope) MergeRun(c *Compile) error {
	var wg sync.WaitGroup
	preScopeResultReceiveChan := make(chan error, len(s.PreScopes))
	for i := range s.PreScopes {
		wg.Add(1)
		scope := s.PreScopes[i]
		errSubmit := ants.Submit(func() {
			defer func() {
				wg.Done()
			}()

			switch scope.Magic {
			case Normal:
				preScopeResultReceiveChan <- scope.Run(c)
			case Merge, MergeInsert:
				preScopeResultReceiveChan <- scope.MergeRun(c)
			case Remote:
				preScopeResultReceiveChan <- scope.RemoteRun(c)
			default:
				preScopeResultReceiveChan <- moerr.NewInternalErrorf(c.proc.Ctx, "unexpected scope Magic %d", scope.Magic)
			}
		})
		if errSubmit != nil {
			preScopeResultReceiveChan <- errSubmit
			wg.Done()
		}
	}

	var notifyMessageResultReceiveChan chan notifyMessageResult
	if len(s.RemoteReceivRegInfos) > 0 {
		notifyMessageResultReceiveChan = make(chan notifyMessageResult, len(s.RemoteReceivRegInfos))
		s.sendNotifyMessage(&wg, notifyMessageResultReceiveChan)
	}

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
			if result.sender != nil {
				result.sender.close()
			}
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
			if err := <-preScopeResultReceiveChan; err != nil {
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
			if result.sender != nil {
				result.sender.close()
			}
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

// RemoteRun send the scope to a remote node for execution.
func (s *Scope) RemoteRun(c *Compile) error {
	if !s.canRemote(c, true) {
		return s.MergeRun(c)
	}

	runtime.ServiceRuntime(s.Proc.GetService()).Logger().
		Debug("remote run pipeline",
			zap.String("local-address", c.addr),
			zap.String("remote-address", s.NodeInfo.Addr))

	p := pipeline.New(0, nil, s.RootOp)
	sender, err := s.remoteRun(c)

	runErr := err
	select {
	case <-s.Proc.Ctx.Done():
		// this clean-up action shouldn't be called before context check.
		// because the clean-up action will cancel the context, and error will be suppressed.
		p.CleanRootOperator(s.Proc, err != nil, c.isPrepare, err)
		runErr = nil

	default:
		p.CleanRootOperator(s.Proc, err != nil, c.isPrepare, err)
	}

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
				zap.String("sql", c.sql),
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

	// others.
	default:
		parallelScope, err = s, nil
	}

	if err != nil {
		return err
	}

	if parallelScope == s {
		return parallelScope.Run(c)
	}

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
		stats.AddBuidReaderTimeConsumption(time.Since(buildStart))
	}()
	readers, err := s.buildReaders(c)
	if err != nil {
		return nil, err
	}

	// only one scan reader, it can just run without any merge.
	if s.NodeInfo.Mcpu == 1 {
		s.DataSource.R = readers[0]
		s.DataSource.R.SetOrderBy(s.DataSource.OrderBy)
		return s, nil
	}

	if len(s.DataSource.OrderBy) > 0 {
		return nil, moerr.NewInternalError(c.proc.Ctx, "ordered scan must run in only one parallel.")
	}

	ms, ss := newParallelScope(s)
	for i := range ss {
		ss[i].DataSource = &Source{
			R:            readers[i],
			SchemaName:   s.DataSource.SchemaName,
			RelationName: s.DataSource.RelationName,
			Attributes:   s.DataSource.Attributes,
			AccountId:    s.DataSource.AccountId,
			node:         s.DataSource.node,
		}
	}

	return ms, nil
}

func (s *Scope) handleRuntimeFilter(c *Compile) error {
	var err error
	var inExprList []*plan.Expr
	exprs := make([]*plan.Expr, 0, len(s.DataSource.RuntimeFilterSpecs))
	filters := make([]message.RuntimeFilterMessage, 0, len(exprs))

	if len(s.DataSource.RuntimeFilterSpecs) > 0 {
		for _, spec := range s.DataSource.RuntimeFilterSpecs {
			msgReceiver := message.NewMessageReceiver([]int32{spec.Tag}, message.AddrBroadCastOnCurrentCN(), c.proc.GetMessageBoard())
			msgs, ctxDone, err := msgReceiver.ReceiveMessage(true, s.Proc.Ctx)
			if err != nil {
				return err
			}
			if ctxDone {
				return nil
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
					return nil
				case message.RuntimeFilter_IN:
					inExpr := plan2.MakeInExpr(c.proc.Ctx, spec.Expr, msg.Card, msg.Data, spec.MatchPrefix)
					inExprList = append(inExprList, inExpr)

					// TODO: implement BETWEEN expression
				}
				exprs = append(exprs, spec.Expr)
				filters = append(filters, msg)
			}
		}
	}

	var appendNotPkFilter []*plan.Expr
	for i := range inExprList {
		fn := inExprList[i].GetF()
		col := fn.Args[0].GetCol()
		if col == nil {
			panic("only support col in runtime filter's left child!")
		}
		isFilterOnPK := s.DataSource.TableDef.Pkey != nil && col.Name == s.DataSource.TableDef.Pkey.PkeyColName
		if !isFilterOnPK {
			appendNotPkFilter = append(appendNotPkFilter, plan2.DeepCopyExpr(inExprList[i]))
		}
	}

	// reset filter
	if len(appendNotPkFilter) > 0 {
		// put expr in filter instruction
		op := vm.GetLeafOp(s.RootOp)
		if _, ok := op.(*table_scan.TableScan); ok {
			op = vm.GetLeafOpParent(nil, s.RootOp)
		}
		arg, ok := op.(*filter.Filter)
		if !ok {
			panic("missing instruction for runtime filter!")
		}
		err = arg.SetRuntimeExpr(s.Proc, appendNotPkFilter)
		if err != nil {
			return err
		}
	}

	// reset datasource
	if len(inExprList) > 0 {
		newExprList := plan2.DeepCopyExprList(inExprList)
		if s.DataSource.FilterExpr != nil {
			newExprList = append(newExprList, s.DataSource.FilterExpr)
		}
		s.DataSource.FilterExpr = colexec.RewriteFilterExprList(newExprList)
	}

	if s.NodeInfo.NeedExpandRanges {
		scanNode := s.DataSource.node
		if scanNode == nil {
			panic("can not expand ranges on remote pipeline!")
		}

		for _, e := range s.DataSource.BlockFilterList {
			err = plan2.EvalFoldExpr(s.Proc, e, &c.filterExprExes)
			if err != nil {
				return err
			}
		}

		newExprList := plan2.DeepCopyExprList(inExprList)
		if len(s.DataSource.node.BlockFilterList) > 0 {
			newExprList = append(newExprList, s.DataSource.BlockFilterList...)
		}

		relData, err := c.expandRanges(s.DataSource.node, s.DataSource.Rel, newExprList)
		if err != nil {
			return err
		}
		//FIXME:: Do need to attache tombstones? No, because the scope runs on local CN
		//relData.AttachTombstones()
		s.NodeInfo.Data = relData

	} else if len(inExprList) > 0 {
		s.NodeInfo.Data, err = ApplyRuntimeFilters(c.proc.Ctx, s.Proc, s.DataSource.TableDef, s.NodeInfo.Data, exprs, filters)
		if err != nil {
			return err
		}
	}
	return nil
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

func (s *Scope) ReplaceLeafOp(dstLeafOp vm.Operator) {
	vm.HandleLeafOp(nil, s.RootOp, func(leafOpParent vm.Operator, leafOp vm.Operator) error {
		leafOp.Release()
		if leafOpParent == nil {
			s.RootOp = dstLeafOp
		} else {
			leafOpParent.GetOperatorBase().SetChild(dstLeafOp, 0)
		}
		return nil
	})
}

// sendNotifyMessage create n routines to notify the remote nodes where their receivers are.
// and keep receiving the data until the query was done or data is ended.
func (s *Scope) sendNotifyMessage(wg *sync.WaitGroup, resultChan chan notifyMessageResult) {
	// if context has done, it means the user or other part of the pipeline stops this query.
	closeWithError := func(err error, reg *process.WaitRegister, sender *messageSenderOnClient) {
		reg.Ch2 <- process.NewPipelineSignalToDirectly(nil, s.Proc.Mp())

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
			resultChan <- notifyMessageResult{err: errSubmit, sender: nil}
			wg.Done()
		}
	}
}

func receiveMsgAndForward(sender *messageSenderOnClient, forwardCh chan process.PipelineSignal) error {
	for {
		bat, end, err := sender.receiveBatch()
		if err != nil || end || bat == nil {
			return err
		}

		forwardCh <- process.NewPipelineSignalToDirectly(bat, sender.mp)
	}
}

func (s *Scope) replace(c *Compile) error {
	tblName := s.Plan.GetQuery().Nodes[0].ReplaceCtx.TableDef.Name
	deleteCond := s.Plan.GetQuery().Nodes[0].ReplaceCtx.DeleteCond
	rewriteFromOnDuplicateKey := s.Plan.GetQuery().Nodes[0].ReplaceCtx.RewriteFromOnDuplicateKey

	delAffectedRows := uint64(0)
	if deleteCond != "" {
		result, err := c.runSqlWithResult(fmt.Sprintf("delete from %s where %s", tblName, deleteCond), NoAccountId)
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
		sql = "insert " + c.sql[7:]
	}
	result, err := c.runSqlWithResult(sql, NoAccountId)
	if err != nil {
		return err
	}
	c.addAffectedRows(result.AffectedRows + delAffectedRows)
	return nil
}

func (s *Scope) buildReaders(c *Compile) (readers []engine.Reader, err error) {
	// receive runtime filter and optimized the datasource.
	if err = s.handleRuntimeFilter(c); err != nil {
		return
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
	case s.DataSource.Rel != nil && s.DataSource.TableDef.Partition == nil:

		readers, err = s.DataSource.Rel.BuildReaders(
			c.proc.Ctx,
			c.proc,
			s.DataSource.FilterExpr,
			s.NodeInfo.Data,
			s.NodeInfo.Mcpu,
			s.TxnOffset,
			len(s.DataSource.OrderBy) > 0,
			engine.Policy_CheckAll,
		)

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

		var db engine.Database
		var rel engine.Relation
		// todo:
		//  these following codes were very likely to `compile.go:compileTableScanDataSource `.
		//  I kept the old codes here without any modify. I don't know if there is one `GetRelation(txn, scanNode, scheme, table)`
		{
			n := s.DataSource.node
			txnOp := s.Proc.GetTxnOperator()
			if n.ScanSnapshot != nil && n.ScanSnapshot.TS != nil {
				if !n.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
					n.ScanSnapshot.TS.Less(c.proc.GetTxnOperator().Txn().SnapshotTS) {
					if c.proc.GetCloneTxnOperator() != nil {
						txnOp = c.proc.GetCloneTxnOperator()
					} else {
						txnOp = c.proc.GetTxnOperator().CloneSnapshotOp(*n.ScanSnapshot.TS)
						c.proc.SetCloneTxnOperator(txnOp)
					}

					if n.ScanSnapshot.Tenant != nil {
						ctx = context.WithValue(ctx, defines.TenantIDKey{}, n.ScanSnapshot.Tenant.TenantID)
					}
				}
			}

			db, err = c.e.Database(ctx, s.DataSource.SchemaName, txnOp)
			if err != nil {
				return
			}
			rel, err = db.Relation(ctx, s.DataSource.RelationName, c.proc)
			if err != nil {
				var e error // avoid contamination of error messages
				db, e = c.e.Database(ctx, defines.TEMPORARY_DBNAME, s.Proc.GetTxnOperator())
				if e != nil {
					err = e
					return
				}
				rel, e = db.Relation(ctx, engine.GetTempTableName(s.DataSource.SchemaName, s.DataSource.RelationName), c.proc)
				if e != nil {
					err = e
					return
				}
			}
		}

		var mainRds []engine.Reader
		var subRds []engine.Reader
		if rel.GetEngineType() == engine.Memory || s.DataSource.PartitionRelationNames == nil {
			mainRds, err = s.DataSource.Rel.BuildReaders(
				c.proc.Ctx,
				c.proc,
				s.DataSource.FilterExpr,
				s.NodeInfo.Data,
				s.NodeInfo.Mcpu,
				s.TxnOffset,
				len(s.DataSource.OrderBy) > 0,
				engine.Policy_CheckAll,
			)
			if err != nil {
				return
			}
			readers = append(readers, mainRds...)
		} else {
			var mp map[int16]engine.RelData
			if s.NodeInfo.Data != nil && s.NodeInfo.Data.DataCnt() > 1 {
				mp = s.NodeInfo.Data.GroupByPartitionNum()
			}
			var subRel engine.Relation
			for num, relName := range s.DataSource.PartitionRelationNames {
				subRel, err = db.Relation(ctx, relName, c.proc)
				if err != nil {
					return
				}

				var subBlkList engine.RelData
				if s.NodeInfo.Data == nil || s.NodeInfo.Data.DataCnt() <= 1 {
					//Even if subBlkList is nil,
					//we still need to build reader for sub partition table to read data from memory.
					subBlkList = nil
				} else {
					subBlkList = mp[int16(num)]
				}

				subRds, err = subRel.BuildReaders(
					ctx,
					c.proc,
					s.DataSource.FilterExpr,
					subBlkList,
					s.NodeInfo.Mcpu,
					s.TxnOffset,
					len(s.DataSource.OrderBy) > 0,
					engine.Policy_CheckAll,
				)
				if err != nil {
					return
				}
				readers = append(readers, subRds...)
			}
		}

	}
	// just for quick GC.
	s.NodeInfo.Data = nil

	//for partition table.
	if len(readers) != s.NodeInfo.Mcpu {
		newReaders := make([]engine.Reader, 0, s.NodeInfo.Mcpu)
		step := len(readers) / s.NodeInfo.Mcpu
		for i := 0; i < len(readers); i += step {
			newReaders = append(newReaders, disttae.NewMergeReader(readers[i:i+step]))
		}
		readers = newReaders
	}
	return
}

func (s Scope) TypeName() string {
	return "compile.Scope"
}
