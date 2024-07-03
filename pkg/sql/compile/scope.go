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

	goruntime "runtime"
	"runtime/debug"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/right"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbpipeline "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/sample"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
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
	for i := range s.Instructions {
		s.Instructions[i].Arg.Release()
		s.Instructions[i].Arg = nil
	}
	reuse.Free[Scope](s, nil)
}

func (s *Scope) Reset(c *Compile) error {
	err := s.resetForReuse(c)
	if err != nil {
		return err
	}
	for _, scope := range s.PreScopes {
		err := scope.Reset(c)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scope) resetForReuse(c *Compile) (err error) {
	if s.Proc != nil {
		newctx, cancel := context.WithCancel(c.proc.Ctx)
		s.Proc.Base = c.proc.Base
		s.Proc.Ctx = newctx
		s.Proc.Cancel = cancel
	}
	for _, ins := range s.Instructions {
		if ins.Op == vm.Output {
			ins.Arg.(*output.Argument).Func = c.fill
		}
	}
	if s.DataSource != nil {
		if s.DataSource.isConst {
			s.DataSource.Bat = nil
		} else {
			s.DataSource.Rel = nil
			s.DataSource.R = nil
		}
	}
	return nil
}

func (s *Scope) initDataSource(c *Compile) (err error) {
	if s.DataSource == nil {
		return nil
	}
	if s.DataSource.isConst {
		if s.DataSource.Bat != nil {
			return
		}

		if len(s.Instructions) > 0 && s.Instructions[0].Op == vm.ValueScan {
			if s.DataSource.node.NodeType == plan.Node_VALUE_SCAN {
				bat, err := constructValueScanBatch(c.proc, s.DataSource.node)
				if err != nil {
					return err
				}
				s.DataSource.Bat = bat
			}
		}
	} else {
		if s.DataSource.Rel != nil {
			return nil
		}
		return c.compileTableScanDataSource(s)
	}
	return nil
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

	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)

	id := uint64(0)
	if s.DataSource.TableDef != nil {
		id = s.DataSource.TableDef.TblId
	}
	p = pipeline.New(id, s.DataSource.Attributes, s.Instructions, s.Reg)
	if s.DataSource.isConst {
		_, err = p.ConstRun(s.DataSource.Bat, s.Proc)
	} else {
		var tag int32
		if s.DataSource.node != nil && len(s.DataSource.node.RecvMsgList) > 0 {
			tag = s.DataSource.node.RecvMsgList[0].MsgTag
		}
		_, err = p.Run(s.DataSource.R, tag, s.Proc)
	}

	select {
	case <-s.Proc.Ctx.Done():
		err = nil
	default:
	}
	return err
}

func (s *Scope) SetContextRecursively(ctx context.Context) {
	if s.Proc == nil {
		return
	}
	newCtx := s.Proc.ResetContextFromParent(ctx)
	for _, scope := range s.PreScopes {
		scope.SetContextRecursively(newCtx)
	}
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
	for i := 0; i < len(s.Instructions); i++ {
		s.Instructions[i].CnAddr = s.NodeInfo.Addr
		s.Instructions[i].OperatorID = cb()
		s.Instructions[i].ParallelID = 0
		s.Instructions[i].MaxParallel = 1
	}

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
			case Parallel:
				preScopeResultReceiveChan <- scope.ParallelRun(c)
			default:
				preScopeResultReceiveChan <- moerr.NewInternalError(c.proc.Ctx, "unexpected scope Magic %d", scope.Magic)
			}
		})
		if errSubmit != nil {
			preScopeResultReceiveChan <- errSubmit
			wg.Done()
		}
	}

	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)
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

	p := pipeline.NewMerge(s.Instructions, s.Reg)
	if _, err := p.MergeRun(s.Proc); err != nil {
		select {
		case <-s.Proc.Ctx.Done():
		default:
			p.Cleanup(s.Proc, true, c.isPrepare, err)
			return err
		}
	}
	p.Cleanup(s.Proc, false, c.isPrepare, nil)

	// receive and check error from pre-scopes and remote scopes.
	preScopeCount := len(s.PreScopes)
	remoteScopeCount := len(s.RemoteReceivRegInfos)
	if remoteScopeCount == 0 {
		for i := 0; i < len(s.PreScopes); i++ {
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
	if !s.canRemote(c, true) || !cnclient.IsCNClientReady() {
		return s.ParallelRun(c)
	}

	runtime.ProcessLevelRuntime().Logger().
		Debug("remote run pipeline",
			zap.String("local-address", c.addr),
			zap.String("remote-address", s.NodeInfo.Addr))

	p := pipeline.New(0, nil, s.Instructions, s.Reg)
	sender, err := s.remoteRun(c)

	runErr := err
	select {
	case <-s.Proc.Ctx.Done():
		// this clean-up action shouldn't be called before context check.
		// because the clean-up action will cancel the context, and error will be suppressed.
		p.Cleanup(s.Proc, err != nil, c.isPrepare, err)
		runErr = nil

	default:
		p.Cleanup(s.Proc, err != nil, c.isPrepare, err)
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
			pipeline.NewMerge(s.Instructions, s.Reg).Cleanup(s.Proc, true, c.isPrepare, err)
		}
	}()

	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)

	switch {
	// probability 1: it's a JOIN pipeline.
	case s.IsJoin:
		parallelScope, err = buildJoinParallelRun(s, c)

	// probability 2: it's a LOAD pipeline.
	case s.IsLoad:
		parallelScope, err = buildLoadParallelRun(s, c)

	// probability 3: it's a SCAN pipeline.
	case s.DataSource != nil:
		parallelScope, err = buildScanParallelRun(s, c)

	// others.
	default:
		parallelScope, err = s, nil
	}

	if err != nil {
		return err
	}

	if parallelScope.Magic == Normal {
		return parallelScope.Run(c)
	}
	return parallelScope.MergeRun(c)
}

// buildJoinParallelRun deal one case of scope.ParallelRun.
// this function will create a pipeline to run a join in parallel.
func buildJoinParallelRun(s *Scope, c *Compile) (*Scope, error) {
	if c.IsTpQuery() {
		//tp query build scope in compile time, not runtime
		return s, nil
	}
	mcpu := s.NodeInfo.Mcpu
	if mcpu <= 1 { // no need to parallel
		buildScope := c.newJoinBuildScope(s, nil)
		s.PreScopes = append(s.PreScopes, buildScope)
		if s.BuildIdx > 1 {
			probeScope := c.newJoinProbeScope(s, nil)
			s.PreScopes = append(s.PreScopes, probeScope)
		}
		return s, nil
	}

	isRight := s.isRight()

	chp := s.PreScopes
	for i := range chp {
		chp[i].IsEnd = true
	}

	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = newScope(Merge)
		ss[i].NodeInfo = s.NodeInfo
		ss[i].Proc = process.NewFromProc(s.Proc, s.Proc.Ctx, 2)
		ss[i].Proc.Reg.MergeReceivers[1].Ch = make(chan *process.RegisterMessage, 10)
	}
	probeScope, buildScope := c.newJoinProbeScope(s, ss), c.newJoinBuildScope(s, ss)

	ns, err := newParallelScope(c, s, ss)
	if err != nil {
		ReleaseScopes(ss)
		return nil, err
	}

	if isRight {
		channel := make(chan *bitmap.Bitmap, mcpu)
		for i := range ns.PreScopes {
			switch arg := ns.PreScopes[i].Instructions[0].Arg.(type) {
			case *right.Argument:
				arg.Channel = channel
				arg.NumCPU = uint64(mcpu)
				if i == 0 {
					arg.IsMerger = true
				}

			case *rightsemi.Argument:
				arg.Channel = channel
				arg.NumCPU = uint64(mcpu)
				if i == 0 {
					arg.IsMerger = true
				}

			case *rightanti.Argument:
				arg.Channel = channel
				arg.NumCPU = uint64(mcpu)
				if i == 0 {
					arg.IsMerger = true
				}
			}
		}
	}
	ns.PreScopes = append(ns.PreScopes, chp...)
	ns.PreScopes = append(ns.PreScopes, buildScope)
	ns.PreScopes = append(ns.PreScopes, probeScope)

	return ns, nil
}

// buildLoadParallelRun deal one case of scope.ParallelRun.
// this function will create a pipeline to load in parallel.
func buildLoadParallelRun(s *Scope, c *Compile) (*Scope, error) {
	mcpu := s.NodeInfo.Mcpu
	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = newScope(Normal)
		ss[i].NodeInfo = s.NodeInfo
		ss[i].DataSource = &Source{
			isConst: true,
		}
		ss[i].Proc = process.NewFromProc(s.Proc, c.proc.Ctx, 0)
		if err := ss[i].initDataSource(c); err != nil {
			return nil, err
		}
	}

	ns, err := newParallelScope(c, s, ss)
	if err != nil {
		ReleaseScopes(ss)
		return nil, err
	}

	return ns, nil
}

// buildScanParallelRun deal one case of scope.ParallelRun.
// this function will create a pipeline which will get data from n scan-pipeline and output it as a while to the outside.
// return true if this was just one scan but not mergeScan.
func buildScanParallelRun(s *Scope, c *Compile) (*Scope, error) {
	// unexpected case.
	if s.IsRemote && len(s.DataSource.OrderBy) > 0 {
		return nil, moerr.NewInternalError(c.proc.Ctx, "ordered scan cannot run in remote.")
	}

	maxProvidedCpuNumber := goruntime.GOMAXPROCS(0)
	if c.IsTpQuery() {
		maxProvidedCpuNumber = 1
	}

	readers, scanUsedCpuNumber, err := s.getReaders(c, maxProvidedCpuNumber)
	if err != nil {
		return nil, err
	}

	// only one scan reader, it can just run without any merge.
	if scanUsedCpuNumber == 1 {
		s.Magic = Normal
		s.DataSource.R = readers[0]
		s.DataSource.R.SetOrderBy(s.DataSource.OrderBy)
		return s, nil
	}

	if len(s.DataSource.OrderBy) > 0 {
		return nil, moerr.NewInternalError(c.proc.Ctx, "ordered scan must run in only one parallel.")
	}

	// return a pipeline which merge result from scanUsedCpuNumber scan.
	readerScopes := make([]*Scope, scanUsedCpuNumber)
	for i := 0; i < scanUsedCpuNumber; i++ {
		readerScopes[i] = newScope(Normal)
		readerScopes[i].NodeInfo = s.NodeInfo
		readerScopes[i].DataSource = &Source{
			R:            readers[i],
			SchemaName:   s.DataSource.SchemaName,
			RelationName: s.DataSource.RelationName,
			Attributes:   s.DataSource.Attributes,
			AccountId:    s.DataSource.AccountId,
			node:         s.DataSource.node,
		}
		readerScopes[i].Proc = process.NewFromProc(s.Proc, c.proc.Ctx, 0)
		readerScopes[i].TxnOffset = s.TxnOffset
	}

	mergeFromParallelScanScope, errNew := newParallelScope(c, s, readerScopes)
	if errNew != nil {
		ReleaseScopes(readerScopes)
		return nil, err
	}
	mergeFromParallelScanScope.SetContextRecursively(s.Proc.Ctx)
	return mergeFromParallelScanScope, nil
}

func DetermineRuntimeDOP(cpunum, blocks int) int {
	if cpunum <= 0 || blocks <= 16 {
		return 1
	}
	ret := blocks/16 + 1
	if ret < cpunum {
		return ret
	}
	return cpunum
}

func (s *Scope) handleRuntimeFilter(c *Compile) error {
	var err error
	var inExprList []*plan.Expr
	exprs := make([]*plan.Expr, 0, len(s.DataSource.RuntimeFilterSpecs))
	filters := make([]process.RuntimeFilterMessage, 0, len(exprs))

	if len(s.DataSource.RuntimeFilterSpecs) > 0 {
		for _, spec := range s.DataSource.RuntimeFilterSpecs {
			msgReceiver := c.proc.NewMessageReceiver([]int32{spec.Tag}, process.AddrBroadCastOnCurrentCN())
			msgs, ctxDone := msgReceiver.ReceiveMessage(true, s.Proc.Ctx)
			if ctxDone {
				return nil
			}
			for i := range msgs {
				msg, ok := msgs[i].(process.RuntimeFilterMessage)
				if !ok {
					panic("expect runtime filter message, receive unknown message!")
				}
				switch msg.Typ {
				case process.RuntimeFilter_PASS:
					continue
				case process.RuntimeFilter_DROP:
					// FIXME: Should give an empty "Data" and then early return
					s.NodeInfo.Data = nil
					s.NodeInfo.NeedExpandRanges = false
					s.DataSource.FilterExpr = plan2.MakeFalseExpr()
					return nil
				case process.RuntimeFilter_IN:
					inExpr := plan2.MakeInExpr(c.proc.Ctx, spec.Expr, msg.Card, msg.Data, spec.MatchPrefix)
					inExprList = append(inExprList, inExpr)

					// TODO: implement BETWEEN expression
				}
				exprs = append(exprs, spec.Expr)
				filters = append(filters, msg)
			}
			msgReceiver.Free()
		}
	}

	for i := range inExprList {
		fn := inExprList[i].GetF()
		col := fn.Args[0].GetCol()
		if col == nil {
			panic("only support col in runtime filter's left child!")
		}

		newExpr := plan2.DeepCopyExpr(inExprList[i])
		//put expr in reader
		newExprList := []*plan.Expr{newExpr}
		if s.DataSource.FilterExpr != nil {
			newExprList = append(newExprList, s.DataSource.FilterExpr)
		}
		s.DataSource.FilterExpr = colexec.RewriteFilterExprList(newExprList)

		isFilterOnPK := s.DataSource.TableDef.Pkey != nil && col.Name == s.DataSource.TableDef.Pkey.PkeyColName
		if !isFilterOnPK {
			// put expr in filter instruction
			idx := 0
			if _, ok := s.Instructions[0].Arg.(*table_scan.Argument); ok {
				idx = 1
			}
			ins := s.Instructions[idx]
			arg, ok := ins.Arg.(*filter.Argument)
			if !ok {
				panic("missing instruction for runtime filter!")
			}
			newExprList := []*plan.Expr{newExpr}
			if arg.E != nil {
				newExprList = append(newExprList, plan2.DeepCopyExpr(arg.E))
			}
			arg.SetExeExpr(colexec.RewriteFilterExprList(newExprList))
		}
	}

	if s.NodeInfo.NeedExpandRanges {
		scanNode := s.DataSource.node
		if scanNode == nil {
			panic("can not expand ranges on remote pipeline!")
		}

		newExprList := plan2.DeepCopyExprList(inExprList)
		if len(s.DataSource.node.BlockFilterList) > 0 {
			newExprList = append(newExprList, s.DataSource.node.BlockFilterList...)
		}

		ranges, err := c.expandRanges(s.DataSource.node, s.DataSource.Rel, newExprList)
		if err != nil {
			return err
		}
		s.NodeInfo.Data = append(s.NodeInfo.Data, ranges.GetAllBytes()...)
		s.NodeInfo.NeedExpandRanges = false

	} else if len(inExprList) > 0 {
		s.NodeInfo.Data, err = ApplyRuntimeFilters(c.proc.Ctx, s.Proc, s.DataSource.TableDef, s.NodeInfo.Data, exprs, filters)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scope) isShuffle() bool {
	// the pipeline is merge->group->xxx
	if s != nil && len(s.Instructions) > 1 && (s.Instructions[1].Op == vm.Group) {
		arg := s.Instructions[1].Arg.(*group.Argument)
		return arg.IsShuffle
	}
	return false
}

func (s *Scope) isRight() bool {
	return s != nil && (s.Instructions[0].Op == vm.Right || s.Instructions[0].Op == vm.RightSemi || s.Instructions[0].Op == vm.RightAnti)
}

func newParallelScope(c *Compile, s *Scope, ss []*Scope) (*Scope, error) {
	var flg bool

	idx := 0
	defer func(ins vm.Instructions) {
		for i := 0; i < idx; i++ {
			if ins[i].Arg != nil {
				ins[i].Arg.Release()
			}
		}
	}(s.Instructions)

	for i, in := range s.Instructions {
		if flg {
			break
		}
		switch in.Op {
		case vm.Top:
			flg = true
			idx = i
			arg := in.Arg.(*top.Argument)
			// release the useless arg
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeTop,
				Idx: in.Idx,
				Arg: mergetop.NewArgument().
					WithFs(arg.Fs).
					WithLimit(arg.Limit),

				CnAddr:      in.CnAddr,
				OperatorID:  c.allocOperatorID(),
				ParallelID:  0,
				MaxParallel: 1,
			}
			for j := range ss {
				newarg := top.NewArgument().WithFs(arg.Fs).WithLimit(arg.Limit)
				newarg.TopValueTag = arg.TopValueTag
				ss[j].appendInstruction(vm.Instruction{
					Op:          vm.Top,
					Idx:         in.Idx,
					IsFirst:     in.IsFirst,
					Arg:         newarg,
					CnAddr:      in.CnAddr,
					OperatorID:  in.OperatorID,
					MaxParallel: int32(len(ss)),
					ParallelID:  int32(j),
				})
			}
			arg.Release()
		// case vm.Order:
		// there is no need to do special merge for order, because the behavior of order is just sort for each batch.
		case vm.Limit:
			flg = true
			idx = i
			arg := in.Arg.(*limit.Argument)
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeLimit,
				Idx: in.Idx,
				Arg: mergelimit.NewArgument().
					WithLimit(arg.LimitExpr),

				CnAddr:      in.CnAddr,
				OperatorID:  c.allocOperatorID(),
				ParallelID:  0,
				MaxParallel: 1,
			}
			for j := range ss {
				ss[j].appendInstruction(vm.Instruction{
					Op:      vm.Limit,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: limit.NewArgument().
						WithLimit(arg.LimitExpr),

					CnAddr:      in.CnAddr,
					OperatorID:  in.OperatorID,
					MaxParallel: int32(len(ss)),
					ParallelID:  int32(j),
				})
			}
			arg.Release()
		case vm.Group:
			flg = true
			idx = i
			arg := in.Arg.(*group.Argument)
			if arg.AnyDistinctAgg() {
				continue
			}
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeGroup,
				Idx: in.Idx,
				Arg: mergegroup.NewArgument().
					WithNeedEval(false),

				CnAddr:      in.CnAddr,
				OperatorID:  c.allocOperatorID(),
				ParallelID:  0,
				MaxParallel: 1,
			}
			for j := range ss {
				ss[j].appendInstruction(vm.Instruction{
					Op:      vm.Group,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: group.NewArgument().
						WithExprs(arg.Exprs).
						WithTypes(arg.Types).
						WithAggsNew(arg.Aggs),

					CnAddr:      in.CnAddr,
					OperatorID:  in.OperatorID,
					MaxParallel: int32(len(ss)),
					ParallelID:  int32(j),
				})
			}
			arg.Release()
		case vm.Sample:
			arg := in.Arg.(*sample.Argument)
			if !arg.IsMergeSampleByRow() {
				flg = true
				idx = i
				// if by percent, there is no need to do merge sample.
				if arg.IsByPercent() {
					s.Instructions = s.Instructions[i:]
				} else {
					s.Instructions = append(make([]vm.Instruction, 1), s.Instructions[i:]...)
					s.Instructions[1] = vm.Instruction{
						Op:      vm.Sample,
						Idx:     in.Idx,
						IsFirst: false,
						Arg:     sample.NewMergeSample(arg, arg.NeedOutputRowSeen),

						CnAddr:      in.CnAddr,
						OperatorID:  c.allocOperatorID(),
						ParallelID:  0,
						MaxParallel: 1,
					}
				}
				s.Instructions[0] = vm.Instruction{
					Op:  vm.Merge,
					Idx: s.Instructions[0].Idx,
					Arg: merge.NewArgument(),

					CnAddr:      in.CnAddr,
					OperatorID:  c.allocOperatorID(),
					ParallelID:  0,
					MaxParallel: 1,
				}

				for j := range ss {
					ss[j].appendInstruction(vm.Instruction{
						Op:      vm.Sample,
						Idx:     in.Idx,
						IsFirst: in.IsFirst,
						Arg:     arg.SimpleDup(),

						CnAddr:      in.CnAddr,
						OperatorID:  in.OperatorID,
						MaxParallel: int32(len(ss)),
						ParallelID:  int32(j),
					})
				}
			}
			arg.Release()
		case vm.Offset:
			flg = true
			idx = i
			arg := in.Arg.(*offset.Argument)
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeOffset,
				Idx: in.Idx,
				Arg: mergeoffset.NewArgument().
					WithOffset(arg.OffsetExpr),

				CnAddr:      in.CnAddr,
				OperatorID:  c.allocOperatorID(),
				ParallelID:  0,
				MaxParallel: 1,
			}
			for j := range ss {
				ss[j].appendInstruction(vm.Instruction{
					Op:      vm.Offset,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: offset.NewArgument().
						WithOffset(arg.OffsetExpr),

					CnAddr:      in.CnAddr,
					OperatorID:  in.OperatorID,
					MaxParallel: int32(len(ss)),
					ParallelID:  int32(j),
				})
			}
			arg.Release()
		case vm.Output:
		default:
			for j := range ss {
				ss[j].appendInstruction(dupInstruction(&in, nil, j))
			}
		}
	}
	if !flg {
		for i := range ss {
			if arg := ss[i].Instructions[len(ss[i].Instructions)-1].Arg; arg != nil {
				arg.Release()
			}
			ss[i].Instructions = ss[i].Instructions[:len(ss[i].Instructions)-1]
		}
		if arg := s.Instructions[0].Arg; arg != nil {
			arg.Release()
		}
		s.Instructions[0] = vm.Instruction{
			Op:  vm.Merge,
			Idx: s.Instructions[0].Idx, // TODO: remove it
			Arg: merge.NewArgument(),

			CnAddr:      s.Instructions[0].CnAddr,
			OperatorID:  c.allocOperatorID(),
			ParallelID:  0,
			MaxParallel: 1,
		}
		// Add log for cn panic which reported on issue 10656
		// If you find this log is printed, please report the repro details
		if len(s.Instructions) < 2 {
			c.proc.Error(c.proc.Ctx, "the length of s.Instructions is too short!"+DebugShowScopes([]*Scope{s}),
				zap.String("stack", string(debug.Stack())),
			)
			return nil, moerr.NewInternalErrorNoCtx("the length of s.Instructions is too short !")
		}
		if len(s.Instructions)-1 != 1 && s.Instructions[1].Arg != nil {
			s.Instructions[1].Arg.Release()
		}
		s.Instructions[1] = s.Instructions[len(s.Instructions)-1]
		for i := 2; i < len(s.Instructions)-1; i++ {
			if arg := s.Instructions[i].Arg; arg != nil {
				arg.Release()
			}
		}
		s.Instructions = s.Instructions[:2]
	}
	s.Magic = Merge
	s.PreScopes = ss
	cnt := 0
	for _, s := range ss {
		if s.IsEnd {
			continue
		}
		cnt++
	}
	s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, cnt)
	{
		for i := 0; i < cnt; i++ {
			s.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Ctx: s.Proc.Ctx,
				Ch:  make(chan *process.RegisterMessage, 1),
			}
		}
	}
	j := 0
	for i := range ss {
		if !ss[i].IsEnd {
			ss[i].appendInstruction(vm.Instruction{
				Op: vm.Connector,
				Arg: connector.NewArgument().
					WithReg(s.Proc.Reg.MergeReceivers[j]),

				CnAddr:      ss[i].Instructions[0].CnAddr,
				OperatorID:  c.allocOperatorID(),
				ParallelID:  0,
				MaxParallel: 1,
			})
			j++
		}
	}
	return s, nil
}

func (s *Scope) appendInstruction(in vm.Instruction) {
	if !s.IsEnd {
		s.Instructions = append(s.Instructions, in)
	}
}

// the result of sendNotifyMessage routine.
// we set sender here because we need to close the sender after canceling the context
// to avoid misreport error (it was possible if there are more than one stream between two compute nodes).
type notifyMessageResult struct {
	sender *messageSenderOnClient
	err    error
}

// sendNotifyMessage create n routines to notify the remote nodes where their receivers are.
// and keep receiving the data until the query was done or data is ended.
func (s *Scope) sendNotifyMessage(wg *sync.WaitGroup, resultChan chan notifyMessageResult) {
	// if context has done, it means the user or other part of the pipeline stops this query.
	closeWithError := func(err error, reg *process.WaitRegister, sender *messageSenderOnClient) {
		if reg != nil {
			select {
			case <-s.Proc.Ctx.Done():
			case reg.Ch <- nil:
			}
		}

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

				sender, err := newMessageSenderOnClient(s.Proc.Ctx, fromAddr, s.Proc.Mp(), nil)
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

				err = receiveMsgAndForward(s.Proc, sender, s.Proc.Reg.MergeReceivers[receiverIdx].Ch)
				closeWithError(err, s.Proc.Reg.MergeReceivers[receiverIdx], sender)
			},
		)

		if errSubmit != nil {
			resultChan <- notifyMessageResult{err: errSubmit, sender: nil}
			wg.Done()
		}
	}
}

func receiveMsgAndForward(proc *process.Process, sender *messageSenderOnClient, forwardCh chan *process.RegisterMessage) error {
	for {
		bat, end, err := sender.receiveBatch()
		if err != nil {
			return err
		}
		if end {
			return nil
		}

		if forwardCh == nil {
			// used for delete.
			// I don't know what is that.
			proc.SetInputBatch(bat)
		} else {
			msg := &process.RegisterMessage{Batch: bat}
			select {
			case <-proc.Ctx.Done():
				bat.Clean(proc.Mp())
				return nil

			case forwardCh <- msg:
			}
		}
	}
}

func (s *Scope) replace(c *Compile) error {
	tblName := s.Plan.GetQuery().Nodes[0].ReplaceCtx.TableDef.Name
	deleteCond := s.Plan.GetQuery().Nodes[0].ReplaceCtx.DeleteCond
	rewriteFromOnDuplicateKey := s.Plan.GetQuery().Nodes[0].ReplaceCtx.RewriteFromOnDuplicateKey

	delAffectedRows := uint64(0)
	if deleteCond != "" {
		result, err := c.runSqlWithResult(fmt.Sprintf("delete from %s where %s", tblName, deleteCond))
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
	result, err := c.runSqlWithResult(sql)
	if err != nil {
		return err
	}
	c.addAffectedRows(result.AffectedRows + delAffectedRows)
	return nil
}

func (s *Scope) getReaders(c *Compile, maxProvidedCpuNumber int) (readers []engine.Reader, scanUsedCpuNumber int, err error) {
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

		// determined how many cpus we should use.
		blkSlice := objectio.BlockInfoSlice(s.NodeInfo.Data)
		scanUsedCpuNumber = DetermineRuntimeDOP(maxProvidedCpuNumber, blkSlice.Len())

		readers, err = c.e.NewBlockReader(
			ctx, scanUsedCpuNumber,
			s.DataSource.Timestamp, s.DataSource.FilterExpr, nil, s.NodeInfo.Data, s.DataSource.TableDef, c.proc)
		if err != nil {
			return
		}

	// Reader can be generated from local relation.
	case s.DataSource.Rel != nil && s.DataSource.TableDef.Partition == nil:
		switch s.DataSource.Rel.GetEngineType() {
		case engine.Disttae:
			blkSlice := objectio.BlockInfoSlice(s.NodeInfo.Data)
			scanUsedCpuNumber = DetermineRuntimeDOP(maxProvidedCpuNumber, blkSlice.Len())
		case engine.Memory:
			idSlice := memoryengine.ShardIdSlice(s.NodeInfo.Data)
			scanUsedCpuNumber = DetermineRuntimeDOP(maxProvidedCpuNumber, idSlice.Len())
		default:
			scanUsedCpuNumber = 1
		}
		if len(s.DataSource.OrderBy) > 0 {
			scanUsedCpuNumber = 1
		}

		readers, err = s.DataSource.Rel.NewReader(c.proc.Ctx,
			scanUsedCpuNumber,
			s.DataSource.FilterExpr,
			s.NodeInfo.Data,
			len(s.DataSource.OrderBy) > 0,
			s.TxnOffset)
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

		switch rel.GetEngineType() {
		case engine.Disttae:
			blkSlice := objectio.BlockInfoSlice(s.NodeInfo.Data)
			scanUsedCpuNumber = DetermineRuntimeDOP(maxProvidedCpuNumber, blkSlice.Len())
		case engine.Memory:
			idSlice := memoryengine.ShardIdSlice(s.NodeInfo.Data)
			scanUsedCpuNumber = DetermineRuntimeDOP(maxProvidedCpuNumber, idSlice.Len())
		default:
			scanUsedCpuNumber = 1
		}
		if len(s.DataSource.OrderBy) > 0 {
			scanUsedCpuNumber = 1
		}

		var mainRds []engine.Reader
		var memRds []engine.Reader
		if rel.GetEngineType() == engine.Memory || s.DataSource.PartitionRelationNames == nil {
			mainRds, err = rel.NewReader(ctx,
				scanUsedCpuNumber,
				s.DataSource.FilterExpr,
				s.NodeInfo.Data,
				len(s.DataSource.OrderBy) > 0,
				s.TxnOffset)
			if err != nil {
				return
			}
			readers = append(readers, mainRds...)
		} else {
			// handle the partition table.
			blkArray := objectio.BlockInfoSlice(s.NodeInfo.Data)
			dirtyRanges := make(map[int]objectio.BlockInfoSlice)
			cleanRanges := make(objectio.BlockInfoSlice, 0, blkArray.Len())
			ranges := objectio.BlockInfoSlice(blkArray.Slice(1, blkArray.Len()))
			for i := 0; i < ranges.Len(); i++ {
				blkInfo := ranges.Get(i)
				if !blkInfo.CanRemote {
					if _, ok := dirtyRanges[blkInfo.PartitionNum]; !ok {
						newRanges := make(objectio.BlockInfoSlice, 0, objectio.BlockInfoSize)
						newRanges = append(newRanges, objectio.EmptyBlockInfoBytes...)
						dirtyRanges[blkInfo.PartitionNum] = newRanges
					}
					dirtyRanges[blkInfo.PartitionNum] = append(dirtyRanges[blkInfo.PartitionNum], ranges.GetBytes(i)...)
					continue
				}
				cleanRanges = append(cleanRanges, ranges.GetBytes(i)...)
			}

			if len(cleanRanges) > 0 {
				// create readers for reading clean blocks from the main table.
				mainRds, err = rel.NewReader(ctx,
					scanUsedCpuNumber,
					s.DataSource.FilterExpr,
					cleanRanges,
					len(s.DataSource.OrderBy) > 0,
					s.TxnOffset)
				if err != nil {
					return
				}
				readers = append(readers, mainRds...)
			}
			// create readers for reading dirty blocks from partition table.
			var subRel engine.Relation
			for num, relName := range s.DataSource.PartitionRelationNames {
				subRel, err = db.Relation(ctx, relName, c.proc)
				if err != nil {
					return
				}
				memRds, err = subRel.NewReader(ctx,
					scanUsedCpuNumber,
					s.DataSource.FilterExpr,
					dirtyRanges[num],
					len(s.DataSource.OrderBy) > 0,
					s.TxnOffset)
				if err != nil {
					return
				}
				readers = append(readers, memRds...)
			}
		}
	}
	// just for quick GC.
	s.NodeInfo.Data = nil

	// need some merge to make sure it is only scanUsedCpuNumber reader.
	// partition table and read from memory will cause len(readers) > scanUsedCpuNumber.
	if len(readers) != scanUsedCpuNumber {
		newReaders := make([]engine.Reader, 0, scanUsedCpuNumber)
		step := len(readers) / scanUsedCpuNumber
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
