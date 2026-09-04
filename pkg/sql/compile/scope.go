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
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbpipeline "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorscan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/window"
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
	rejectZeroTemporal, err := util.RejectZeroTemporalWritePolicy(c.proc)
	if err != nil {
		return err
	}
	return s.reset(c, rejectZeroTemporal)
}

func (s *Scope) reset(c *Compile, rejectZeroTemporal bool) error {
	s.releaseParallelGenerations(c)
	if err := refreshZeroTemporalWritePolicy(s.RootOp, rejectZeroTemporal); err != nil {
		return err
	}
	err := s.resetForReuse(c)
	if err != nil {
		return err
	}
	for _, scope := range s.PreScopes {
		if err = scope.reset(c, rejectZeroTemporal); err != nil {
			return err
		}
	}
	return nil
}

// releaseParallelGenerations closes the ownership interval that starts in
// newParallelScope. The trees must remain reachable through PreScopes until
// AnalyzeExecPlan has consumed their physical shape and runtime statistics,
// so the next Compile.Reset is the first safe generation boundary.
func (s *Scope) releaseParallelGenerations(c *Compile) {
	if s == nil || len(s.parallelGenerations) == 0 {
		return
	}
	generations := s.parallelGenerations
	s.parallelGenerations = nil
	for _, generation := range generations {
		if generation == nil {
			continue
		}
		if idx := slices.Index(s.PreScopes, generation); idx >= 0 {
			s.PreScopes = slices.Delete(s.PreScopes, idx, idx+1)
		}
		// Prepared pipeline cleanup intentionally Reset-only. These clones are
		// execution-local rather than reusable templates, so finish their
		// physical ownership before returning them to the reuse pools.
		if c != nil && c.isPrepare {
			generation.freeOperatorsWithOwnProcess()
		}
		generation.release()
	}
}

func (s *Scope) freeOperatorsWithOwnProcess() {
	if s == nil {
		return
	}
	for _, preScope := range s.PreScopes {
		preScope.freeOperatorsWithOwnProcess()
	}
	if s.Proc == nil {
		return
	}
	_ = vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
		op.Free(s.Proc, false, nil)
		return nil
	})
}

// discardParallelGeneration handles construction failure before a generated
// tree can contribute execution statistics. It removes both ownership links
// and releases the complete tree exactly once.
func (s *Scope) discardParallelGeneration(generation *Scope) {
	if s == nil || generation == nil {
		return
	}
	if idx := slices.Index(s.PreScopes, generation); idx >= 0 {
		s.PreScopes = slices.Delete(s.PreScopes, idx, idx+1)
	}
	if idx := slices.Index(s.parallelGenerations, generation); idx >= 0 {
		s.parallelGenerations = slices.Delete(s.parallelGenerations, idx, idx+1)
	}
	generation.release()
}

type zeroTemporalWritePolicySetter interface {
	SetRejectZeroTemporal(bool)
}

func refreshZeroTemporalWritePolicy(root vm.Operator, reject bool) error {
	if root == nil {
		return nil
	}
	return vm.HandleAllOp(root, func(_ vm.Operator, op vm.Operator) error {
		if setter, ok := op.(zeroTemporalWritePolicySetter); ok {
			setter.SetRejectZeroTemporal(reject)
		}
		return nil
	})
}

func refreshGroupConcatMaxLen(scopes []*Scope, proc *process.Process) error {
	var maxLen uint64
	resolved := false
	visited := make(map[*Scope]struct{})

	var refreshScope func(*Scope) error
	refreshScope = func(scope *Scope) error {
		if scope == nil {
			return nil
		}
		if _, ok := visited[scope]; ok {
			return nil
		}
		visited[scope] = struct{}{}

		if err := vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			var aggs []aggexec.AggFuncExecExpression
			switch arg := op.(type) {
			case *group.Group:
				aggs = arg.Aggs
			case *group.MergeGroup:
				aggs = arg.Aggs
			case *window.Window:
				aggs = arg.Aggs
			}

			for i := range aggs {
				if aggs[i].GetAggID() != aggexec.AggIdOfGroupConcat {
					continue
				}
				if !resolved {
					value, err := resolveVariableOrDefault(proc, "group_concat_max_len", true, false)
					if err != nil {
						return err
					}
					sessionMaxLen, ok := value.(int64)
					if !ok || sessionMaxLen < 0 {
						return moerr.NewInternalErrorNoCtxf(
							"group_concat_max_len has invalid value %v", value)
					}
					maxLen = uint64(sessionMaxLen)
					resolved = true
				}
				aggs[i].SetExtraConfig(aggexec.RefreshGroupConcatConfigMaxLen(
					aggs[i].GetExtraConfig(), maxLen))
			}
			return nil
		}); err != nil {
			return err
		}

		for _, preScope := range scope.PreScopes {
			if err := refreshScope(preScope); err != nil {
				return err
			}
		}
		return nil
	}

	for _, scope := range scopes {
		if err := refreshScope(scope); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scope) resetForReuse(c *Compile) (err error) {
	s.resourceExecutedLocally = false

	if err = vm.HandleAllOp(s.RootOp, func(parentOp vm.Operator, op vm.Operator) error {
		if op.OpType() == vm.Output {
			op.(*output.Output).Func = c.resultWriter()
		}
		return nil
	}); err != nil {
		return err
	}

	if s.DataSource != nil && !s.DataSource.isConst {
		s.DataSource.R = nil
	}

	// The previous execution's cleanup delivered terminal signals into this
	// scope's pipeline edges and marked them done (doneClosed/endRecorded).
	// A done edge silently rejects both data and End signals, so a reused
	// pipeline would leave its receivers waiting forever. Clear the terminal
	// state so the edges can carry the next execution's signals.
	// See: https://github.com/matrixorigin/matrixone/issues/25614
	if s.Proc != nil {
		s.Proc.CopyPlanSnapshotFrom(c.proc)
		s.Proc.CopyStringShuffleHashAlgorithmFrom(c.proc)
		for _, reg := range s.Proc.Reg.MergeReceivers {
			reg.ResetTerminalStateForReuse()
		}
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

	if s.DataSource.node != nil && s.DataSource.node.NodeType == plan.Node_VECTOR_INDEX_SCAN {
		return c.compileVectorIndexScanDataSource(s)
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
	err, _ = normalizeScopeRunError(err, s.Proc.Ctx, scopeRunQueryContext(s.Proc))
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
type sequentialBranchStarter interface {
	SetBranchStarter(func(int) error)
	ClearBranchStarter()
}

type receiverWaitStartFailureDisabler interface {
	DisableReceiverWaitForStartFailure(*process.Process)
}

func cleanLazyScopeStartFailure(s *Scope, c *Compile, err error) {
	_ = vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
		if disabler, ok := op.(receiverWaitStartFailureDisabler); ok {
			disabler.DisableReceiverWaitForStartFailure(s.Proc)
		}
		return nil
	})
	cleanScopeTreeWithStartFail(s, err, c.isPrepare)
}

func installSequentialBranchStarter(root vm.Operator, start func(int) error) (func(), error) {
	var target sequentialBranchStarter
	err := vm.HandleAllOp(root, func(_ vm.Operator, op vm.Operator) error {
		candidate, ok := op.(sequentialBranchStarter)
		if !ok {
			return nil
		}
		if target != nil {
			return moerr.NewInternalErrorNoCtx(
				"lazy union all scope contains multiple branch starters")
		}
		target = candidate
		return nil
	})
	if err != nil {
		return nil, err
	}
	if target == nil {
		return nil, moerr.NewInternalErrorNoCtx(
			"lazy union all scope has no branch starter")
	}
	target.SetBranchStarter(start)
	return target.ClearBranchStarter, nil
}

func (s *Scope) MergeRun(c *Compile) (err error) {
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
				return s.cancelMergeSiblingsOnError(err)
			}
		}
		return s.cancelMergeSiblingsOnError(s.ParallelRun(c))
	}

	// Merge Run normally.
	var wg sync.WaitGroup
	preScopeResultReceiveChan := make(chan scopeRunResult, len(s.PreScopes))
	startedPreScopeCount := 0
	claimedPreScopes := make([]bool, len(s.PreScopes))

	startPreScope := func(i int) error {
		if i < 0 || i >= len(s.PreScopes) || claimedPreScopes[i] {
			return moerr.NewInternalErrorNoCtx("invalid lazy union all branch activation")
		}
		scope := s.PreScopes[i]
		if cause := context.Cause(s.Proc.Ctx); cause != nil {
			// The union installs this branch's receiver before invoking us. Complete
			// the unsubmitted scope through the ordinary start-failure cleanup so
			// that receiver has a terminal signal to drain.
			claimedPreScopes[i] = true
			cleanScopeTreeWithStartFail(scope, cause, c.isPrepare)
			return cause
		}
		claimedPreScopes[i] = true
		startedPreScopeCount++
		wg.Add(1)

		submitPreScope := ants.Submit(
			func() {
				defer wg.Done()

				var err error
				switch scope.Magic {
				case Normal:
					err = scope.Run(c)
				case Merge, MergeInsert:
					err = scope.MergeRun(c)
				case Remote:
					err = scope.RemoteRun(c)
				default:
					err = moerr.NewInternalErrorf(c.proc.Ctx, "unexpected scope Magic %d", scope.Magic)
					cleanScopeTreeWithStartFail(scope, err, c.isPrepare)
				}
				s.cancelMergeSiblingsOnError(err)
				preScopeResultReceiveChan <- newScopeRunResult(err, scope)
			})

		// build routine failed.
		if submitPreScope != nil {
			wg.Done() // this is necessary, because the submitPreScope may panic.
			cleanScopeTreeWithStartFail(scope, submitPreScope, c.isPrepare)
			s.cancelMergeSiblingsOnError(submitPreScope)
			preScopeResultReceiveChan <- newScopeRunResult(submitPreScope, scope)
		}
		return submitPreScope
	}

	// step 1.
	if s.LazyPreScopes {
		if len(s.PreScopes) < 2 || len(s.RemoteReceivRegInfos) != 0 {
			err = moerr.NewInternalErrorNoCtx("invalid lazy union all scope topology")
			cleanLazyScopeStartFailure(s, c, err)
			return err
		}
		clearStarter, installErr := installSequentialBranchStarter(s.RootOp, startPreScope)
		if installErr != nil {
			cleanLazyScopeStartFailure(s, c, installErr)
			return installErr
		}
		defer clearStarter()
		defer func() {
			cause := context.Cause(s.Proc.Ctx)
			if cause == nil {
				cause = context.Canceled
			}
			for i := range claimedPreScopes {
				if !claimedPreScopes[i] {
					claimedPreScopes[i] = true
					cleanScopeTreeWithStartFail(s.PreScopes[i], cause, c.isPrepare)
				}
			}
		}()
		// Submission failures are delivered through the first branch receiver,
		// matching the ordinary MergeRun start-failure protocol.
		_ = startPreScope(0)
	} else {
		for i := range s.PreScopes {
			_ = startPreScope(i)
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
		err = collectMergeRunResults(
			s.Proc,
			newScopeRunResultForProcess(err, s.Proc),
			preScopeResultReceiveChan,
			notifyMessageResultReceiveChan)
	}()

	remoteScopeCount := len(s.RemoteReceivRegInfos)

	err = s.ParallelRun(c)
	if err != nil {
		return s.cancelMergeSiblingsOnError(err)
	}
	// Lazy UNION ALL may activate more branches while ParallelRun consumes its
	// input. Count only scopes actually submitted in this execution generation;
	// later branches intentionally remain absent after an early LIMIT stop.
	preScopeCount := startedPreScopeCount

	// receive and check error from pre-scopes and remote scopes.
	if remoteScopeCount == 0 {
		for i := 0; i < preScopeCount; i++ {
			result := <-preScopeResultReceiveChan
			result, _ = result.resolveCancelCause()
			if err = result.err; err != nil {
				return err
			}
		}
		return nil
	}

	for {
		select {
		case result := <-preScopeResultReceiveChan:
			result, _ = result.resolveCancelCause()
			err := result.err
			if err != nil {
				return s.cancelMergeSiblingsOnError(err)
			}
			preScopeCount--

		case result := <-notifyMessageResultReceiveChan:
			result.clean(s.Proc)
			if result.err != nil {
				return s.cancelMergeSiblingsOnError(result.err)
			}
			remoteScopeCount--
		}

		if preScopeCount == 0 && remoteScopeCount == 0 {
			return nil
		}
	}
}

// collectMergeRunResults consumes results left behind after MergeRun returns
// early. A terminal-signal delivery fallback from the merge pipeline is
// secondary when a producer or remote notifier reports the execution error
// that caused cleanup to race a full pipeline channel.
func collectMergeRunResults(
	proc *process.Process,
	current scopeRunResult,
	preScopeResults <-chan scopeRunResult,
	notifyResults <-chan notifyMessageResult,
) error {
	for len(preScopeResults) > 0 {
		current = preferPrimaryScopeResult(current, <-preScopeResults)
	}
	for len(notifyResults) > 0 {
		result := <-notifyResults
		current = preferPrimaryScopeResult(current, newScopeRunResultForProcess(result.err, proc))
		result.clean(proc)
	}
	current, _ = current.resolveCancelCause()
	return current.err
}

// cancelMergeSiblingsOnError breaks the wait-for cycle between a failed
// pipeline, its sibling goroutines, and remote receiver registration. It must
// run before MergeRun waits for those siblings to exit.
func (s *Scope) cancelMergeSiblingsOnError(err error) error {
	if err != nil && s != nil && s.Proc != nil && s.Proc.Cancel != nil {
		s.Proc.Cancel(err)
	}
	return err
}

// cleanPipelineWitchStartFail is used to clean up the pipelines that has failed to start due to a certain reasons.
func cleanPipelineWitchStartFail(sp *Scope, fail error, isPrepare bool) {
	p := pipeline.New(0, nil, sp.RootOp)
	p.Cleanup(sp.Proc, true, isPrepare, fail)
}

// cleanScopeTreeWithStartFail retires a scope tree that was never submitted.
// Children must publish their terminal signals before a parent Merge cleanup
// waits on them. This also releases materialized readers owned by lazy UNION
// ALL branches that an early LIMIT never starts.
func cleanScopeTreeWithStartFail(sp *Scope, fail error, isPrepare bool) {
	if sp == nil {
		return
	}
	for _, preScope := range sp.PreScopes {
		cleanScopeTreeWithStartFail(preScope, fail, isPrepare)
	}
	cleanPipelineWitchStartFail(sp, fail, isPrepare)
}

// RemoteRun send the scope to a remote node for execution.
func (s *Scope) RemoteRun(c *Compile) error {
	s.resourceExecutedLocally = false

	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	if err := validateRemoteRunAddress(s.NodeInfo.Addr, c.addr); err != nil {
		return s.failRemoteRunBeforeStart(c, err)
	}
	if s.ipAddrMatch(c.addr) {
		return s.MergeRun(c)
	}
	if err := s.holdAnyCannotRemoteOperator(); err != nil {
		return s.failRemoteRunBeforeStart(c, err)
	}

	if !checkPipelineStandaloneExecutableAtRemote(s) {
		return s.failRemoteRunBeforeStart(c, moerr.NewInternalErrorNoCtxf(
			"remote pipeline for CN %q is not standalone executable", s.NodeInfo.Addr))
	}
	runtime.ServiceRuntime(s.Proc.GetService()).Logger().
		Debug("remote run pipeline",
			zap.String("local-address", c.addr),
			zap.String("remote-address", s.NodeInfo.Addr))

	p := pipeline.New(0, nil, s.RootOp)
	sender, err := s.remoteRun(c)

	runErr, _ := normalizeScopeRunError(
		err,
		s.Proc.Ctx,
		scopeRunQueryContext(s.Proc),
	)
	if runErr != nil && s.Proc.Cancel != nil {
		s.Proc.Cancel(runErr)
	}
	// Normalize before cleanup mutates the pipeline context so a substantive
	// cancellation cause remains available to the caller.
	p.CleanRootOperator(s.Proc, runErr != nil, c.isPrepare, runErr)

	// sender should be closed after cleanup (tell the children-pipeline that query was done).
	if sender != nil {
		if runErr == nil {
			sender.prepareForLocalCleanup()
		}
		sender.close()
	}
	return runErr
}

func (s *Scope) failRemoteRunBeforeStart(c *Compile, err error) error {
	if c != nil && c.proc != nil && c.proc.Cancel != nil {
		c.proc.Cancel(err)
	}
	cleanScopeTreeWithStartFail(s, err, c.isPrepare)
	return err
}

func validateRemoteRunAddress(scopeAddr, localAddr string) error {
	if scopeAddr == "" || scopeAddr == localAddr {
		return nil
	}
	host, port, err := net.SplitHostPort(scopeAddr)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("malformed remote CN address %q: %v", scopeAddr, err)
	}
	if host == "" {
		return moerr.NewInternalErrorNoCtxf("malformed remote CN address %q: host is empty", scopeAddr)
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return moerr.NewInternalErrorNoCtxf("malformed remote CN address %q: invalid port %q", scopeAddr, port)
	}
	return nil
}

const (
	parallelScopeBuildInternalCancel     = "parallel_scope_build_internal_cancel"
	parallelScopeBuildQueryCancel        = "parallel_scope_build_query_cancel"
	parallelScopeBuildCancelWithCause    = "parallel_scope_build_cancel_with_cause"
	parallelScopeBuildUnattributedCancel = "parallel_scope_build_unattributed_cancel"
)

func scopeCancellationContextState(ctx context.Context) (error, error) {
	if ctx == nil {
		return nil, nil
	}
	return ctx.Err(), context.Cause(ctx)
}

func reportParallelScopeBuildCancellation(
	s *Scope,
	rawErr error,
	normalizedErr error,
	normalized bool,
	queryCtx context.Context,
) {
	pipelineErr, pipelineCause := scopeCancellationContextState(s.Proc.Ctx)
	queryErr, queryCause := scopeCancellationContextState(queryCtx)

	key := parallelScopeBuildUnattributedCancel
	switch {
	case queryErr != nil:
		key = parallelScopeBuildQueryCancel
	case normalized && normalizedErr == nil:
		key = parallelScopeBuildInternalCancel
	case normalized:
		key = parallelScopeBuildCancelWithCause
	}

	terminalEvent := process.EventError.String()
	if normalizedErr == nil {
		terminalEvent = process.EventEnd.String()
	}
	queryID := ""
	if s.Proc.Base != nil {
		queryID = s.Proc.QueryId()
	}
	rootOp := ""
	if s.RootOp != nil {
		rootOp = s.RootOp.OpType().String()
	}
	process.WarnPipelineCleanupf(
		s.Proc,
		key,
		"parallel scope build cleanup classified cancellation: classification=%s phase=build_parallel_scope query_id=%s node_id=%s node_addr=%s mcpu=%d root_op=%s normalized=%t terminal=%s raw_err=%v normalized_err=%v pipeline_err=%v pipeline_cause=%v query_err=%v query_cause=%v",
		key,
		queryID,
		s.NodeInfo.Id,
		s.NodeInfo.Addr,
		s.NodeInfo.Mcpu,
		rootOp,
		normalized,
		terminalEvent,
		rawErr,
		normalizedErr,
		pipelineErr,
		pipelineCause,
		queryErr,
		queryCause)
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
			// ParallelRun owns the source operator until construction publishes a
			// parallel scope. StopSending can cancel the pipeline while reader
			// construction is still in flight, so classify that cancellation at
			// this boundary before cleanup chooses EventEnd versus EventError.
			rawErr := err
			queryCtx := scopeRunQueryContext(s.Proc)
			var normalized bool
			err, normalized = normalizeScopeRunError(
				err,
				s.Proc.Ctx,
				queryCtx,
			)
			if isScopeCancellationError(rawErr) {
				reportParallelScopeBuildCancellation(s, rawErr, err, normalized, queryCtx)
			}
			pipeline.NewMerge(s.RootOp).Cleanup(s.Proc, err != nil, c.isPrepare, err)
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
			s.discardParallelGeneration(ms)
			return nil, err
		}
	}
	if err := c.attachRuntimeAllocationOwners(ss); err != nil {
		s.discardParallelGeneration(ms)
		return nil, err
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

		readers[i].SetOrderBy(s.DataSource.OrderBy)
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
	if err := c.attachRuntimeAllocationOwners(ss); err != nil {
		s.discardParallelGeneration(ms)
		return nil, err
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
		// Remote scope decoding intentionally does not carry relation handles.
		// When this scope owns the complete scan, retain the relation opened on
		// the executing CN so buildReaders can consume the in-memory sentinel
		// returned by Policy_CollectAllData instead of stripping it.
		if s.IsRemote {
			s.DataSource.Rel = engine.NewRelationHandle(rel)
		}
		return nil
	}

	//need to shuffle blocks when cncnt>1
	rsp := &engine.RangesShuffleParam{
		Node:              s.DataSource.node,
		CNCNT:             s.NodeInfo.CNCNT,
		CNIDX:             s.NodeInfo.CNIDX,
		ShuffleByObjectID: false,
		Init:              false,
	}
	if !s.IsRemote { // this is local CN
		rsp.IsLocalCN = true
	}

	policyForLocal := localRangesPolicy(s.DataSource.node, s.NodeInfo.CNIDX)
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

func localRangesPolicy(node *plan.Node, cnidx int32) engine.DataCollectPolicy {
	return engine.Policy_CollectAllData
}

type receivedRuntimeFilter struct {
	spec *plan.RuntimeFilterSpec
	expr *plan.Expr
	data []byte
}

func (s *Scope) waitForRuntimeFilters(c *Compile) ([]receivedRuntimeFilter, bool, error) {
	var runtimeFilters []receivedRuntimeFilter

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
					runtimeFilters = append(runtimeFilters, receivedRuntimeFilter{spec: spec, expr: inExpr})
				case message.RuntimeFilter_UNIQUEJOINKEYS:
					if spec.UseMembershipFilter {
						runtimeFilters = append(runtimeFilters, receivedRuntimeFilter{
							spec: spec,
							data: append([]byte(nil), msg.Data...),
						})
					}

					// TODO: implement BETWEEN expression
				}
			}
		}
	}

	return runtimeFilters, false, nil
}

func (s *Scope) handleRuntimeFilters(c *Compile, runtimeFilters []receivedRuntimeFilter) ([]*plan.Expr, error) {
	runtimeInExprList := make([]*plan.Expr, 0, len(runtimeFilters)+len(s.DataSource.BlockFilterList))
	var nonPkFilters, pkFilters []*plan.Expr

	for _, runtimeFilter := range runtimeFilters {
		runtimeInExprList = append(runtimeInExprList, runtimeFilter.expr)
		fn := runtimeFilter.expr.GetF()
		col := fn.Args[0].GetCol()
		if col == nil {
			panic("only support col in runtime filter's left child!")
		}
		if runtimeFilter.spec.NotOnPk {
			nonPkFilters = append(nonPkFilters, runtimeFilter.expr)
		} else {
			pkFilters = append(pkFilters, runtimeFilter.expr)
		}
	}

	// reset filter
	if len(nonPkFilters) > 0 {
		// Phase 2: if the leaf op is TableScan (inline filter path), set RuntimeFilterExprs directly.
		// Otherwise fall back to the legacy Filter operator path.
		leafOp := vm.GetLeafOp(s.RootOp)
		if ts, ok := leafOp.(*table_scan.TableScan); ok {
			ts.RuntimeFilterExprs = nonPkFilters
		} else {
			arg, ok := leafOp.(*filter.Filter)
			if !ok {
				panic("missing instruction for runtime filter!")
			}
			arg.RuntimeFilterExprs = nonPkFilters
		}
	}

	// reset datasource
	if len(pkFilters) > 0 {
		if s.DataSource.FilterExpr != nil {
			pkFilters = append(pkFilters, s.DataSource.FilterExpr)
		}
		s.DataSource.FilterExpr = colexec.RewriteFilterExprList(pkFilters)
	}

	blockFilterList := s.DataSource.BlockFilterList
	if s.IsRemote {
		// Keep the decoded scope as a reusable raw-expression template. Fold IDs
		// belong to this Compile generation and must not be stored back in it.
		blockFilterList = plan2.DeepCopyExprList(blockFilterList)
	}
	for _, e := range blockFilterList {
		// RemoteRun carries the original block-filter expressions so this Compile
		// owns the Fold executors used to expand ranges. A Fold already present on
		// the wire contains a sender-owned executor ID and is therefore invalid.
		if s.IsRemote {
			if plan2.HasFoldValExpr(e) {
				return nil, moerr.NewInternalErrorNoCtx("remote block filter contains a sender-owned Fold value")
			}
			if _, err := plan2.ReplaceFoldExpr(s.Proc, e, &c.filterExprExes); err != nil {
				return nil, err
			}
		}
		err := plan2.EvalFoldExpr(s.Proc, e, &c.filterExprExes)
		if err != nil {
			return nil, err
		}
	}

	if len(runtimeInExprList) == 0 && len(blockFilterList) == 0 {
		return nil, nil
	}
	return append(runtimeInExprList, blockFilterList...), nil
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
	dupCtx := newOperatorDupContext()
	for i := 0; i < s.NodeInfo.Mcpu; i++ {
		parallelScopes[i] = newScope(Normal)
		parallelScopes[i].NodeInfo = s.NodeInfo
		parallelScopes[i].NodeInfo.Mcpu = 1
		parallelScopes[i].Proc = rs.Proc.NewContextChildProc(0)
		parallelScopes[i].TxnOffset = s.TxnOffset
		parallelScopes[i].setRootOperator(dupOperatorRecursivelyWithContext(s.RootOp, i, s.NodeInfo.Mcpu, dupCtx))
	}

	rs.PreScopes = parallelScopes
	s.PreScopes = append(s.PreScopes, rs)
	s.parallelGenerations = append(s.parallelGenerations, rs)

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

const (
	notifyMessageRetryInitialInterval = 100 * time.Millisecond
	notifyMessageRetryMaxInterval     = time.Second
)

type notifyMessageSenderFactory func(
	ctx context.Context,
	sid string,
	toAddr string,
	mp *mpool.MPool,
	analyzeModule *AnalyzeModule,
) (*messageSenderOnClient, error)

// clean do final work for a notifyMessageResult.
func (r *notifyMessageResult) clean(proc *process.Process) {
	if r.sender != nil {
		if r.err == nil {
			r.sender.prepareForLocalCleanup()
		}
		r.sender.close()
	}
	if r.err != nil {
		proc.Infof(proc.Ctx, "send notify message failed : %s", r.err)
	}
}

// sendNotifyMessage create n routines to notify the remote nodes where their receivers are.
// and keep receiving the data until the query was done or data is ended.
func (s *Scope) sendNotifyMessage(wg *sync.WaitGroup, resultChan chan notifyMessageResult) {
	s.sendNotifyMessageWithFactory(wg, resultChan, newMessageSenderOnClient)
}

func (s *Scope) sendNotifyMessageWithFactory(
	wg *sync.WaitGroup,
	resultChan chan notifyMessageResult,
	newSender notifyMessageSenderFactory,
) {
	s.sendNotifyMessageWithFactoryAndWait(
		wg,
		resultChan,
		newSender,
		waitRemoteDispatchRetry,
	)
}

type notifyMessageRetryWait func(context.Context, int, uuid.UUID) error

func waitRemoteDispatchRetry(
	ctx context.Context,
	attempt int,
	uid uuid.UUID,
) error {
	delay := notifyMessageRetryDelay(attempt, uid)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return remoteRegistrationContextError(ctx)
	case <-timer.C:
		return nil
	}
}

func notifyMessageRetryDelay(attempt int, uid uuid.UUID) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := notifyMessageRetryInitialInterval
	for i := 0; i < attempt && delay < notifyMessageRetryMaxInterval; i++ {
		delay *= 2
	}
	if delay > notifyMessageRetryMaxInterval {
		delay = notifyMessageRetryMaxInterval
	}

	// Stable per-receiver jitter avoids synchronizing thousands of legacy-peer
	// compatibility retries while keeping tests and behavior deterministic.
	seed := uint32(uid[12])<<24 |
		uint32(uid[13])<<16 |
		uint32(uid[14])<<8 |
		uint32(uid[15])
	seed ^= uint32(attempt+1) * 0x9e3779b9
	jitterPercent := int(seed%41) - 20 // [-20%, +20%]
	return delay + time.Duration(int64(delay)*int64(jitterPercent)/100)
}

func (s *Scope) sendNotifyMessageWithFactoryAndWait(
	wg *sync.WaitGroup,
	resultChan chan notifyMessageResult,
	newSender notifyMessageSenderFactory,
	waitRetry notifyMessageRetryWait,
) {
	// if context has done, it means the user or other part of the pipeline stops this query.
	closeWithError := func(err error, reg *process.WaitRegister, sender *messageSenderOnClient) {
		err, _ = normalizeScopeRunError(
			err,
			s.Proc.Ctx,
			scopeRunQueryContext(s.Proc),
		)
		s.cancelMergeSiblingsOnError(err)
		sendRemoteNotifyCleanupTerminal(s.Proc, reg, err)
		resultChan <- notifyMessageResult{err: err, sender: sender}
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
				attempt := 0
				for {
					sender, err := newSender(
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
					sender.requestStreamProtocols(message)
					message.NeedNotReply = false
					message.Uuid = uuid

					if errSend := sender.streamSender.Send(sender.ctx, message); errSend != nil {
						closeWithError(errSend, s.Proc.Reg.MergeReceivers[receiverIdx], sender)
						return
					}
					sender.markStreamActive(pbpipeline.Method_PrepareDoneNotifyMessage)

					err = receiveMsgAndForward(sender, s.Proc.Reg.MergeReceivers[receiverIdx])
					if !isRemoteDispatchNotRegisteredYetError(err) {
						closeWithError(err, s.Proc.Reg.MergeReceivers[receiverIdx], sender)
						return
					}
					// "not registered yet" is an expected retry response. The
					// negotiated terminal response proves the old attempt can use
					// FIN/ACK after its server cleanup barrier.
					sender.prepareForLocalCleanup()
					sender.close()
					metricv2.PipelineRemoteNotifyRetryCounter.Inc()
					if err = waitRetry(s.Proc.Ctx, attempt, op.Uuid); err != nil {
						closeWithError(err, s.Proc.Reg.MergeReceivers[receiverIdx], nil)
						return
					}
					attempt++
				}
			},
		)

		if errSubmit != nil {
			closeWithError(errSubmit, s.Proc.Reg.MergeReceivers[receiverIdx], nil)
		}
	}
}

func sendRemoteNotifyCleanupTerminal(proc *process.Process, reg *process.WaitRegister, err error) bool {
	terminalSignal := process.BuildCleanupSignal(false, err)
	signalCtx, signalCancel := context.WithTimeout(context.TODO(), process.PipelineSignalSendTimeout)
	defer signalCancel()

	if process.SendPipelineSignalWithContext(signalCtx, reg, terminalSignal) {
		return true
	}
	logRemoteNotifyCleanupSendFailure(
		proc,
		reg,
		terminalSignal,
		"remote_notify_cleanup_send_terminal_signal",
		"remote notify cleanup timed out sending terminal %s signal: timeout=%s channel_len=%d channel_cap=%d err=%v",
		err)

	if terminalSignal.EventType != process.EventEnd {
		return false
	}

	fallbackErr := process.ErrPipelineEndSignalDeliveryFailed
	fallbackSignal := process.NewAbortSignal(fallbackErr)
	if process.SendPipelineSignalWithContext(signalCtx, reg, fallbackSignal) {
		return false
	}
	logRemoteNotifyCleanupSendFailure(
		proc,
		reg,
		fallbackSignal,
		"remote_notify_cleanup_send_fallback_abort_signal",
		"remote notify cleanup timed out sending fallback %s signal after end delivery failure: timeout=%s channel_len=%d channel_cap=%d err=%v",
		fallbackErr)
	return false
}

func logRemoteNotifyCleanupSendFailure(
	proc *process.Process,
	reg *process.WaitRegister,
	signal process.PipelineSignal,
	key string,
	format string,
	err error,
) {
	chLen, chCap := process.WaitRegisterChannelState(reg)
	process.WarnPipelineCleanupf(
		proc,
		key,
		format,
		signal.EventType.String(),
		process.PipelineSignalSendTimeout,
		chLen,
		chCap,
		err)
}

func receiveMsgAndForward(sender *messageSenderOnClient, forwardReg *process.WaitRegister) error {
	for {
		bat, end, err := sender.receiveBatch()
		if err != nil || end || bat == nil {
			return err
		}

		var receiverDone bool
		if receiverDone, err = forwardRemoteBatchWithContext(sender, forwardReg, bat, sender.mp); err != nil {
			return err
		}
		// A stopped receiver intentionally discarded the decoded batch, but the
		// remote sender still owns its credit until this ACK is sent.
		if err = sender.acknowledgeRemoteBatch(); err != nil {
			return err
		}
		if receiverDone {
			return nil
		}
	}
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
	var runtimeFilterList []receivedRuntimeFilter
	var blockFilterList []*plan.Expr
	var emptyScan bool
	runtimeFilterList, emptyScan, err = s.waitForRuntimeFilters(c)
	if err != nil {
		return
	}
	if s.DataSource.node != nil && s.DataSource.node.NodeType == plan.Node_VECTOR_INDEX_SCAN {
		if emptyScan {
			return []engine.Reader{new(readutil.EmptyReader)}, nil
		}
		return s.buildVectorIndexReaders(runtimeFilterList)
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
	// A distributed remote scope only owns its assigned persisted blocks. Keep
	// using the engine reader, which deliberately excludes the memory-block
	// sentinel owned by the local scope. A single remote scope is different: it
	// owns the complete scan, including committed rows in this CN's partition
	// state, so it must use the relation reader below.
	case s.IsRemote && (s.NodeInfo.CNCNT != 1 || s.DataSource.Rel == nil):
		// this cannot use c.proc.Ctx directly, please refer to `default case`.
		ctx := c.proc.Ctx
		if util.TableIsClusterTable(s.DataSource.TableDef.GetTableType()) {
			ctx = defines.AttachAccountId(ctx, catalog.System_Account)
		}
		if s.DataSource.AccountId != nil {
			ctx = defines.AttachAccountId(ctx, uint32(s.DataSource.AccountId.GetTenantId()))
		}
		hint := engine.FilterHint{}
		if tableDef := s.DataSource.TableDef; tableDef != nil {
			switch {
			case catalog.IsFullTextIndexTableType(tableDef.TableType, tableDef.Name):
				hint.MembershipFilterBytes = s.DataSource.MembershipFilterBytes
				if len(hint.MembershipFilterBytes) == 0 {
					hint.MembershipFilterBytes, _ = c.proc.Ctx.Value(
						defines.FulltextMembershipFilter{}).([]byte)
				}
			}
		}

		readers, err = c.e.BuildBlockReaders(
			ctx,
			c.proc,
			s.DataSource.Timestamp,
			s.DataSource.FilterExpr,
			s.DataSource.TableDef,
			s.NodeInfo.Data,
			s.NodeInfo.Mcpu,
			hint)
		if err != nil {
			return
		}
	// Reader can be generated from the relation on the executing CN.
	case s.DataSource.Rel != nil:
		ctx := c.proc.Ctx
		if s.IsRemote {
			if util.TableIsClusterTable(s.DataSource.TableDef.GetTableType()) {
				ctx = defines.AttachAccountId(ctx, catalog.System_Account)
			}
			account := s.DataSource.AccountId
			if account == nil && s.DataSource.node != nil && s.DataSource.node.ObjRef != nil {
				account = s.DataSource.node.ObjRef.PubInfo
			}
			if account != nil {
				ctx = defines.AttachAccountId(ctx, uint32(account.GetTenantId()))
			}
		}
		stats := statistic.StatsInfoFromContext(ctx)
		crs := new(perfcounter.CounterSet)
		newCtx := perfcounter.AttachS3RequestKey(ctx, crs)

		hint := engine.FilterHint{}
		// Pass runtime membership filter bytes to reader via FilterHint (for fulltext index table).
		if n := s.DataSource.node; n != nil && n.TableDef != nil &&
			catalog.IsFullTextIndexTableType(n.TableDef.TableType, n.TableDef.Name) {
			if s.IsRemote {
				hint.MembershipFilterBytes = s.DataSource.MembershipFilterBytes
			}
			if len(hint.MembershipFilterBytes) == 0 {
				if bf, ok := c.proc.Ctx.Value(defines.FulltextMembershipFilter{}).([]byte); ok {
					hint.MembershipFilterBytes = bf
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
		// Pass runtime membership filter bytes to reader via FilterHint (for fulltext index table).
		if n := s.DataSource.node; n != nil && n.TableDef != nil &&
			catalog.IsFullTextIndexTableType(n.TableDef.TableType, n.TableDef.Name) {
			if bfVal := c.proc.Ctx.Value(defines.FulltextMembershipFilter{}); bfVal != nil {
				if bf, ok := bfVal.([]byte); ok && len(bf) > 0 {
					hint.MembershipFilterBytes = bf
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

func (s *Scope) buildVectorIndexReaders(runtimeFilters []receivedRuntimeFilter) ([]engine.Reader, error) {
	node := s.DataSource.node
	spec := node.GetVectorIndexScan()
	if spec == nil || spec.GetIndex() == nil {
		return nil, moerr.NewInvalidInputNoCtx("vector index scan is missing index metadata")
	}
	p, ok := indexplugin.Get(spec.GetIndex().GetIndexAlgo())
	if !ok {
		return nil, moerr.NewNotSupportedNoCtxf("vector index algorithm %q is not registered", spec.GetIndex().GetIndexAlgo())
	}
	searcher, ok := p.(indexplugin.SearchPlugin)
	if !ok {
		return nil, moerr.NewNotSupportedNoCtxf("vector index algorithm %q has no scan reader", spec.GetIndex().GetIndexAlgo())
	}

	membership, hasMembership := vectorScanMembershipFilter(runtimeFilters)
	currentSnapshot := timestamp.Timestamp{}
	if s.Proc != nil && s.Proc.GetTxnOperator() != nil {
		currentSnapshot = s.Proc.GetTxnOperator().Txn().SnapshotTS
	}
	identity, err := vectorscan.Identity(
		spec, currentSnapshot,
		s.TxnOffset, s.NodeInfo.CNCNT, s.NodeInfo.CNIDX)
	if err != nil {
		return nil, err
	}
	req, hasQuery, err := vectorscan.RequestFromScalar(spec, identity, membership, hasMembership)
	if err != nil {
		return nil, err
	}
	if !hasQuery {
		return []engine.Reader{new(readutil.EmptyReader)}, nil
	}
	reader, err := searcher.Search().NewReader(s.Proc, spec, req)
	if err != nil {
		return nil, err
	}
	return []engine.Reader{reader}, nil
}

func vectorScanMembershipFilter(runtimeFilters []receivedRuntimeFilter) ([]byte, bool) {
	for _, runtimeFilter := range runtimeFilters {
		hasMembership := runtimeFilter.spec != nil && runtimeFilter.spec.UseMembershipFilter
		if len(runtimeFilter.data) > 0 {
			return append([]byte(nil), runtimeFilter.data...), hasMembership
		}
		fn := runtimeFilter.expr.GetF()
		if fn == nil || len(fn.Args) != 2 || fn.Args[1].GetVec() == nil {
			if hasMembership {
				return nil, true
			}
			continue
		}
		return append([]byte(nil), fn.Args[1].GetVec().GetData()...), hasMembership
	}
	return nil, false
}

func (s Scope) TypeName() string {
	return "compile.Scope"
}
