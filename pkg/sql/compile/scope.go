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
	"hash/crc32"
	"runtime/debug"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pbpipeline "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/right"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// Run read data from storage engine and run the instructions of scope.
func (s *Scope) Run(c *Compile) (err error) {
	var p *pipeline.Pipeline
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(s.Proc.Ctx, e)
		}
		p.Cleanup(s.Proc, err != nil)
	}()

	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)
	// DataSource == nil specify the empty scan
	if s.DataSource == nil {
		p = pipeline.New(nil, s.Instructions, s.Reg)
		if _, err = p.ConstRun(nil, s.Proc); err != nil {
			return err
		}
	} else {
		p = pipeline.New(s.DataSource.Attributes, s.Instructions, s.Reg)
		if s.DataSource.Bat != nil {
			_, err = p.ConstRun(s.DataSource.Bat, s.Proc)
		} else {
			_, err = p.Run(s.DataSource.R, s.Proc)
		}
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

// MergeRun range and run the scope's pre-scopes by go-routine, and finally run itself to do merge work.
func (s *Scope) MergeRun(c *Compile) error {
	errChan := make(chan error, len(s.PreScopes))
	var wg sync.WaitGroup
	for i := range s.PreScopes {
		scope := s.PreScopes[i]
		wg.Add(1)
		ants.Submit(func() {
			switch scope.Magic {
			case Normal:
				errChan <- scope.Run(c)
			case Merge, MergeInsert:
				errChan <- scope.MergeRun(c)
			case Remote:
				errChan <- scope.RemoteRun(c)
			case Parallel:
				errChan <- scope.ParallelRun(c, scope.IsRemote)
			case Pushdown:
				errChan <- scope.PushdownRun()
			}
			wg.Done()
		})
	}
	defer wg.Wait()

	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)
	var errReceiveChan chan error
	if len(s.RemoteReceivRegInfos) > 0 {
		errReceiveChan = make(chan error, len(s.RemoteReceivRegInfos))
		s.notifyAndReceiveFromRemote(errReceiveChan)
	}
	p := pipeline.NewMerge(s.Instructions, s.Reg)
	if _, err := p.MergeRun(s.Proc); err != nil {
		select {
		case <-s.Proc.Ctx.Done():
		default:
			p.Cleanup(s.Proc, true)
			return err
		}
	}
	p.Cleanup(s.Proc, false)

	// receive and check error from pre-scopes and remote scopes.
	preScopeCount := len(s.PreScopes)
	remoteScopeCount := len(s.RemoteReceivRegInfos)
	if remoteScopeCount == 0 {
		for i := 0; i < len(s.PreScopes); i++ {
			if err := <-errChan; err != nil {
				return err
			}
		}
		return nil
	}

	for {
		select {
		case err := <-errChan:
			if err != nil {
				return err
			}
			preScopeCount--

		case err := <-errReceiveChan:
			if err != nil {
				return err
			}
			remoteScopeCount--
		}

		if preScopeCount == 0 && remoteScopeCount == 0 {
			return nil
		}
	}
}

// RemoteRun send the scope to a remote node for execution.
// if no target node information, just execute it at local.
func (s *Scope) RemoteRun(c *Compile) error {
	// if send to itself, just run it parallel at local.
	if len(s.NodeInfo.Addr) == 0 || len(c.addr) == 0 || isSameCN(c.addr, s.NodeInfo.Addr) {
		return s.ParallelRun(c, s.IsRemote)
	}

	if !cnclient.IsCNClientReady() {
		return s.ParallelRun(c, s.IsRemote)
	}

	runtime.ProcessLevelRuntime().Logger().
		Debug("remote run pipeline",
			zap.String("local-address", c.addr),
			zap.String("remote-address", s.NodeInfo.Addr))

	err := s.remoteRun(c)
	select {
	case <-s.Proc.Ctx.Done():
		// if context has done, it means other pipeline stop the query normally.
		// so there is no need to return the error again.
		return nil

	default:
		return err
	}
}

// ParallelRun try to execute the scope in parallel way.
func (s *Scope) ParallelRun(c *Compile, remote bool) error {
	var rds []engine.Reader

	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)
	if s.IsJoin {
		return s.JoinRun(c)
	}
	if s.IsLoad {
		return s.LoadRun(c)
	}
	if s.DataSource == nil {
		return s.MergeRun(c)
	}

	mcpu := s.NodeInfo.Mcpu
	var err error

	if len(s.DataSource.RuntimeFilterSpecs) > 0 {
		exprs := make([]*plan.Expr, 0, len(s.DataSource.RuntimeFilterSpecs))
		filters := make([]*pbpipeline.RuntimeFilter, 0, len(exprs))

		for _, spec := range s.DataSource.RuntimeFilterSpecs {
			c.lock.RLock()
			receiver, ok := c.runtimeFilterReceiverMap[spec.Tag]
			c.lock.RUnlock()
			if !ok {
				continue
			}

			for i := 0; i < receiver.size; i++ {
				select {
				case <-s.Proc.Ctx.Done():
					return nil

				case filter := <-receiver.ch:
					if filter == nil {
						exprs = nil
						s.NodeInfo.Data = s.NodeInfo.Data[:0]
						break
					}
					switch filter.Typ {
					case pbpipeline.RuntimeFilter_NO_FILTER:
						continue

					case pbpipeline.RuntimeFilter_IN:
						inExpr := &plan.Expr{
							Typ: &plan.Type{
								Id:          int32(types.T_bool),
								NotNullable: spec.Expr.Typ.NotNullable,
							},
							Expr: &plan.Expr_F{
								F: &plan.Function{
									Func: &plan.ObjectRef{
										Obj:     function.InFunctionEncodedID,
										ObjName: function.InFunctionName,
									},
									Args: []*plan.Expr{
										spec.Expr,
										{
											Typ: &plan.Type{
												Id: int32(types.T_tuple),
											},
											Expr: &plan.Expr_Bin{
												Bin: &plan.BinaryData{
													Data: filter.Data,
												},
											},
										},
									},
								},
							},
						}

						if s.DataSource.Expr == nil {
							s.DataSource.Expr = inExpr
						} else {
							s.DataSource.Expr = &plan.Expr{
								Typ: &plan.Type{
									Id:          int32(types.T_bool),
									NotNullable: s.DataSource.Expr.Typ.NotNullable && inExpr.Typ.NotNullable,
								},
								Expr: &plan.Expr_F{
									F: &plan.Function{
										Func: &plan.ObjectRef{
											Obj:     function.AndFunctionEncodedID,
											ObjName: function.AndFunctionName,
										},
										Args: []*plan.Expr{
											s.DataSource.Expr,
											inExpr,
										},
									},
								},
							}
						}

						// TODO: implement BETWEEN expression
					}

					exprs = append(exprs, spec.Expr)
					filters = append(filters, filter)
				}
			}
		}

		if len(exprs) > 0 {
			s.NodeInfo.Data, err = ApplyRuntimeFilters(c.ctx, s.Proc, s.DataSource.TableDef, s.NodeInfo.Data, exprs, filters)
			if err != nil {
				return err
			}
		}
	}

	switch {
	case remote:
		ctx := c.ctx
		if util.TableIsClusterTable(s.DataSource.TableDef.GetTableType()) {
			ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
		}
		if s.DataSource.AccountId != nil {
			ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(s.DataSource.AccountId.GetTenantId()))
		}
		rds, err = c.e.NewBlockReader(ctx, mcpu, s.DataSource.Timestamp, s.DataSource.Expr,
			s.NodeInfo.Data, s.DataSource.TableDef, c.proc)
		if err != nil {
			return err
		}
		s.NodeInfo.Data = nil

	case s.NodeInfo.Rel != nil:
		if rds, err = s.NodeInfo.Rel.NewReader(c.ctx, mcpu, s.DataSource.Expr, s.NodeInfo.Data); err != nil {
			return err
		}
		s.NodeInfo.Data = nil

	//FIXME:: s.NodeInfo.Rel == nil, partition table?
	default:
		var db engine.Database
		var rel engine.Relation

		ctx := c.ctx
		if util.TableIsClusterTable(s.DataSource.TableDef.GetTableType()) {
			ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
		}
		db, err = c.e.Database(ctx, s.DataSource.SchemaName, s.Proc.TxnOperator)
		if err != nil {
			return err
		}
		rel, err = db.Relation(ctx, s.DataSource.RelationName, c.proc)
		if err != nil {
			var e error // avoid contamination of error messages
			db, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, s.Proc.TxnOperator)
			if e != nil {
				return e
			}
			rel, e = db.Relation(c.ctx, engine.GetTempTableName(s.DataSource.SchemaName, s.DataSource.RelationName), c.proc)
			if e != nil {
				return err
			}
		}
		if rel.GetEngineType() == engine.Memory ||
			s.DataSource.PartitionRelationNames == nil {
			mainRds, err := rel.NewReader(
				ctx,
				mcpu,
				s.DataSource.Expr,
				s.NodeInfo.Data)
			if err != nil {
				return err
			}
			rds = append(rds, mainRds...)
		} else {
			//handle partition table.
			dirtyRanges := make(map[int][][]byte, 0)
			cleanRanges := make([][]byte, 0, len(s.NodeInfo.Data))
			ranges := s.NodeInfo.Data[1:]
			for _, r := range ranges {
				blkInfo := catalog.DecodeBlockInfo(r)
				if !blkInfo.CanRemote {
					if _, ok := dirtyRanges[blkInfo.PartitionNum]; !ok {
						newRanges := make([][]byte, 0, 1)
						newRanges = append(newRanges, []byte{})
						dirtyRanges[blkInfo.PartitionNum] = newRanges
					}
					dirtyRanges[blkInfo.PartitionNum] =
						append(dirtyRanges[blkInfo.PartitionNum], r)
					continue
				}
				cleanRanges = append(cleanRanges, r)
			}

			if len(cleanRanges) > 0 {
				// create readers for reading clean blocks from main table.
				mainRds, err := rel.NewReader(
					ctx,
					mcpu,
					s.DataSource.Expr,
					cleanRanges)
				if err != nil {
					return err
				}
				rds = append(rds, mainRds...)

			}
			// create readers for reading dirty blocks from partition table.
			for num, relName := range s.DataSource.PartitionRelationNames {
				subrel, err := db.Relation(c.ctx, relName, c.proc)
				if err != nil {
					return err
				}
				memRds, err := subrel.NewReader(c.ctx, mcpu, s.DataSource.Expr, dirtyRanges[num])
				if err != nil {
					return err
				}
				rds = append(rds, memRds...)
			}
		}
		s.NodeInfo.Data = nil
	}

	if len(rds) != mcpu {
		newRds := make([]engine.Reader, 0, mcpu)
		step := len(rds) / mcpu
		for i := 0; i < len(rds); i += step {
			m := disttae.NewMergeReader(rds[i : i+step])
			newRds = append(newRds, m)
		}
		rds = newRds
	}

	if mcpu == 1 {
		s.Magic = Normal
		s.DataSource.R = rds[0] // rds's length is equal to mcpu so it is safe to do it
		return s.Run(c)
	}

	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = &Scope{
			Magic: Normal,
			DataSource: &Source{
				R:            rds[i],
				SchemaName:   s.DataSource.SchemaName,
				RelationName: s.DataSource.RelationName,
				Attributes:   s.DataSource.Attributes,
				AccountId:    s.DataSource.AccountId,
			},
			NodeInfo: s.NodeInfo,
		}
		ss[i].Proc = process.NewWithAnalyze(s.Proc, c.ctx, 0, c.anal.Nodes())
	}
	newScope, err := newParallelScope(s, ss)
	if err != nil {
		return err
	}
	newScope.SetContextRecursively(s.Proc.Ctx)
	return newScope.MergeRun(c)
}

func (s *Scope) PushdownRun() error {
	var end bool // exist flag
	var err error

	reg := colexec.Srv.GetConnector(s.DataSource.PushdownId)
	for {
		bat := <-reg.Ch
		if bat == nil {
			s.Proc.SetInputBatch(bat)
			_, err = vm.Run(s.Instructions, s.Proc)
			s.Proc.Cancel()
			return err
		}
		if bat.RowCount() == 0 {
			continue
		}
		s.Proc.SetInputBatch(bat)
		if end, err = vm.Run(s.Instructions, s.Proc); err != nil || end {
			return err
		}
	}
}

func (s *Scope) JoinRun(c *Compile) error {
	mcpu := s.NodeInfo.Mcpu
	if mcpu <= 1 { // no need to parallel
		buildScope := c.newJoinBuildScope(s, nil)
		s.PreScopes = append(s.PreScopes, buildScope)
		if s.BuildIdx > 1 {
			probeScope := c.newJoinProbeScope(s, nil)
			s.PreScopes = append(s.PreScopes, probeScope)
		}
		// this is for shuffle join probe scope
		s.Proc.Reg.MergeReceivers[0].Ch = make(chan *batch.Batch, shuffleJoinProbeChannelBufferSize)
		return s.MergeRun(c)
	}

	isRight := s.isRight()

	chp := s.PreScopes
	for i := range chp {
		chp[i].IsEnd = true
	}

	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = &Scope{
			Magic:    Merge,
			NodeInfo: s.NodeInfo,
		}
		ss[i].Proc = process.NewWithAnalyze(s.Proc, s.Proc.Ctx, 2, c.anal.Nodes())
		ss[i].Proc.Reg.MergeReceivers[1].Ch = make(chan *batch.Batch, 10)
	}
	probe_scope, build_scope := c.newJoinProbeScope(s, ss), c.newJoinBuildScope(s, ss)
	var err error
	s, err = newParallelScope(s, ss)
	if err != nil {
		return err
	}

	if isRight {
		channel := make(chan *bitmap.Bitmap, mcpu)
		for i := range s.PreScopes {
			switch arg := s.PreScopes[i].Instructions[0].Arg.(type) {
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
	s.PreScopes = append(s.PreScopes, chp...)
	s.PreScopes = append(s.PreScopes, build_scope)
	s.PreScopes = append(s.PreScopes, probe_scope)

	return s.MergeRun(c)
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

func (s *Scope) LoadRun(c *Compile) error {
	mcpu := s.NodeInfo.Mcpu
	ss := make([]*Scope, mcpu)
	bat := batch.NewWithSize(1)
	{
		bat.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 1, c.proc.Mp())
		bat.SetRowCount(1)
	}
	for i := 0; i < mcpu; i++ {
		ss[i] = &Scope{
			Magic: Normal,
			DataSource: &Source{
				Bat: bat,
			},
			NodeInfo: s.NodeInfo,
		}
		ss[i].Proc = process.NewWithAnalyze(s.Proc, c.ctx, 0, c.anal.Nodes())
	}
	newScope, err := newParallelScope(s, ss)
	if err != nil {
		return err
	}

	return newScope.MergeRun(c)
}

func newParallelScope(s *Scope, ss []*Scope) (*Scope, error) {
	var flg bool

	for i, in := range s.Instructions {
		if flg {
			break
		}
		switch in.Op {
		case vm.Top:
			flg = true
			arg := in.Arg.(*top.Argument)
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeTop,
				Idx: in.Idx,
				Arg: &mergetop.Argument{
					Fs:    arg.Fs,
					Limit: arg.Limit,
				},
			}
			for j := range ss {
				ss[j].appendInstruction(vm.Instruction{
					Op:      vm.Top,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: &top.Argument{
						Fs:    arg.Fs,
						Limit: arg.Limit,
					},
				})
			}
		case vm.Order:
			flg = true
			arg := in.Arg.(*order.Argument)
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeOrder,
				Idx: in.Idx,
				Arg: &mergeorder.Argument{
					OrderBySpecs: arg.OrderBySpec,
				},
			}
			for j := range ss {
				ss[j].appendInstruction(vm.Instruction{
					Op:      vm.Order,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: &order.Argument{
						OrderBySpec: arg.OrderBySpec,
					},
				})
			}
		case vm.Limit:
			flg = true
			arg := in.Arg.(*limit.Argument)
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeLimit,
				Idx: in.Idx,
				Arg: &mergelimit.Argument{
					Limit: arg.Limit,
				},
			}
			for j := range ss {
				ss[j].appendInstruction(vm.Instruction{
					Op:      vm.Limit,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: &limit.Argument{
						Limit: arg.Limit,
					},
				})
			}
		case vm.Group:
			flg = true
			arg := in.Arg.(*group.Argument)
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeGroup,
				Idx: in.Idx,
				Arg: &mergegroup.Argument{
					NeedEval: false,
				},
			}
			for j := range ss {
				ss[j].appendInstruction(vm.Instruction{
					Op:      vm.Group,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: &group.Argument{
						Aggs:      arg.Aggs,
						Exprs:     arg.Exprs,
						Types:     arg.Types,
						MultiAggs: arg.MultiAggs,
					},
				})
			}
		case vm.Offset:
			flg = true
			arg := in.Arg.(*offset.Argument)
			s.Instructions = s.Instructions[i:]
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeOffset,
				Idx: in.Idx,
				Arg: &mergeoffset.Argument{
					Offset: arg.Offset,
				},
			}
			for j := range ss {
				ss[j].appendInstruction(vm.Instruction{
					Op:      vm.Offset,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: &offset.Argument{
						Offset: arg.Offset,
					},
				})
			}
		case vm.Output:
		default:
			for j := range ss {
				ss[j].appendInstruction(dupInstruction(&in, nil, j))
			}
		}
	}
	if !flg {
		for i := range ss {
			ss[i].Instructions = ss[i].Instructions[:len(ss[i].Instructions)-1]
		}
		s.Instructions[0] = vm.Instruction{
			Op:  vm.Merge,
			Idx: s.Instructions[0].Idx, // TODO: remove it
			Arg: &merge.Argument{},
		}
		//Add log for cn panic which reported on issue 10656
		//If you find this log is printed, please report the repro details
		if len(s.Instructions) < 2 {
			logutil.Error("the length of s.Instructions is too short!"+DebugShowScopes([]*Scope{s}),
				zap.String("stack", string(debug.Stack())),
			)
			return nil, moerr.NewInternalErrorNoCtx("the length of s.Instructions is too short !")
		}
		s.Instructions[1] = s.Instructions[len(s.Instructions)-1]
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
				Ch:  make(chan *batch.Batch, 1),
			}
		}
	}
	j := 0
	for i := range ss {
		if !ss[i].IsEnd {
			ss[i].appendInstruction(vm.Instruction{
				Op: vm.Connector,
				Arg: &connector.Argument{
					Reg: s.Proc.Reg.MergeReceivers[j],
				},
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

func (s *Scope) notifyAndReceiveFromRemote(errChan chan error) {
	for i := range s.RemoteReceivRegInfos {
		op := &s.RemoteReceivRegInfos[i]

		go func(info *RemoteReceivRegInfo, reg *process.WaitRegister) {
			// if context has done, it means other pipeline stop the query normally.
			closeWithError := func(err error) {
				if reg != nil {
					reg.Ch <- nil
					close(reg.Ch)
				}

				select {
				case <-s.Proc.Ctx.Done():
					errChan <- nil
				default:
					errChan <- err
				}
			}

			streamSender, errStream := cnclient.GetStreamSender(info.FromAddr)
			if errStream != nil {
				closeWithError(errStream)
				return
			}
			defer streamSender.Close(true)

			message := cnclient.AcquireMessage()
			{
				message.Id = streamSender.ID()
				message.Cmd = pbpipeline.PrepareDoneNotifyMessage
				message.Sid = pbpipeline.Last
				message.Uuid = info.Uuid[:]
			}
			if errSend := streamSender.Send(s.Proc.Ctx, message); errSend != nil {
				closeWithError(errSend)
				return
			}

			messagesReceive, errReceive := streamSender.Receive()
			if errReceive != nil {
				closeWithError(errReceive)
				return
			}
			var ch chan *batch.Batch
			if reg != nil {
				ch = reg.Ch
			}
			err := receiveMsgAndForward(s.Proc, messagesReceive, ch)
			closeWithError(err)
		}(op, s.Proc.Reg.MergeReceivers[op.Idx])
	}
}

func receiveMsgAndForward(proc *process.Process, receiveCh chan morpc.Message, forwardCh chan *batch.Batch) error {
	var val morpc.Message
	var dataBuffer []byte
	var ok bool
	var m *pbpipeline.Message

	for {
		select {
		case <-proc.Ctx.Done():
			logutil.Warnf("proc ctx done during forward")
			return nil
		case val, ok = <-receiveCh:
			if val == nil || !ok {
				return moerr.NewStreamClosedNoCtx()
			}
		}

		m, ok = val.(*pbpipeline.Message)
		if !ok {
			panic("unexpected message type for cn-server")
		}

		// receive an end message from remote
		if err := pbpipeline.GetMessageErrorInfo(m); err != nil {
			return err
		}

		// end message
		if m.IsEndMessage() {
			return nil
		}

		// normal receive
		if dataBuffer == nil {
			dataBuffer = m.Data
		} else {
			dataBuffer = append(dataBuffer, m.Data...)
		}

		switch m.GetSid() {
		case pbpipeline.WaitingNext:
			continue
		case pbpipeline.Last:
			if m.Checksum != crc32.ChecksumIEEE(dataBuffer) {
				return moerr.NewInternalError(proc.Ctx, "Packages delivered by morpc is broken")
			}
			bat, err := decodeBatch(proc.Mp(), nil, dataBuffer)
			if err != nil {
				return err
			}
			if forwardCh == nil {
				// used for delete
				proc.SetInputBatch(bat)
			} else {
				// used for BroadCastJoin
				forwardCh <- bat
			}
			dataBuffer = nil
		}
	}
}

func (s *Scope) replace(c *Compile) error {
	tblName := s.Plan.GetQuery().Nodes[0].ReplaceCtx.TableDef.Name
	deleteCond := s.Plan.GetQuery().Nodes[0].ReplaceCtx.DeleteCond

	delAffectedRows := uint64(0)
	if deleteCond != "" {
		result, err := c.runSqlWithResult(fmt.Sprintf("delete from %s where %s", tblName, deleteCond))
		if err != nil {
			return err
		}
		delAffectedRows = result.AffectedRows
	}
	result, err := c.runSqlWithResult("insert " + c.sql[7:])
	if err != nil {
		return err
	}
	c.addAffectedRows(result.AffectedRows + delAffectedRows)
	return nil
}
