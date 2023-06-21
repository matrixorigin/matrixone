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
	"hash/crc32"
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
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

func DebugPrintScope(prefix []byte, ss []*Scope) {
	for _, s := range ss {
		DebugPrintScope(append(prefix, '\t'), s.PreScopes)
		p := pipeline.NewMerge(s.Instructions, nil)
		logutil.Debugf("%s:%v %v", prefix, s.Magic, p)
	}
}

// Run read data from storage engine and run the instructions of scope.
func (s *Scope) Run(c *Compile) (err error) {
	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)
	// DataSource == nil specify the empty scan
	if s.DataSource == nil {
		p := pipeline.New(nil, s.Instructions, s.Reg)
		if _, err = p.ConstRun(nil, s.Proc); err != nil {
			return err
		}
	} else {
		p := pipeline.New(s.DataSource.Attributes, s.Instructions, s.Reg)
		if s.DataSource.Bat != nil {
			if _, err = p.ConstRun(s.DataSource.Bat, s.Proc); err != nil {
				return err
			}
		} else {
			if _, err = p.Run(s.DataSource.R, s.Proc); err != nil {
				return err
			}
		}
	}

	return nil
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
	for _, scope := range s.PreScopes {
		wg.Add(1)
		switch scope.Magic {
		case Normal:
			go func(cs *Scope) {
				errChan <- cs.Run(c)
				wg.Done()
			}(scope)
		case Merge, MergeInsert:
			go func(cs *Scope) {
				errChan <- cs.MergeRun(c)
				wg.Done()
			}(scope)
		case Remote:
			go func(cs *Scope) {
				errChan <- cs.RemoteRun(c)
				wg.Done()
			}(scope)
		case Parallel:
			go func(cs *Scope) {
				errChan <- cs.ParallelRun(c, cs.IsRemote)
				wg.Done()
			}(scope)
		case Pushdown:
			go func(cs *Scope) {
				errChan <- cs.PushdownRun()
				wg.Done()
			}(scope)
		}
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
		return err
	}
	// check sub-goroutine's error
	if errReceiveChan == nil {
		// check sub-goroutine's error
		for i := 0; i < len(s.PreScopes); i++ {
			if err := <-errChan; err != nil {
				return err
			}
		}
		return nil
	}

	slen := len(s.PreScopes)
	rlen := len(s.RemoteReceivRegInfos)
	for {
		select {
		case err := <-errChan:
			if err != nil {
				return err
			}
			slen--
		case err := <-errReceiveChan:
			if err != nil {
				return err
			}
			rlen--
		}

		if slen == 0 && rlen == 0 {
			return nil
		}
	}
}

// RemoteRun send the scope to a remote node for execution.
// if no target node information, just execute it at local.
func (s *Scope) RemoteRun(c *Compile) error {
	// if send to itself, just run it parallel at local.
	if len(s.NodeInfo.Addr) == 0 || !cnclient.IsCNClientReady() ||
		len(c.addr) == 0 || isSameCN(c.addr, s.NodeInfo.Addr) {
		return s.ParallelRun(c, s.IsRemote)
	}

	runtime.ProcessLevelRuntime().Logger().
		Debug("remote run pipeline",
			zap.String("local-address", c.addr),
			zap.String("remote-address", s.NodeInfo.Addr))
	err := s.remoteRun(c)
	// tell connect operator that it's over
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*connector.Argument)
	arg.Free(s.Proc, err != nil)
	return err
}

// ParallelRun try to execute the scope in parallel way.
func (s *Scope) ParallelRun(c *Compile, remote bool) error {
	if logutil.GetSkip1Logger().Core().Enabled(zap.InfoLevel) {
		logutil.Debugf("---->ParallelRun---> %s", DebugShowScopes([]*Scope{s}))
	}
	//fmt.Printf("---->ParallelRun---> %s \n", DebugShowScopes([]*Scope{s}))

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
	switch {
	case remote:
		var err error
		ctx := c.ctx
		if util.TableIsClusterTable(s.DataSource.TableDef.GetTableType()) {
			ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
		}
		if s.DataSource.AccountId != nil {
			ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(s.DataSource.AccountId.GetTenantId()))
		}
		rds, err = c.e.NewBlockReader(ctx, mcpu, s.DataSource.Timestamp, s.DataSource.Expr,
			s.NodeInfo.Data, s.DataSource.TableDef)
		if err != nil {
			return err
		}
		s.NodeInfo.Data = nil

	case s.NodeInfo.Rel != nil:
		var err error

		if len(s.DataSource.RuntimeFilterReceivers) > 0 {
			exprs := make([]*plan.Expr, 0, len(s.DataSource.RuntimeFilterReceivers))
			filters := make([]*pbpipeline.RuntimeFilter, 0, len(exprs))

			for _, receiver := range s.DataSource.RuntimeFilterReceivers {
				select {
				case <-s.Proc.Ctx.Done():
					return nil

				case filter := <-receiver.Chan:
					if filter == nil {
						exprs = nil
						s.NodeInfo.Data = s.NodeInfo.Data[:0]
						break
					}
					if filter.Typ == pbpipeline.RuntimeFilter_NO_FILTER {
						continue
					}

					exprs = append(exprs, receiver.Spec.Expr)
					filters = append(filters, filter)
				}
			}

			if len(exprs) > 0 {
				s.NodeInfo.Data, err = s.NodeInfo.Rel.ApplyRuntimeFilters(c.ctx, s.NodeInfo.Data, exprs, filters)
				if err != nil {
					return err
				}
			}
		}

		if rds, err = s.NodeInfo.Rel.NewReader(c.ctx, mcpu, s.DataSource.Expr, s.NodeInfo.Data); err != nil {
			return err
		}
		s.NodeInfo.Data = nil
	//FIXME:: s.NodeInfo.Rel == nil, partition table?
	default:
		var err error
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
		rel, err = db.Relation(ctx, s.DataSource.RelationName)
		if err != nil {
			var e error // avoid contamination of error messages
			db, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, s.Proc.TxnOperator)
			if e != nil {
				return e
			}
			rel, e = db.Relation(c.ctx, engine.GetTempTableName(s.DataSource.SchemaName, s.DataSource.RelationName))
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
				subrel, err := db.Relation(c.ctx, relName)
				if err != nil {
					return err
				}
				memRds, err := subrel.NewReader(c.ctx, mcpu, s.DataSource.Expr, dirtyRanges[num])
				if err != nil {
					return err
				}
				rds = append(rds, memRds...)
			}
			//for num, r := range dirtyRanges {
			//	subrel, err := db.Relation(c.ctx, s.DataSource.PartitionRelationNames[num])
			//	if err != nil {
			//		return err
			//	}
			//	memRds, err := subrel.NewReader(c.ctx, mcpu, s.DataSource.Expr, r)
			//	if err != nil {
			//		return err
			//	}
			//	rds = append(rds, memRds...)
			//}
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
	newScope := newParallelScope(s, ss)
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
			s.Proc.Reg.InputBatch = bat
			_, err = vm.Run(s.Instructions, s.Proc)
			s.Proc.Cancel()
			return err
		}
		if bat.Length() == 0 {
			continue
		}
		s.Proc.Reg.InputBatch = bat
		if end, err = vm.Run(s.Instructions, s.Proc); err != nil || end {
			return err
		}
	}
}

func (s *Scope) JoinRun(c *Compile) error {
	mcpu := s.NodeInfo.Mcpu
	if mcpu < 1 {
		mcpu = 1
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
	s = newParallelScope(s, ss)

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
	s.PreScopes = append(s.PreScopes, probe_scope)
	s.PreScopes = append(s.PreScopes, build_scope)

	return s.MergeRun(c)
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
		bat.InitZsOne(1)
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
	newScope := newParallelScope(s, ss)

	return newScope.MergeRun(c)
}

func newParallelScope(s *Scope, ss []*Scope) *Scope {
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
	return s
}

func (s *Scope) appendInstruction(in vm.Instruction) {
	if !s.IsEnd {
		s.Instructions = append(s.Instructions, in)
	}
}

/*
func dupScopeList(ss []*Scope) []*Scope {
	rs := make([]*Scope, len(ss))
	regMap := make(map[*process.WaitRegister]*process.WaitRegister)
	var err error
	for i := range ss {
		rs[i], err = copyScope(ss[i], regMap)
		if err != nil {
			return nil
		}
	}

	for i := range ss {
		err = fillInstructionsByCopyScope(rs[i], ss[i], regMap)
		if err != nil {
			return nil
		}
	}
	return rs
}

func copyScope(srcScope *Scope, regMap map[*process.WaitRegister]*process.WaitRegister) (*Scope, error) {
	var err error
	newScope := &Scope{
		Magic:        srcScope.Magic,
		IsJoin:       srcScope.IsJoin,
		IsEnd:        srcScope.IsEnd,
		IsRemote:     srcScope.IsRemote,
		Plan:         srcScope.Plan,
		PreScopes:    make([]*Scope, len(srcScope.PreScopes)),
		Instructions: make([]vm.Instruction, len(srcScope.Instructions)),
		NodeInfo: engine.Node{
			Rel:  srcScope.NodeInfo.Rel,
			Mcpu: srcScope.NodeInfo.Mcpu,
			Id:   srcScope.NodeInfo.Id,
			Addr: srcScope.NodeInfo.Addr,
			Data: make([][]byte, len(srcScope.NodeInfo.Data)),
		},
		RemoteReceivRegInfos: srcScope.RemoteReceivRegInfos,
	}

	// copy node.Data
	copy(newScope.NodeInfo.Data, srcScope.NodeInfo.Data)

	if srcScope.DataSource != nil {
		newScope.DataSource = &Source{
			PushdownId:   srcScope.DataSource.PushdownId,
			PushdownAddr: srcScope.DataSource.PushdownAddr,
			SchemaName:   srcScope.DataSource.SchemaName,
			RelationName: srcScope.DataSource.RelationName,
			Attributes:   srcScope.DataSource.Attributes,
			Timestamp: timestamp.Timestamp{
				PhysicalTime: srcScope.DataSource.Timestamp.PhysicalTime,
				LogicalTime:  srcScope.DataSource.Timestamp.LogicalTime,
				NodeID:       srcScope.DataSource.Timestamp.NodeID,
			},
			// read only.
			Expr:     srcScope.DataSource.Expr,
			TableDef: srcScope.DataSource.TableDef,
		}

		// IF const run.
		if srcScope.DataSource.Bat != nil {
			newScope.DataSource.Bat, _ = constructValueScanBatch(context.TODO(), nil, nil)
		}
	}

	newScope.Proc = process.NewFromProc(srcScope.Proc, srcScope.Proc.Ctx, len(srcScope.Proc.Reg.MergeReceivers))
	for i := range srcScope.Proc.Reg.MergeReceivers {
		regMap[srcScope.Proc.Reg.MergeReceivers[i]] = newScope.Proc.Reg.MergeReceivers[i]
	}

	//copy preScopes.
	for i := range srcScope.PreScopes {
		newScope.PreScopes[i], err = copyScope(srcScope.PreScopes[i], regMap)
		if err != nil {
			return nil, err
		}
	}
	return newScope, nil
}

func fillInstructionsByCopyScope(targetScope *Scope, srcScope *Scope,
	regMap map[*process.WaitRegister]*process.WaitRegister) error {
	var err error

	for i := range srcScope.PreScopes {
		if err = fillInstructionsByCopyScope(targetScope.PreScopes[i], srcScope.PreScopes[i], regMap); err != nil {
			return err
		}
	}

	for i := range srcScope.Instructions {
		targetScope.Instructions[i] = dupInstruction(&srcScope.Instructions[i], regMap)
	}
	return nil
}
*/

func (s *Scope) notifyAndReceiveFromRemote(errChan chan error) {
	for i := range s.RemoteReceivRegInfos {
		op := &s.RemoteReceivRegInfos[i]
		go func(info *RemoteReceivRegInfo, reg *process.WaitRegister) {
			streamSender, errStream := cnclient.GetStreamSender(info.FromAddr)
			if errStream != nil {
				close(reg.Ch)
				errChan <- errStream
				return
			}
			defer func(streamSender morpc.Stream) {
				close(reg.Ch)
				_ = streamSender.Close(true)
			}(streamSender)

			message := cnclient.AcquireMessage()
			{
				message.Id = streamSender.ID()
				message.Cmd = pbpipeline.PrepareDoneNotifyMessage
				message.Sid = pbpipeline.Last
				message.Uuid = info.Uuid[:]
			}
			if errSend := streamSender.Send(s.Proc.Ctx, message); errSend != nil {
				errChan <- errSend
				return
			}

			messagesReceive, errReceive := streamSender.Receive()
			if errReceive != nil {
				errChan <- errReceive
				return
			}
			var ch chan *batch.Batch
			if reg != nil {
				ch = reg.Ch
			}
			if err := receiveMsgAndForward(s.Proc, messagesReceive, ch); err != nil {
				errChan <- err
				return
			}
			reg.Ch <- nil
			errChan <- nil
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
		dataBuffer = append(dataBuffer, m.Data...)
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
