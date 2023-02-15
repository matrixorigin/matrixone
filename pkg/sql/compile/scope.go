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
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pbpipeline "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func PrintScope(prefix []byte, ss []*Scope) {
	for _, s := range ss {
		PrintScope(append(prefix, '\t'), s.PreScopes)
		p := pipeline.NewMerge(s.Instructions, nil)
		logutil.Infof("%s:%v %v", prefix, s.Magic, p)
	}
}

// Run read data from storage engine and run the instructions of scope.
func (s *Scope) Run(c *Compile) (err error) {
	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)
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
	return nil
}

// MergeRun range and run the scope's pre-scopes by go-routine, and finally run itself to do merge work.
func (s *Scope) MergeRun(c *Compile) error {
	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)
	errChan := make(chan error, len(s.PreScopes))

	for i := range s.PreScopes {
		switch s.PreScopes[i].Magic {
		case Normal:
			go func(cs *Scope) {
				var err error
				defer func() {
					errChan <- err
				}()
				err = cs.Run(c)
			}(s.PreScopes[i])
		case Merge:
			go func(cs *Scope) {
				var err error
				defer func() {
					errChan <- err
				}()
				err = cs.MergeRun(c)
			}(s.PreScopes[i])
		case Remote:
			go func(cs *Scope) {
				var err error
				defer func() {
					errChan <- err
				}()
				err = cs.RemoteRun(c)
			}(s.PreScopes[i])
		case Parallel:
			go func(cs *Scope) {
				var err error
				defer func() {
					errChan <- err
				}()
				err = cs.ParallelRun(c, cs.IsRemote)
			}(s.PreScopes[i])
		case Pushdown:
			go func(cs *Scope) {
				var err error
				defer func() {
					errChan <- err
				}()
				err = cs.PushdownRun(c)
			}(s.PreScopes[i])
		}
	}
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
		len(c.addr) == 0 || strings.Split(c.addr, ":")[0] == strings.Split(s.NodeInfo.Addr, ":")[0] {
		return s.ParallelRun(c, s.IsRemote)
	}

	err := s.remoteRun(c)

	// tell connect operator that it's over
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*connector.Argument)
	arg.Free(s.Proc, err != nil)
	return err
}

// ParallelRun try to execute the scope in parallel way.
func (s *Scope) ParallelRun(c *Compile, remote bool) error {
	var rds []engine.Reader

	s.Proc.Ctx = context.WithValue(s.Proc.Ctx, defines.EngineKey{}, c.e)
	if s.IsJoin {
		return s.JoinRun(c)
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
		rds, err = c.e.NewBlockReader(ctx, mcpu, s.DataSource.Timestamp, s.DataSource.Expr,
			s.NodeInfo.Data, s.DataSource.TableDef)
		if err != nil {
			return err
		}
		s.NodeInfo.Data = nil
	case s.NodeInfo.Rel != nil:
		var err error

		if rds, err = s.NodeInfo.Rel.NewReader(c.ctx, mcpu, s.DataSource.Expr, s.NodeInfo.Data); err != nil {
			return err
		}
		s.NodeInfo.Data = nil
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
		if rds, err = rel.NewReader(ctx, mcpu, s.DataSource.Expr, s.NodeInfo.Data); err != nil {
			return err
		}
		s.NodeInfo.Data = nil
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
			},
			NodeInfo: s.NodeInfo,
		}
		ss[i].Proc = process.NewWithAnalyze(s.Proc, c.ctx, 0, c.anal.Nodes())
	}
	newScope := newParallelScope(c, s, ss)
	return newScope.MergeRun(c)
}

func (s *Scope) PushdownRun(c *Compile) error {
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
		ss[i].Proc = process.NewWithAnalyze(s.Proc, c.ctx, 2, c.anal.Nodes())
		ss[i].Proc.Reg.MergeReceivers[1].Ch = make(chan *batch.Batch, 10)
	}
	left_scope, right_scope := c.newLeftScope(s, ss), c.newRightScope(s, ss)
	s = newParallelScope(c, s, ss)

	if isRight {
		channel := make(chan *[]int64)
		for i := range s.PreScopes {
			arg := s.PreScopes[i].Instructions[0].Arg.(*right.Argument)
			arg.Channel = channel
			arg.NumCPU = uint64(mcpu)
			if i == 0 {
				arg.Is_receiver = true
			}
		}
	}
	s.PreScopes = append(s.PreScopes, chp...)
	s.PreScopes = append(s.PreScopes, left_scope)
	s.PreScopes = append(s.PreScopes, right_scope)
	return s.MergeRun(c)
}
func (s *Scope) isRight() bool {
	return s != nil && s.Instructions[0].Op == vm.Right
}

func newParallelScope(c *Compile, s *Scope, ss []*Scope) *Scope {
	var flg bool

	for i, in := range s.Instructions {
		if flg {
			break
		}
		switch in.Op {
		case vm.Top:
			flg = true
			arg := in.Arg.(*top.Argument)
			s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeTop,
				Idx: in.Idx,
				Arg: &mergetop.Argument{
					Fs:    arg.Fs,
					Limit: arg.Limit,
				},
			}
			for i := range ss {
				ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
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
			s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeOrder,
				Idx: in.Idx,
				Arg: &mergeorder.Argument{
					Fs: arg.Fs,
				},
			}
			for i := range ss {
				ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
					Op:      vm.Order,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: &order.Argument{
						Fs: arg.Fs,
					},
				})
			}
		case vm.Limit:
			flg = true
			arg := in.Arg.(*limit.Argument)
			s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeLimit,
				Idx: in.Idx,
				Arg: &mergelimit.Argument{
					Limit: arg.Limit,
				},
			}
			for i := range ss {
				ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
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
			s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeGroup,
				Idx: in.Idx,
				Arg: &mergegroup.Argument{
					NeedEval: false,
				},
			}
			for i := range ss {
				ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
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
			s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
			s.Instructions[0] = vm.Instruction{
				Op:  vm.MergeOffset,
				Idx: in.Idx,
				Arg: &mergeoffset.Argument{
					Offset: arg.Offset,
				},
			}
			for i := range ss {
				ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
					Op:      vm.Offset,
					Idx:     in.Idx,
					IsFirst: in.IsFirst,
					Arg: &offset.Argument{
						Offset: arg.Offset,
					},
				})
			}
		default:
			for i := range ss {
				ss[i].Instructions = append(ss[i].Instructions, dupInstruction(&in, nil))
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

func dupScopeList(ss []*Scope) []*Scope {
	rs := make([]*Scope, len(ss))
	for i := range rs {
		rs[i] = dupScope(ss[i])
	}
	return rs
}

func dupScope(s *Scope) *Scope {
	regMap := make(map[*process.WaitRegister]*process.WaitRegister)

	newScope, err := copyScope(s, regMap)
	if err != nil {
		return nil
	}
	err = fillInstructionsByCopyScope(newScope, s, regMap)
	if err != nil {
		return nil
	}
	return newScope
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

	newScope.Proc = process.NewFromProc(srcScope.Proc, srcScope.Proc.Ctx, len(srcScope.PreScopes))
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

func (s *Scope) notifyAndReceiveFromRemote(errChan chan error) error {
	mp, err := mpool.NewMPool("compile", 0, mpool.NoFixed)
	if err != nil {
		panic(err)
	}

	for _, rr := range s.RemoteReceivRegInfos {
		go func(info RemoteReceivRegInfo, reg *process.WaitRegister, mp *mpool.MPool) {
			c, cancel := context.WithTimeout(context.Background(), time.Second*10000)
			_ = cancel

			streamSender, errStream := cnclient.GetStreamSender(info.FromAddr)
			if errStream != nil {
				errChan <- errStream
				return
			}
			defer func(streamSender morpc.Stream) {
				// TODO: should close the channel or not?
				_ = streamSender.Close()
			}(streamSender)

			message := cnclient.AcquireMessage()
			{
				message.Id = streamSender.ID()
				message.Cmd = pbpipeline.PrepareDoneNotifyMessage
				message.Uuid = info.Uuid[:]
			}
			if errSend := streamSender.Send(c, message); errSend != nil {
				errChan <- errSend
				return
			}

			messagesReceive, errReceive := streamSender.Receive()
			if errReceive != nil {
				errChan <- errReceive
				return
			}

			var val morpc.Message
			var dataBuffer []byte
			for {
				select {
				case <-c.Done():
					errChan <- nil
					return
				case val = <-messagesReceive:
				}

				// TODO: what val = nil means?
				if val == nil {
					reg.Ch <- nil
					close(reg.Ch)
					errChan <- nil
					return
				}

				m := val.(*pbpipeline.Message)

				if m.IsEndMessage() {
					reg.Ch <- nil
					close(reg.Ch)
					errChan <- nil
					return
				}

				dataBuffer = append(dataBuffer, m.Data...)

				if m.BatcWaitingNextToMerge() {
					continue
				}

				bat, err := decodeBatch(mp, dataBuffer)
				if err != nil {
					errChan <- err
					return
				}
				reg.Ch <- bat
				dataBuffer = nil
			}
		}(rr, s.Proc.Reg.MergeReceivers[rr.Idx], mp)
	}

	return nil
}
