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
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/generate_series"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopleft"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsingle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unnest"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
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
	p := pipeline.NewMerge(s.Instructions, s.Reg)
	if _, err := p.MergeRun(s.Proc); err != nil {
		return err
	}
	// check sub-goroutine's error
	for i := 0; i < len(s.PreScopes); i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}
	return nil
}

// RemoteRun send the scope to a remote node for execution.
// if no target node information, just execute it at local.
func (s *Scope) RemoteRun(c *Compile) error {
	// if send to itself, just run it parallel at local.
	if len(s.NodeInfo.Addr) == 0 || !cnclient.IsCNClientReady() ||
		s.NodeInfo.Addr == c.addr || len(c.addr) == 0 {
		return s.ParallelRun(c, s.IsRemote)
	}

	err := s.remoteRun(c)
	// tell connect operator that it's over
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*connector.Argument)
	sendToConnectOperator(arg, nil)
	return err
}

// ParallelRun try to execute the scope in parallel way.
func (s *Scope) ParallelRun(c *Compile, remote bool) error {
	var rds []engine.Reader

	if s.IsJoin {
		return s.JoinRun(c)
	}
	if s.DataSource == nil {
		return s.MergeRun(c)
	}
	mcpu := s.NodeInfo.Mcpu
	if remote {
		var err error

		rds, err = c.e.NewBlockReader(c.ctx, mcpu, s.DataSource.Timestamp, s.DataSource.Expr,
			s.NodeInfo.Data, s.DataSource.TableDef)
		if err != nil {
			return err
		}
	} else {
		var err error

		db, err := c.e.Database(c.ctx, s.DataSource.SchemaName, s.Proc.TxnOperator)
		if err != nil {
			return err
		}
		rel, err := db.Relation(c.ctx, s.DataSource.RelationName)
		if err != nil {
			return err
		}
		if rds, err = rel.NewReader(c.ctx, mcpu, s.DataSource.Expr, s.NodeInfo.Data); err != nil {
			return err
		}
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
		}
		ss[i].Proc = process.NewWithAnalyze(s.Proc, c.ctx, 0, c.anal.Nodes())
	}
	newScope := newParallelScope(c, s, ss)
	return newScope.MergeRun(c)
}

func (s *Scope) PushdownRun(c *Compile) error {
	var end bool // exist flag
	var err error

	reg := srv.GetConnector(s.DataSource.PushdownId)
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
	chp := s.PreScopes
	for i := range chp {
		chp[i].IsEnd = true
	}
	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = &Scope{
			Magic: Merge,
		}
		ss[i].Proc = process.NewWithAnalyze(s.Proc, c.ctx, 2, c.anal.Nodes())
		ss[i].Proc.Reg.MergeReceivers[1].Ch = make(chan *batch.Batch, 10)
	}
	left, right := c.newLeftScope(s, ss), c.newRightScope(s, ss)
	s = newParallelScope(c, s, ss)
	s.PreScopes = append(s.PreScopes, chp...)
	s.PreScopes = append(s.PreScopes, left)
	s.PreScopes = append(s.PreScopes, right)
	return s.MergeRun(c)
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
					Op:  vm.Top,
					Idx: in.Idx,
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
					Op:  vm.Order,
					Idx: in.Idx,
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
					Op:  vm.Limit,
					Idx: in.Idx,
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
				Op: vm.MergeGroup,
				Arg: &mergegroup.Argument{
					NeedEval: false,
				},
			}
			for i := range ss {
				ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
					Op: vm.Group,
					Arg: &group.Argument{
						Aggs:  arg.Aggs,
						Exprs: arg.Exprs,
						Types: arg.Types,
					},
				})
			}
		case vm.Offset:
			flg = true
			arg := in.Arg.(*offset.Argument)
			s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
			s.Instructions[0] = vm.Instruction{
				Op: vm.MergeOffset,
				Arg: &mergeoffset.Argument{
					Offset: arg.Offset,
				},
			}
			for i := range ss {
				ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
					Op: vm.Offset,
					Arg: &offset.Argument{
						Offset: arg.Offset,
					},
				})
			}
		default:
			for i := range ss {
				ss[i].Instructions = append(ss[i].Instructions, dupInstruction(in))
			}
		}
	}
	if !flg {
		for i := range ss {
			ss[i].Instructions = ss[i].Instructions[:len(ss[i].Instructions)-1]
		}
		s.Instructions[0] = vm.Instruction{
			Op:  vm.Merge,
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
					Reg: s.Proc.Reg.MergeReceivers[i],
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

func dupScope(s *Scope) *Scope {
	data, err := encodeScope(s)
	if err != nil {
		return nil
	}
	rs, err := decodeScope(data, s.Proc, s.IsRemote)
	if err != nil {
		return nil
	}
	return rs
}

func dupScopeList(ss []*Scope) []*Scope {
	rs := make([]*Scope, len(ss))
	for i := range rs {
		rs[i] = dupScope(ss[i])
	}
	return rs
}

func dupScope2(s *Scope) *Scope {
	ctx := &scopeContext{
		id:     0,
		parent: nil,
		regs:   make(map[*process.WaitRegister]int32),
	}
	ctx.root = ctx

	newScope, err := copyScope(s, nil, nil)
	if err != nil {
		return nil
	}

	return newScope
}

func copyScope(srcScope *Scope, parentProc *process.Process, analNodes []*process.AnalyzeInfo) (*Scope, error) {
	var err error
	newScope := &Scope{
		Magic:        srcScope.Magic,
		IsJoin:       srcScope.IsJoin,
		IsEnd:        srcScope.IsEnd,
		IsRemote:     srcScope.IsRemote,
		Plan:         srcScope.Plan, // we do not deep copy plan. because it's read only
		PreScopes:    make([]*Scope, len(srcScope.PreScopes)),
		Instructions: make([]vm.Instruction, len(srcScope.Instructions)),
		NodeInfo: engine.Node{
			Mcpu: srcScope.NodeInfo.Mcpu,
			Id:   srcScope.NodeInfo.Id,
			Addr: srcScope.NodeInfo.Addr,
			Data: make([][]byte, len(srcScope.NodeInfo.Data)),
		},
		// Proc:         s.Proc,
		// Reg:          &process.WaitRegister{},
	}

	// copy node.Data
	for i := range srcScope.NodeInfo.Data {
		newScope.NodeInfo.Data[i] = []byte(srcScope.NodeInfo.Data[i])
	}

	if srcScope.DataSource != nil {
		newScope.DataSource = &Source{
			PushdownId:   srcScope.DataSource.PushdownId,
			PushdownAddr: srcScope.DataSource.PushdownAddr,
			SchemaName:   srcScope.DataSource.SchemaName,
			RelationName: srcScope.DataSource.RelationName,
			Attributes:   make([]string, len(srcScope.DataSource.Attributes)),
			Expr:         plan.DeepCopyExpr(srcScope.DataSource.Expr),
			TableDef:     plan.DeepCopyTableDef(srcScope.DataSource.TableDef),
			Timestamp: timestamp.Timestamp{
				PhysicalTime: srcScope.DataSource.Timestamp.PhysicalTime,
				LogicalTime:  srcScope.DataSource.Timestamp.LogicalTime,
				NodeID:       srcScope.DataSource.Timestamp.NodeID,
			},
		}
		copy(newScope.DataSource.Attributes, srcScope.DataSource.Attributes)

		if srcScope.DataSource.Bat != nil {
			newScope.DataSource.Bat = &batch.Batch{
				Ro:    srcScope.DataSource.Bat.Ro,
				Cnt:   srcScope.DataSource.Bat.Cnt,
				Attrs: make([]string, len(srcScope.DataSource.Bat.Attrs)),
				Zs:    make([]int64, len(srcScope.DataSource.Bat.Zs)),
				Vecs:  make([]*vector.Vector, len(srcScope.DataSource.Bat.Vecs)),
				// Aggs:  make([]agg.Agg, len(s.DataSource.Bat.Aggs)),  //it's an interface  need copy ？
				// Ht:    nil,  // it's a hashtable, need copy ？
			}
			copy(newScope.DataSource.Bat.Attrs, srcScope.DataSource.Bat.Attrs)
			copy(newScope.DataSource.Bat.Zs, srcScope.DataSource.Bat.Zs)

			// copy vecs
			for i := 0; i < len(srcScope.DataSource.Bat.Vecs); i++ {
				newScope.DataSource.Bat.Vecs[i], err = vector.Dup(srcScope.DataSource.Bat.Vecs[i], srcScope.Proc.Mp())
				if err != nil {
					return nil, err
				}
			}
		}

	}

	newProc := process.NewWithAnalyze(srcScope.Proc, srcScope.Proc.Ctx, int(len(srcScope.PreScopes)), nil)
	newScope.Proc = newProc

	//copy prescopes
	for i := range srcScope.PreScopes {
		newScope.PreScopes[i], err = copyScope(srcScope.PreScopes[i], newProc, analNodes)
		if err != nil {
			return nil, err
		}
	}

	for i, src := range srcScope.Instructions {
		in := vm.Instruction{
			Op:  src.Op,
			Idx: src.Idx,
		}
		switch t := src.Arg.(type) {
		case *anti.Argument:
			arg := &anti.Argument{
				Ibucket:    t.Ibucket,
				Nbucket:    t.Nbucket,
				Result:     t.Result,
				Typs:       make([]types.Type, len(t.Typs)),
				Cond:       plan.DeepCopyExpr(t.Cond),
				Conditions: make([][]*plan.Expr, len(t.Conditions)),
			}
			copy(arg.Typs, t.Typs)
			for i, es := range t.Conditions {
				arg.Conditions[i] = make([]*plan.Expr, len(es))
				for j, e := range es {
					arg.Conditions[i][j] = plan.DeepCopyExpr(e)
				}
			}
			in.Arg = arg

		case *dispatch.Argument:
			arg := &dispatch.Argument{
				All:  t.All,
				Regs: make([]*process.WaitRegister, len(t.Regs)),
			}
			// need confirm
			for i := range t.Regs {
				// id := srv.RegistConnector(parentProc.Reg.MergeReceivers[i])
				arg.Regs[i] = parentProc.Reg.MergeReceivers[i]
			}
			in.Arg = arg

		case *group.Argument:
			arg := &group.Argument{
				NeedEval: t.NeedEval,
				Ibucket:  t.Ibucket,
				Nbucket:  t.Nbucket,
				Exprs:    make([]*plan.Expr, len(t.Exprs)),
				Types:    make([]types.Type, len(t.Types)),
				Aggs:     make([]agg.Aggregate, len(t.Aggs)),
			}
			copy(arg.Types, t.Types)
			for i, e := range t.Exprs {
				arg.Exprs[i] = plan.DeepCopyExpr(e)
			}
			for i, a := range t.Aggs {
				arg.Aggs[i] = agg.Aggregate{
					Op:   a.Op,
					Dist: a.Dist,
					E:    plan.DeepCopyExpr(a.E),
				}
			}
			in.Arg = arg

		case *join.Argument:
			arg := &join.Argument{
				Ibucket:    t.Ibucket,
				Nbucket:    t.Nbucket,
				Result:     make([]colexec.ResultPos, len(t.Result)),
				Typs:       make([]types.Type, len(t.Typs)),
				Cond:       plan.DeepCopyExpr(t.Cond),
				Conditions: make([][]*plan.Expr, len(t.Conditions)),
			}
			copy(arg.Result, t.Result)
			copy(arg.Typs, t.Typs)
			for i, es := range t.Conditions {
				arg.Conditions[i] = make([]*plan.Expr, len(es))
				for j, e := range es {
					arg.Conditions[i][j] = plan.DeepCopyExpr(e)
				}
			}
			in.Arg = arg

		case *left.Argument:
			arg := &left.Argument{
				Ibucket:    t.Ibucket,
				Nbucket:    t.Nbucket,
				Result:     make([]colexec.ResultPos, len(t.Result)),
				Typs:       make([]types.Type, len(t.Typs)),
				Cond:       plan.DeepCopyExpr(t.Cond),
				Conditions: make([][]*plan.Expr, len(t.Conditions)),
			}
			copy(arg.Result, t.Result)
			copy(arg.Typs, t.Typs)
			for i, es := range t.Conditions {
				arg.Conditions[i] = make([]*plan.Expr, len(es))
				for j, e := range es {
					arg.Conditions[i][j] = plan.DeepCopyExpr(e)
				}
			}
			in.Arg = arg

		case *limit.Argument:
			arg := &limit.Argument{
				Seen:  t.Seen,
				Limit: t.Limit,
			}
			in.Arg = arg

		case *loopanti.Argument:
			arg := &loopanti.Argument{
				Result: make([]int32, len(t.Result)),
				Cond:   plan.DeepCopyExpr(t.Cond),
				Typs:   make([]types.Type, len(t.Typs)),
			}
			copy(arg.Result, t.Result)
			copy(arg.Typs, t.Typs)
			in.Arg = arg

		case *loopjoin.Argument:
			arg := &loopjoin.Argument{
				Result: make([]colexec.ResultPos, len(t.Result)),
				Cond:   plan.DeepCopyExpr(t.Cond),
				Typs:   make([]types.Type, len(t.Typs)),
			}
			copy(arg.Result, t.Result)
			copy(arg.Typs, t.Typs)
			in.Arg = arg

		case *loopleft.Argument:
			arg := &loopleft.Argument{
				Result: make([]colexec.ResultPos, len(t.Result)),
				Cond:   plan.DeepCopyExpr(t.Cond),
				Typs:   make([]types.Type, len(t.Typs)),
			}
			copy(arg.Result, t.Result)
			copy(arg.Typs, t.Typs)
			in.Arg = arg

		case *loopsemi.Argument:
			arg := &loopsemi.Argument{
				Result: make([]int32, len(t.Result)),
				Cond:   plan.DeepCopyExpr(t.Cond),
				Typs:   make([]types.Type, len(t.Typs)),
			}
			copy(arg.Result, t.Result)
			copy(arg.Typs, t.Typs)
			in.Arg = arg

		case *loopsingle.Argument:
			arg := &loopsingle.Argument{
				Result: make([]colexec.ResultPos, len(t.Result)),
				Cond:   plan.DeepCopyExpr(t.Cond),
				Typs:   make([]types.Type, len(t.Typs)),
			}
			copy(arg.Result, t.Result)
			copy(arg.Typs, t.Typs)
			in.Arg = arg

		case *offset.Argument:
			arg := &offset.Argument{
				Seen:   t.Seen,
				Offset: t.Offset,
			}
			in.Arg = arg

		case *order.Argument:
			arg := &order.Argument{
				Fs: make([]*plan.OrderBySpec, len(t.Fs)),
			}
			for i, f := range t.Fs {
				arg.Fs[i] = plan.DeepCopyOrderBy(f)
			}
			in.Arg = arg

		case *product.Argument:
			arg := &product.Argument{
				Result: make([]colexec.ResultPos, len(t.Result)),
				Typs:   make([]types.Type, len(t.Typs)),
			}
			copy(arg.Result, t.Result)
			copy(arg.Typs, t.Typs)
			in.Arg = arg

		case *projection.Argument:
			arg := &projection.Argument{
				Es: make([]*plan.Expr, len(t.Es)),
			}
			for i, e := range t.Es {
				arg.Es[i] = plan.DeepCopyExpr(e)
			}
			in.Arg = arg

		case *restrict.Argument:
			arg := &restrict.Argument{
				E: plan.DeepCopyExpr(t.E),
			}
			in.Arg = arg

		case *semi.Argument:
			arg := &semi.Argument{
				Ibucket:    t.Ibucket,
				Nbucket:    t.Nbucket,
				Result:     make([]int32, len(t.Result)),
				Typs:       make([]types.Type, len(t.Typs)),
				Cond:       plan.DeepCopyExpr(t.Cond),
				Conditions: make([][]*plan.Expr, len(t.Conditions)),
			}
			copy(arg.Result, t.Result)
			copy(arg.Typs, t.Typs)
			for i, es := range t.Conditions {
				arg.Conditions[i] = make([]*plan.Expr, len(es))
				for j, e := range es {
					arg.Conditions[i][j] = plan.DeepCopyExpr(e)
				}
			}
			in.Arg = arg

		case *single.Argument:
			arg := &single.Argument{
				Ibucket:    t.Ibucket,
				Nbucket:    t.Nbucket,
				Result:     make([]colexec.ResultPos, len(t.Result)),
				Typs:       make([]types.Type, len(t.Typs)),
				Cond:       plan.DeepCopyExpr(t.Cond),
				Conditions: make([][]*plan.Expr, len(t.Conditions)),
			}
			copy(arg.Result, t.Result)
			copy(arg.Typs, t.Typs)
			for i, es := range t.Conditions {
				arg.Conditions[i] = make([]*plan.Expr, len(es))
				for j, e := range es {
					arg.Conditions[i][j] = plan.DeepCopyExpr(e)
				}
			}
			in.Arg = arg

		case *top.Argument:
			arg := &top.Argument{
				Limit: t.Limit,
				Fs:    make([]*plan.OrderBySpec, len(t.Fs)),
			}
			for i, f := range t.Fs {
				arg.Fs[i] = plan.DeepCopyOrderBy(f)
			}
			in.Arg = arg

		case *intersect.Argument: // 1
			arg := &intersect.Argument{
				IBucket: t.IBucket,
				NBucket: t.NBucket,
			}
			in.Arg = arg

		case *minus.Argument: // 2
			arg := &minus.Argument{
				IBucket: t.IBucket,
				NBucket: t.NBucket,
			}
			in.Arg = arg

		case *intersectall.Argument:
			arg := &intersectall.Argument{
				IBucket: t.IBucket,
				NBucket: t.NBucket,
			}
			in.Arg = arg

		case *merge.Argument:
			arg := &merge.Argument{}
			in.Arg = arg

		case *mergegroup.Argument:
			arg := &mergegroup.Argument{
				NeedEval: t.NeedEval,
			}
			in.Arg = arg

		case *mergelimit.Argument:
			arg := &mergelimit.Argument{
				Limit: t.Limit,
			}
			in.Arg = arg

		case *mergeoffset.Argument:
			arg := &mergeoffset.Argument{
				Offset: t.Offset,
			}
			in.Arg = arg

		case *mergetop.Argument:
			arg := &mergetop.Argument{
				Limit: t.Limit,
				Fs:    make([]*plan.OrderBySpec, len(t.Fs)),
			}
			for i, f := range t.Fs {
				arg.Fs[i] = plan.DeepCopyOrderBy(f)
			}
			in.Arg = arg

		case *mergeorder.Argument:
			arg := &mergeorder.Argument{
				Fs: make([]*plan.OrderBySpec, len(t.Fs)),
			}
			for i, f := range t.Fs {
				arg.Fs[i] = plan.DeepCopyOrderBy(f)
			}
			in.Arg = arg

		case *connector.Argument:
			arg := &connector.Argument{
				Reg: &process.WaitRegister{},
			}
			// need confirm
			arg.Reg = parentProc.Reg.MergeReceivers[0]
			in.Arg = arg

		case *mark.Argument:
			arg := &mark.Argument{
				Ibucket:      t.Ibucket,
				Nbucket:      t.Nbucket,
				OutputNull:   t.OutputNull,
				OutputMark:   t.OutputMark,
				MarkMeaning:  t.MarkMeaning,
				OutputAnyway: t.OutputAnyway,
				Cond:         plan.DeepCopyExpr(t.Cond),
				Result:       make([]int32, len(t.Result)),
				Conditions:   make([][]*plan.Expr, len(t.Conditions)),
				Typs:         make([]types.Type, len(t.Typs)),
				OnList:       make([]*plan.Expr, len(t.OnList)),
			}
			copy(arg.Result, t.Result)
			copy(arg.Typs, t.Typs)
			for i, es := range t.Conditions {
				arg.Conditions[i] = make([]*plan.Expr, len(es))
				for j, e := range es {
					arg.Conditions[i][j] = plan.DeepCopyExpr(e)
				}
			}
			for i, e := range t.OnList {
				arg.OnList[i] = plan.DeepCopyExpr(e)
			}
			in.Arg = arg

		case *unnest.Argument:
			arg := &unnest.Argument{
				Es: &unnest.Param{
					ColName:  t.Es.ColName,
					Attrs:    make([]string, len(t.Es.Attrs)),
					Cols:     make([]*plan.ColDef, len(t.Es.Cols)),
					ExprList: make([]*plan.Expr, len(t.Es.ExprList)),
				},
			}
			for i, e := range t.Es.ExprList {
				arg.Es.ExprList[i] = plan.DeepCopyExpr(e)
			}
			for i, col := range t.Es.Cols {
				arg.Es.Cols[i] = plan.DeepCopyColDef(col)
			}
			in.Arg = arg

		case *generate_series.Argument:
			arg := &generate_series.Argument{
				Es: &generate_series.Param{
					Attrs:    make([]string, len(t.Es.Attrs)),
					ExprList: make([]*plan.Expr, len(t.Es.ExprList)),
				},
			}
			for i, e := range t.Es.ExprList {
				arg.Es.ExprList[i] = plan.DeepCopyExpr(e)
			}
			in.Arg = arg

		case *hashbuild.Argument:
			arg := &hashbuild.Argument{
				NeedExpr:    t.NeedExpr,
				NeedHashMap: t.NeedHashMap,
				Ibucket:     t.Ibucket,
				Nbucket:     t.Nbucket,
				Typs:        make([]types.Type, len(t.Typs)),
				Conditions:  make([]*plan.Expr, len(t.Conditions)),
			}
			copy(arg.Typs, t.Typs)
			for i, e := range t.Conditions {
				arg.Conditions[i] = plan.DeepCopyExpr(e)
			}
			in.Arg = arg

		case *external.Argument:
			arg := &external.Argument{
				Es: &external.ExternalParam{
					CreateSql:     t.Es.CreateSql,
					Ctx:           t.Es.Ctx,
					IgnoreLine:    t.Es.IgnoreLine,
					IgnoreLineTag: t.Es.IgnoreLineTag,
					Fileparam: &external.ExternalFileparam{
						End:       t.Es.Fileparam.End,
						FileCnt:   t.Es.Fileparam.FileCnt,
						FileFin:   t.Es.Fileparam.FileFin,
						FileIndex: t.Es.Fileparam.FileIndex,
					},
					Attrs:         make([]string, len(t.Es.Attrs)),
					FileList:      make([]string, len(t.Es.FileList)),
					Cols:          make([]*plan.ColDef, len(t.Es.Cols)),
					Name2ColIndex: make(map[string]int32),
				},
			}
			copy(arg.Es.Attrs, t.Es.Attrs)
			copy(arg.Es.FileList, t.Es.FileList)
			for i, col := range t.Es.Cols {
				arg.Es.Cols[i] = plan.DeepCopyColDef(col)
			}
			for k, v := range t.Es.Name2ColIndex {
				arg.Es.Name2ColIndex[k] = v
			}
			in.Arg = arg
		}
		newScope.Instructions[i] = in
	}

	return newScope, nil
}
