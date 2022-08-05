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

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// PrintScope Print is to format scope list
func PrintScope(prefix []byte, ss []*Scope) {
	for _, s := range ss {
		if s.Magic == Merge || s.Magic == Remote {
			PrintScope(append(prefix, '\t'), s.PreScopes)
		}
		p := pipeline.NewMerge(s.Instructions, nil)
		fmt.Printf("%s:%v %v\n", prefix, s.Magic, p)
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
				err = cs.ParallelRun(c)
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

// RemoteRun send the scope to a remote node (if target node is itself, it is same to function ParallelRun) and run it.
func (s *Scope) RemoteRun(c *Compile) error {
	return s.ParallelRun(c)
}

// ParallelRun try to execute the scope in parallel way.
func (s *Scope) ParallelRun(c *Compile) error {
	var rds []engine.Reader

	if s.DataSource == nil {
		return s.MergeRun(c)
	}
	mcpu := s.NodeInfo.Mcpu
	snap := engine.Snapshot(s.Proc.Snapshot)
	{
		db, err := c.e.Database(c.ctx, s.DataSource.SchemaName, snap)
		if err != nil {
			return err
		}
		rel, err := db.Relation(c.ctx, s.DataSource.RelationName)
		if err != nil {
			return err
		}
		rds, _ = rel.NewReader(c.ctx, mcpu, nil, s.NodeInfo.Data)
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
	{
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
	}
	ctx, cancel := context.WithCancel(c.ctx)
	s.Magic = Merge
	s.PreScopes = ss
	s.Proc.Cancel = cancel
	s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
	{
		for i := 0; i < len(ss); i++ {
			s.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 1),
			}
		}
	}
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Reg: s.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return s.MergeRun(c)
}

func (s *Scope) appendInstruction(in vm.Instruction) {
	if !s.IsEnd {
		s.Instructions = append(s.Instructions, in)
	}
}
