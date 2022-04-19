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

package handler

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func recoverScope(ps protocol.Scope, proc *process.Process) *compile.Scope {
	s := new(compile.Scope)
	s.Instructions = ps.Ins
	s.Magic = ps.Magic
	s.NodeInfo.Id = ps.NodeInfo.Id
	s.NodeInfo.Addr = ps.NodeInfo.Addr
	s.NodeInfo.Data = ps.NodeInfo.Data
	s.Proc = process.New(mheap.New(guest.New(proc.Mp.Gm.Limit, proc.Mp.Gm.Mmu)))
	if len(ps.PreScopes) > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		s.Proc.Cancel = cancel
		s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ps.PreScopes))
		for i := 0; i < len(ps.PreScopes); i++ {
			s.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 1),
			}
		}
	}
	if len(ps.DataSource.RefCounts) > 0 {
		s.DataSource = new(compile.Source)
		s.DataSource.IsMerge = ps.DataSource.IsMerge
		s.DataSource.SchemaName = ps.DataSource.SchemaName
		s.DataSource.RelationName = ps.DataSource.RelationName
		s.DataSource.RefCounts = ps.DataSource.RefCounts
		s.DataSource.Attributes = ps.DataSource.Attributes
	}
	s.PreScopes = make([]*compile.Scope, len(ps.PreScopes))
	for i := range ps.PreScopes {
		ps.PreScopes[i].Ins = recoverInstructions(ps.PreScopes[i].Ins, s.Proc, s.Proc.Reg.MergeReceivers[i])
		s.PreScopes[i] = recoverScope(ps.PreScopes[i], s.Proc)
	}
	return s
}

func recoverInstructions(ins vm.Instructions, proc *process.Process, reg *process.WaitRegister) vm.Instructions {
	for i, in := range ins {
		if in.Op == vm.Connector {
			in.Arg = &connector.Argument{
				Reg: reg,
				Mmu: proc.Mp.Gm,
			}
		}
		ins[i] = in
	}
	return ins
}
