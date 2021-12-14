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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/plus"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/times"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/untransform"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

func Transfer(s *Scope) protocol.Scope {
	var ps protocol.Scope

	ps.Ins = s.Instructions
	ps.Magic = s.Magic
	if s.DataSource != nil {
		ps.DataSource.IsMerge = s.DataSource.IsMerge
		ps.DataSource.SchemaName = s.DataSource.SchemaName
		ps.DataSource.RelationName = s.DataSource.RelationName
		ps.DataSource.RefCounts = s.DataSource.RefCounts
		ps.DataSource.Attributes = s.DataSource.Attributes
	}
	ps.NodeInfo.Id = s.NodeInfo.Id
	ps.NodeInfo.Addr = s.NodeInfo.Addr
	ps.PreScopes = make([]protocol.Scope, len(s.PreScopes))
	for i := range s.PreScopes {
		ps.PreScopes[i] = Transfer(s.PreScopes[i])
	}
	return ps
}

func Untransfer(s *Scope, ps protocol.Scope)  {
	UntransferIns(s.Instructions, ps.Ins)
	s.Magic = ps.Magic
	if s.DataSource != nil {
		s.DataSource.IsMerge = ps.DataSource.IsMerge
		s.DataSource.SchemaName = ps.DataSource.SchemaName
		s.DataSource.RelationName = ps.DataSource.RelationName
		s.DataSource.RefCounts = ps.DataSource.RefCounts
		s.DataSource.Attributes = ps.DataSource.Attributes
	}
	s.NodeInfo.Id = ps.NodeInfo.Id
	s.NodeInfo.Addr = ps.NodeInfo.Addr

	for i := 0; i < len(s.PreScopes); i++ {
		 Untransfer(s.PreScopes[i], ps.PreScopes[i])
	}
}

func UntransferIns(ins, pins vm.Instructions) {
	for i, in := range ins {
		switch in.Op {
		case vm.Top:
			a := ins[i].Arg.(*top.Argument)
			pa := in.Arg.(*top.Argument)
			a.Fs = pa.Fs
			a.Limit = pa.Limit
		case vm.Plus:
			a := ins[i].Arg.(*plus.Argument)
			pa := in.Arg.(*plus.Argument)
			a.Typ = pa.Typ
		case vm.Limit:
			a := ins[i].Arg.(*limit.Argument)
			pa := in.Arg.(*limit.Argument)
			a.Seen = pa.Seen
			a.Limit = pa.Limit
		case vm.Times:
			a := ins[i].Arg.(*times.Argument)
			pa := in.Arg.(*times.Argument)
			a.IsBare = pa.IsBare
			a.R = pa.R
			a.Rvars = pa.Rvars
			a.Ss = pa.Ss
			a.Svars = pa.Svars
			a.VarsMap = pa.VarsMap

			if pa.Arg != nil {
				a.Arg.Typ = pa.Arg.Typ
				a.Arg.IsMerge = pa.Arg.IsMerge
				a.Arg.FreeVars = pa.Arg.FreeVars
				a.Arg.Restrict = pa.Arg.Restrict
				a.Arg.Projection = pa.Arg.Projection
				a.Arg.BoundVars = pa.Arg.BoundVars
			}
		case vm.Merge:
		case vm.Dedup:
		case vm.Order:
			a := ins[i].Arg.(*order.Argument)
			pa := in.Arg.(*order.Argument)
			a.Fs = pa.Fs
		case vm.Output:
			a := ins[i].Arg.(*output.Argument)
			pa := in.Arg.(*output.Argument)
			a.Attrs = pa.Attrs
		case vm.Offset:
			a := ins[i].Arg.(*offset.Argument)
			pa := in.Arg.(*offset.Argument)
			a.Seen = pa.Seen
			a.Offset = pa.Offset
		case vm.Restrict:
			a := ins[i].Arg.(*restrict.Argument)
			pa := in.Arg.(*restrict.Argument)
			a.Attrs = pa.Attrs
			a.E = pa.E
		case vm.Connector:
		case vm.Transform:
			a := ins[i].Arg.(*transform.Argument)
			pa := in.Arg.(*transform.Argument)
			a.Typ = pa.Typ
			a.IsMerge = pa.IsMerge
			a.FreeVars = pa.FreeVars
			a.Restrict = pa.Restrict
			a.Projection = pa.Projection
			a.BoundVars = pa.BoundVars
		case vm.Projection:
			a := ins[i].Arg.(*projection.Argument)
			pa := in.Arg.(*projection.Argument)
			a.Es = pa.Es
			a.As = pa.As
			a.Rs = pa.Rs
		case vm.UnTransform:
			a := ins[i].Arg.(*untransform.Argument)
			pa := in.Arg.(*untransform.Argument)
			a.Type = pa.Type
			a.FreeVars = pa.FreeVars
		}
	}
}