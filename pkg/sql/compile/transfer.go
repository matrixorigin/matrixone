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

import "matrixone/pkg/sql/protocol"

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