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
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
)

func Transfer(s *Scope) protocol.Scope {
	var ps protocol.Scope

	ps.Ins = s.Instructions
	ps.Magic = s.Magic
	if s.DataSource != nil {
		ps.Data.ID = s.DataSource.RelationName
		ps.Data.DB = s.DataSource.DBName
		ps.Data.Refer = s.DataSource.RefCount
		ps.Data.Segs = make([]protocol.Segment, len(s.DataSource.Segments))
		for i, seg := range s.DataSource.Segments {
			ps.Data.Segs[i].Id = seg.Id
			ps.Data.Segs[i].GroupId = seg.GroupId
			ps.Data.Segs[i].Version = seg.Version
			ps.Data.Segs[i].IsRemote = seg.IsRemote
			ps.Data.Segs[i].TabletId = seg.TabletId
		}
	}
	ps.Ss = make([]protocol.Scope, len(s.PreScopes))
	for i := range s.PreScopes {
		ps.Ss[i] = Transfer(s.PreScopes[i])
	}
	return ps
}
