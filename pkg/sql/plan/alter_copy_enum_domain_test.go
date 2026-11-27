// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/assert"
)

func TestAlterCopyEnumDomainPreservation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		before, after pb.Type
		want          bool
	}{
		{"identical bytes, different domain", pb.Type{Id: int32(types.T_enum), Enumvalues: "2#1:a1:b", EnumEncoding: types.EnumEncodingLegacy}, pb.Type{Id: int32(types.T_enum), Enumvalues: "2#1:a1:b", EnumEncoding: types.EnumEncodingFramed}, false},
		{"same domain, different encoding", pb.Type{Id: int32(types.T_enum), Enumvalues: "a,b", EnumEncoding: types.EnumEncodingLegacy}, pb.Type{Id: int32(types.T_enum), Enumvalues: "2#1:a1:b", EnumEncoding: types.EnumEncodingFramed}, true},
		{"malformed framed domain", pb.Type{Id: int32(types.T_enum), Enumvalues: "broken", EnumEncoding: types.EnumEncodingFramed}, pb.Type{Id: int32(types.T_enum), Enumvalues: "broken", EnumEncoding: types.EnumEncodingFramed}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := &TableDef{Name: "src", Cols: []*ColDef{
				{Name: "a", ColId: 1, Seqnum: 1, Typ: tc.before},
				{Name: "g", ColId: 2, Seqnum: 2, Typ: pb.Type{Id: int32(types.T_int32)}, GeneratedCol: &pb.GeneratedCol{IsStored: true, OriginString: "a", Expr: &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 0}}}}},
			}}
			target := DeepCopyTableDef(old, true)
			target.Cols[0].Typ = tc.after
			alterCtx := initAlterTableContext(old, target, "db")
			assert.Equal(t, tc.want, AlterCopyKeyColumnValueUnchanged(old.Cols[0], target.Cols[0], true))
			assert.Equal(t, tc.want, alterCopyPreservedGeneratedColumns(old, target, alterCtx)["g"])
		})
	}
}
