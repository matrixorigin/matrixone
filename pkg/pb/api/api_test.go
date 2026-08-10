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

package api

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// alterTableRenameColV13 models the receiver before the checks field was added.
// Protobuf compatibility permits that receiver to decode the request while its
// generated type cannot expose the new field to the alter handler, which is why
// the sender needs a rollout gate.
type alterTableRenameColV13 struct {
	OldName              string   `protobuf:"bytes,1,opt,name=old_name,json=oldName,proto3" json:"old_name,omitempty"`
	NewName              string   `protobuf:"bytes,2,opt,name=new_name,json=newName,proto3" json:"new_name,omitempty"`
	SequenceNum          uint32   `protobuf:"varint,3,opt,name=sequence_num,json=sequenceNum,proto3" json:"sequence_num,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *alterTableRenameColV13) Reset()         { *m = alterTableRenameColV13{} }
func (m *alterTableRenameColV13) String() string { return proto.CompactTextString(m) }
func (*alterTableRenameColV13) ProtoMessage()    {}

func TestNewRenameColumnReqWithChecks(t *testing.T) {
	checks := []*plan.CheckDef{{OriginSql: "CHECK (`new_col` > 0)"}}
	req := NewRenameColumnReqWithChecks(1, 2, "old_col", "new_col", 3, checks)

	rename := req.GetRenameCol()
	require.Equal(t, "CHECK (`new_col` > 0)", rename.GetChecks()[0].GetOriginSql())
	require.NotSame(t, checks[0], rename.GetChecks()[0])

	checks[0].OriginSql = "mutated"
	require.Equal(t, "CHECK (`new_col` > 0)", rename.GetChecks()[0].GetOriginSql())

	data, err := req.Marshal()
	require.NoError(t, err)
	var decoded AlterTableReq
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, "CHECK (`new_col` > 0)", decoded.GetRenameCol().GetChecks()[0].GetOriginSql())
}

func TestV13RenameColumnDecoderAcceptsUnknownChecks(t *testing.T) {
	rename := &AlterTableRenameCol{
		OldName:     "old_col",
		NewName:     "new_col",
		SequenceNum: 3,
		Checks: []*plan.CheckDef{{
			Name:      "ck_old_col",
			OriginSql: "`new_col` > 0",
		}},
	}

	data, err := rename.Marshal()
	require.NoError(t, err)

	var legacy alterTableRenameColV13
	require.NoError(t, proto.Unmarshal(data, &legacy))
	require.Equal(t, "old_col", legacy.OldName)
	require.Equal(t, "new_col", legacy.NewName)
	require.Equal(t, uint32(3), legacy.SequenceNum)
	require.NotEmpty(t, legacy.XXX_unrecognized,
		"v13 accepts the request but exposes no CHECK field for the TN alter handler to apply")
}
