// Copyright 2022 Matrix Origin
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

package plan

// when autocommit is set to false, and no active txn is started
// an implicit txn need to be started for statements , like insert/delete/update
// and most select statement, like select * from t1.
// but for statement like select 1 or SELECT @@session.autocommit , implicit txn is not needed
// walk through the plan for select statement and check if there is an node_table_scan
func (p *Plan) NeedImplicitTxn() bool {
	if p.GetQuery().GetStmtType() != Query_SELECT {
		return true
	}
	for _, n := range p.GetQuery().GetNodes() {
		if n.GetNodeType() == Node_TABLE_SCAN {
			return true
		}
	}
	return false
}

func (p *Plan) MarshalBinary() ([]byte, error) {
	data := make([]byte, p.ProtoSize())
	_, err := p.MarshalTo(data)
	return data, err
}

func (p *Plan) UnmarshalBinary(data []byte) error {
	return p.Unmarshal(data)
}

func (p *PartitionInfo) MarshalPartitionInfo() ([]byte, error) {
	data := make([]byte, p.ProtoSize())
	_, err := p.MarshalTo(data)
	return data, err
}

func (p *PartitionInfo) UnMarshalPartitionInfo(data []byte) error {
	return p.Unmarshal(data)
}

func (u *UniqueIndexDef) MarshalUniqueIndexDef() ([]byte, error) {
	data := make([]byte, u.ProtoSize())
	_, err := u.MarshalTo(data)
	return data, err
}

func (u *UniqueIndexDef) UnMarshalUniqueIndexDef(data []byte) error {
	return u.Unmarshal(data)
}

func (s *SecondaryIndexDef) MarshalSecondaryIndexDef() ([]byte, error) {
	data := make([]byte, s.ProtoSize())
	_, err := s.MarshalTo(data)
	return data, err
}

func (s *SecondaryIndexDef) UnMarshalSecondaryIndexDef(data []byte) error {
	return s.Unmarshal(data)
}
