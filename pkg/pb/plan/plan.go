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

const (
	SystemExternalRel = "e"
)

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

func (p *PartitionByDef) MarshalPartitionInfo() ([]byte, error) {
	data := make([]byte, p.ProtoSize())
	_, err := p.MarshalTo(data)
	return data, err
}

func (p *PartitionByDef) UnMarshalPartitionInfo(data []byte) error {
	return p.Unmarshal(data)
}

func (m *OnUpdate) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *OnUpdate) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

func (m *Default) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *Default) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

func (m CreateTable) IsSystemExternalRel() bool {
	return m.TableDef.TableType == SystemExternalRel
}
