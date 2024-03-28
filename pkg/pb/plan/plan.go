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

import "bytes"

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

func (p *PartitionByDef) GenPartitionExprString() string {
	switch p.Type {
	case PartitionType_HASH, PartitionType_LINEAR_HASH,
		PartitionType_RANGE, PartitionType_LIST:
		return p.PartitionExpr.ExprStr
	case PartitionType_KEY, PartitionType_LINEAR_KEY,
		PartitionType_LIST_COLUMNS, PartitionType_RANGE_COLUMNS:
		partitionColumns := p.PartitionColumns
		buf := bytes.NewBuffer(make([]byte, 0, 128))

		for i, column := range partitionColumns.PartitionColumns {
			if i == 0 {
				buf.WriteString(column)
			} else {
				buf.WriteString(",")
				buf.WriteString(column)
			}
		}
		return buf.String()
	default:
		return ""
	}
}

func (tableDef *TableDef) GetColsByIndex(colIndex []int32) []*ColDef {
	// colIndex == nil means all columns, colIndex == [] means no columns
	if colIndex == nil {
		return tableDef.Cols
	}
	cols := make([]*ColDef, len(colIndex))
	for i, idx := range colIndex {
		cols[i] = tableDef.Cols[idx]
	}
	return cols
}

func (tableDef *TableDef) GetPartitionExprByIndex(colIndex []int32) *Expr {
	if colIndex == nil {
		return tableDef.Partition.PartitionExpression
	}
	return nil
}

func (tableDef *TableDef) GetColLength(colIndex []int32) int {
	if colIndex == nil {
		return len(tableDef.Cols)
	}
	return len(colIndex)
}

func (tableDef *TableDef) GetName2ColIndexByColIdx(colIndex []int32) map[string]int32 {
	name2ColIndex := make(map[string]int32)
	for i, col := range tableDef.GetColsByIndex(colIndex) {
		name2ColIndex[col.Name] = int32(i)
	}
	return name2ColIndex
}
