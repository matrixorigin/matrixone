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

	// CheckConstraintWarningFilterAuxID marks a FILTER predicate that implements
	// CHECK enforcement for an IGNORE statement. Older CNs safely ignore AuxId;
	// newer CNs use it only to count rows dropped as warnings.
	CheckConstraintWarningFilterAuxID int32 = -24950
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

func (m *GeneratedCol) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *GeneratedCol) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

func (m CreateTable) IsSystemExternalRel() bool {
	return m.TableDef.TableType == SystemExternalRel
}

func (t *Type) IsEmpty() bool {
	return t == nil || (t.Id == 0 &&
		!t.NotNullable &&
		!t.AutoIncr &&
		t.Width == 0 &&
		t.Scale == 0 &&
		t.Table == "" &&
		t.Enumvalues == "")
}

func (def *ColDef) GetOriginCaseName() string {
	if def.OriginName == "" {
		return def.Name
	}
	return def.OriginName
}

func (m *Expr) ExprString() string {
	if m == nil {
		return ""
	}

	data, err := m.Marshal()
	if err != nil {
		return ""
	}

	return string(data)
}

func (m *ColRef) ColRefString() string {
	if m == nil {
		return ""
	}

	data, err := m.Marshal()
	if err != nil {
		return ""
	}

	return string(data)
}

// GetLiteralFloat64 extracts a comparable float64 from a plain numeric literal
// expression, returning ok=false for anything that is not a non-null numeric
// literal (nil, non-literal, null, or a non-numeric literal such as a vector).
// It is the single owner of the "distance bound / literal -> float64" decode
// shared by the planner (DistRange folding) and the reader (index range setup).
func GetLiteralFloat64(expr *Expr) (float64, bool) {
	if expr == nil {
		return 0, false
	}
	lit := expr.GetLit()
	if lit == nil || lit.Isnull {
		return 0, false
	}
	switch v := lit.Value.(type) {
	case *Literal_Fval:
		return float64(v.Fval), true
	case *Literal_Dval:
		return v.Dval, true
	case *Literal_I8Val:
		return float64(v.I8Val), true
	case *Literal_I16Val:
		return float64(v.I16Val), true
	case *Literal_I32Val:
		return float64(v.I32Val), true
	case *Literal_I64Val:
		return float64(v.I64Val), true
	case *Literal_U8Val:
		return float64(v.U8Val), true
	case *Literal_U16Val:
		return float64(v.U16Val), true
	case *Literal_U32Val:
		return float64(v.U32Val), true
	case *Literal_U64Val:
		return float64(v.U64Val), true
	default:
		return 0, false
	}
}
