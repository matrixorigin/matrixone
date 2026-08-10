// Copyright 2026 Matrix Origin
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

package engine

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestConstraintDefMarshalBinaryWireCompatibility(t *testing.T) {
	constraint := &ConstraintDef{Cts: []Constraint{
		&IndexDef{Indexes: []*plan.IndexDef{{
			IdxId:          "idx-id",
			IndexName:      "idx_name",
			Parts:          []string{"id", "name"},
			IndexTableName: "__mo_index_idx_name",
			Unique:         true,
			TableExist:     true,
			Visible:        true,
		}}},
		&RefChildTableDef{Tables: []uint64{7, 11}},
		&ForeignKeyDef{Fkeys: []*plan.ForeignKeyDef{{
			Name:        "fk_parent",
			Cols:        []uint64{2},
			ForeignTbl:  42,
			ForeignCols: []uint64{1},
			OnDelete:    plan.ForeignKeyDef_SET_NULL,
			OnUpdate:    plan.ForeignKeyDef_CASCADE,
		}}},
		&PrimaryKeyDef{Pkey: &plan.PrimaryKeyDef{
			PkeyColId:   1,
			PkeyColName: "id",
			Names:       []string{"id"},
		}},
		&StreamConfigsDef{Configs: []*plan.Property{{Key: "k", Value: "v"}}},
	}}

	want, err := legacyConstraintDefMarshalBinary(constraint)
	require.NoError(t, err)

	got, err := constraint.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, want, got)

	decoded := new(ConstraintDef)
	require.NoError(t, decoded.UnmarshalBinary(got))
	require.Len(t, decoded.Cts, len(constraint.Cts))
}

// legacyConstraintDefMarshalBinary is kept only as a byte-for-byte oracle for
// the allocation-reduced encoder above.
func legacyConstraintDefMarshalBinary(def *ConstraintDef) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	for _, ct := range def.Cts {
		switch def := ct.(type) {
		case *IndexDef:
			buf.WriteByte(byte(Index))
			if err := binary.Write(buf, binary.BigEndian, uint64(len(def.Indexes))); err != nil {
				return nil, err
			}
			for _, indexDef := range def.Indexes {
				data, err := indexDef.Marshal()
				if err != nil {
					return nil, err
				}
				if err := binary.Write(buf, binary.BigEndian, uint64(len(data))); err != nil {
					return nil, err
				}
				if _, err := buf.Write(data); err != nil {
					return nil, err
				}
			}
		case *RefChildTableDef:
			buf.WriteByte(byte(RefChildTable))
			if err := binary.Write(buf, binary.BigEndian, uint64(len(def.Tables))); err != nil {
				return nil, err
			}
			for _, tableID := range def.Tables {
				if err := binary.Write(buf, binary.BigEndian, tableID); err != nil {
					return nil, err
				}
			}
		case *ForeignKeyDef:
			buf.WriteByte(byte(ForeignKey))
			if err := binary.Write(buf, binary.BigEndian, uint64(len(def.Fkeys))); err != nil {
				return nil, err
			}
			for _, foreignKey := range def.Fkeys {
				data, err := foreignKey.Marshal()
				if err != nil {
					return nil, err
				}
				if err := binary.Write(buf, binary.BigEndian, uint64(len(data))); err != nil {
					return nil, err
				}
				if _, err := buf.Write(data); err != nil {
					return nil, err
				}
			}
		case *PrimaryKeyDef:
			buf.WriteByte(byte(PrimaryKey))
			data, err := def.Pkey.Marshal()
			if err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.BigEndian, uint64(len(data))); err != nil {
				return nil, err
			}
			if _, err := buf.Write(data); err != nil {
				return nil, err
			}
		case *StreamConfigsDef:
			buf.WriteByte(byte(StreamConfig))
			if err := binary.Write(buf, binary.BigEndian, uint64(len(def.Configs))); err != nil {
				return nil, err
			}
			for _, config := range def.Configs {
				data, err := config.Marshal()
				if err != nil {
					return nil, err
				}
				if err := binary.Write(buf, binary.BigEndian, uint64(len(data))); err != nil {
					return nil, err
				}
				if _, err := buf.Write(data); err != nil {
					return nil, err
				}
			}
		}
	}
	return buf.Bytes(), nil
}
