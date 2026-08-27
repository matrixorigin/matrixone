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

package lifecycle

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const bindingSchemaDigestVersion uint16 = 1

// BindingSchemaDigest is the DDL fence fingerprint. It intentionally differs
// from the Archive SchemaDescriptor digest: the former includes physical
// catalog identity, while the latter is self-contained Restore metadata.
func BindingSchemaDigest(table *plan.TableDef) [sha256.Size]byte {
	if table == nil {
		return [sha256.Size]byte{}
	}
	var value bytes.Buffer
	writeBindingUint16(&value, bindingSchemaDigestVersion)
	writeBindingUint64(&value, table.TblId)
	writeBindingUint64(&value, table.LogicalId)
	writeBindingUint32(&value, table.Version)
	writeBindingString(&value, table.DbName)
	writeBindingString(&value, table.Name)
	writeBindingUint32(&value, uint32(len(table.Cols)))
	for _, column := range table.Cols {
		if column == nil {
			value.WriteByte(0)
			continue
		}
		value.WriteByte(1)
		writeBindingUint64(&value, column.ColId)
		writeBindingString(&value, column.Name)
		writeBindingUint32(&value, column.Seqnum)
		writeBindingUint32(&value, uint32(column.Typ.Id))
		writeBindingUint32(&value, uint32(column.Typ.Width))
		writeBindingUint32(&value, uint32(column.Typ.Scale))
		writeBindingString(&value, column.Typ.Enumvalues)
		writeBindingBool(&value, column.NotNull)
		writeBindingBool(&value, column.Typ.NotNullable)
		writeBindingBool(&value, column.Typ.AutoIncr)
		writeBindingBool(&value, column.Hidden)
		if column.Default == nil {
			value.WriteByte(0)
		} else {
			value.WriteByte(1)
			writeBindingString(&value, column.Default.OriginString)
		}
	}
	return sha256.Sum256(value.Bytes())
}

func writeBindingString(value *bytes.Buffer, field string) {
	writeBindingUint32(value, uint32(len(field)))
	value.WriteString(field)
}

func writeBindingBool(value *bytes.Buffer, field bool) {
	if field {
		value.WriteByte(1)
	} else {
		value.WriteByte(0)
	}
}

func writeBindingUint16(value *bytes.Buffer, field uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], field)
	value.Write(encoded[:])
}

func writeBindingUint32(value *bytes.Buffer, field uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], field)
	value.Write(encoded[:])
}

func writeBindingUint64(value *bytes.Buffer, field uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], field)
	value.Write(encoded[:])
}
