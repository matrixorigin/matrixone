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

package siriusbridge

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ColumnEvidence contains declared descriptors. Physical decimal width belongs
// to OID; precision alone cannot establish it. Headings participate only in SHA.
type ColumnEvidence struct {
	OID       int32 `json:"oid"`
	Precision int32 `json:"precision"`
	Scale     int32 `json:"scale"`
	Nullable  bool  `json:"nullable"`
}

// SchemaEvidence shares the existing embedded digest with the independently
// executed native-MO result owner. It performs no native work or allocation.
func SchemaEvidence(columns []Column) ([]ColumnEvidence, string, error) {
	if len(columns) == 0 || len(columns) > maxColumns {
		return nil, "", moerr.NewInvalidInputNoCtx("campaign schema column bound exceeded")
	}
	nameBytes := 0
	for _, column := range columns {
		if len(column.Name) > maxMetadataTextBytes-nameBytes || column.OID > 1<<31-1 {
			return nil, "", moerr.NewInvalidInputNoCtx("campaign schema text or OID bound exceeded")
		}
		nameBytes += len(column.Name)
	}
	result := make([]ColumnEvidence, len(columns))
	digest := sha256.New()
	_, _ = digest.Write([]byte("sirius-mo-output-schema-v1\x00"))
	var descriptor [17]byte
	for index, column := range columns {
		result[index] = ColumnEvidence{OID: int32(column.OID), Precision: column.Width, Scale: column.Scale, Nullable: column.Nullable}
		binary.LittleEndian.PutUint32(descriptor[:4], column.OID)
		binary.LittleEndian.PutUint32(descriptor[4:8], uint32(column.Width))
		binary.LittleEndian.PutUint32(descriptor[8:12], uint32(column.Scale))
		descriptor[12] = 0
		if column.Nullable {
			descriptor[12] = 1
		}
		binary.LittleEndian.PutUint32(descriptor[13:], uint32(len(column.Name)))
		_, _ = digest.Write(descriptor[:])
		_, _ = digest.Write([]byte(column.Name))
	}
	return result, hex.EncodeToString(digest.Sum(nil)), nil
}
