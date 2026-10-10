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

package cnservice

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/matrixorigin/matrixone/pkg/sql/compile"
)

type embeddedColumnEvidence struct {
	OID       int32 `json:"oid"`
	Precision int32 `json:"precision"`
	Scale     int32 `json:"scale"`
	Nullable  bool  `json:"nullable"`
}

// Preparation verified these descriptors against the native output schema.
// The bridge bounds them to 1024 columns before an execution can exist. Names
// participate only in the digest: log neither expression headings nor values.
func embeddedSchemaEvidence(request compile.SiriusPrepareRequest) ([]embeddedColumnEvidence, string) {
	columns := make([]embeddedColumnEvidence, len(request.OutputTypes))
	digest := sha256.New()
	_, _ = digest.Write([]byte("sirius-mo-output-schema-v1\x00"))
	var descriptor [17]byte
	for i, typ := range request.OutputTypes {
		columns[i] = embeddedColumnEvidence{OID: typ.Id, Precision: typ.Width, Scale: typ.Scale, Nullable: !typ.NotNullable}
		binary.LittleEndian.PutUint32(descriptor[:4], uint32(typ.Id))
		binary.LittleEndian.PutUint32(descriptor[4:8], uint32(typ.Width))
		binary.LittleEndian.PutUint32(descriptor[8:12], uint32(typ.Scale))
		descriptor[12] = 0
		if !typ.NotNullable {
			descriptor[12] = 1
		}
		binary.LittleEndian.PutUint32(descriptor[13:], uint32(len(request.Headings[i])))
		_, _ = digest.Write(descriptor[:])
		_, _ = digest.Write([]byte(request.Headings[i]))
	}
	return columns, hex.EncodeToString(digest.Sum(nil))
}
