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
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/siriusbridge"
)

type embeddedColumnEvidence = siriusbridge.ColumnEvidence

// Preparation verified these descriptors against the native output schema.
// The bridge bounds them to 1024 columns before an execution can exist. Names
// participate only in the digest: log neither expression headings nor values.
func embeddedSchemaEvidence(request compile.SiriusPrepareRequest) ([]embeddedColumnEvidence, string) {
	columns := make([]siriusbridge.Column, len(request.OutputTypes))
	for i, typ := range request.OutputTypes {
		columns[i] = siriusbridge.Column{OID: uint32(typ.Id), Width: typ.Width, Scale: typ.Scale, Nullable: !typ.NotNullable, Name: request.Headings[i]}
	}
	// Preparation already validated these immutable descriptor bounds.
	evidence, digest, _ := siriusbridge.SchemaEvidence(columns)
	return evidence, digest
}
