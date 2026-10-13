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

package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/siriusbridge"
	"go.uber.org/zap"
)

// Called only by the once-per-statement executed metadata publication owner,
// after a definition retry has installed and frozen the producing generation.
func recordCampaignResultMetadata(ses *Session, columns []*plan.ColDef) {
	if !ses.campaignEvidenceEnabled.Load() {
		return
	}
	if len(columns) == 0 || len(columns) > 1024 {
		return
	}
	nameBytes := 0
	for _, column := range columns {
		if column == nil || len(column.Name) > (1<<20)-nameBytes {
			return
		}
		nameBytes += len(column.Name)
	}
	descriptors := make([]siriusbridge.Column, len(columns))
	for index, column := range columns {
		descriptors[index] = siriusbridge.Column{OID: uint32(column.Typ.Id), Width: column.Typ.Width, Scale: column.Typ.Scale, Nullable: !column.Typ.NotNullable, Name: column.Name}
	}
	evidence, digest, err := siriusbridge.SchemaEvidence(descriptors)
	if err != nil {
		return
	}
	logutil.Info("MO campaign result schema",
		zap.String("query_id", ses.GetStmtId().String()), zap.Uint32("account_id", ses.GetAccountId()),
		zap.Any("output_schema", evidence), zap.String("output_schema_sha256", digest))
}
