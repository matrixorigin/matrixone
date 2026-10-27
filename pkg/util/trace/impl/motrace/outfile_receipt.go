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

package motrace

import (
	"encoding/json"
	"strings"
	"unicode/utf8"

	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
)

const statementOutfileTargetLimit = 4096

// StatementOutfileReceipt is the bounded, detached audit receipt for one
// SELECT ... INTO OUTFILE execution. Incomplete totals are lower bounds.
type StatementOutfileReceipt struct {
	Version         int    `json:"version"`
	Target          string `json:"target"`
	Format          string `json:"format"`
	BytesWritten    uint64 `json:"bytes_written"`
	RowsWritten     uint64 `json:"rows_written"`
	Complete        bool   `json:"complete"`
	TargetTruncated bool   `json:"target_truncated,omitempty"`
	TargetRedacted  bool   `json:"target_redacted,omitempty"`
	TargetInvalid   bool   `json:"target_invalid,omitempty"`
}

// SetOutfileReceipt marks this statement as non-aggregatable and installs its
// bounded attempted-output identity before authorization/compile can fail.
func (s *StatementInfo) SetOutfileReceipt(receipt StatementOutfileReceipt) {
	if s == nil {
		return
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	if receipt.Version == 0 {
		receipt.Version = 1
	}
	if !utf8.ValidString(receipt.Target) {
		receipt.Target = ""
		receipt.TargetInvalid = true
	}
	if len(receipt.Target) > statementOutfileTargetLimit {
		cut := statementOutfileTargetLimit
		for cut > 0 && !utf8.ValidString(receipt.Target[:cut]) {
			cut--
		}
		receipt.Target = receipt.Target[:cut]
		receipt.TargetTruncated = true
	}
	receipt.Target = strings.Clone(receipt.Target)
	receipt.Format = normalizeOutfileFormat(receipt.Format)
	s.outfileReceipt = &receipt
	s.disableAgg = true
	s.jsonByte = nil
}

// UpdateOutfileReceipt records only sink-confirmed totals transferred after
// the existing exporter has stopped and joined its workers.
func (s *StatementInfo) UpdateOutfileReceipt(format string, bytesWritten, rowsWritten uint64, complete bool) {
	if s == nil {
		return
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.outfileReceipt == nil {
		return
	}
	s.outfileReceipt.Format = normalizeOutfileFormat(format)
	s.outfileReceipt.BytesWritten = bytesWritten
	s.outfileReceipt.RowsWritten = rowsWritten
	s.outfileReceipt.Complete = complete
	s.jsonByte = nil
}

// HasOutfileReceipt reports whether this statement owns an audit receipt.
func (s *StatementInfo) HasOutfileReceipt() bool {
	if s == nil {
		return false
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.outfileReceipt != nil
}

func normalizeOutfileFormat(format string) string {
	switch strings.ToLower(format) {
	case "csv", "jsonline", "parquet":
		return strings.ToLower(format)
	default:
		return "unknown"
	}
}

// execPlanWithOutfileReceipt composes the additive field without decoding plan
// numbers into float64. json.RawMessage preserves large integer precision.
func execPlanWithOutfileReceipt(plan []byte, receipt StatementOutfileReceipt) []byte {
	originalPlanBytes := len(plan)
	fields := make(map[string]json.RawMessage)
	if len(plan) > db_holder.StatementInfoTextLimit {
		plan = db_holder.StatementInfoPlanSummary(originalPlanBytes)
		_ = json.Unmarshal(plan, &fields)
	} else if err := json.Unmarshal(plan, &fields); err != nil || fields == nil {
		plan = db_holder.StatementInfoPlanSummary(originalPlanBytes)
		fields = make(map[string]json.RawMessage)
		_ = json.Unmarshal(plan, &fields)
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		return noExecPlan
	}
	fields["outfile"] = receiptJSON
	encoded, err := json.Marshal(fields)
	if err != nil {
		return receiptOnlyJSON(receiptJSON)
	}
	if len(encoded) <= db_holder.StatementInfoTextLimit {
		return encoded
	}

	fields = make(map[string]json.RawMessage)
	_ = json.Unmarshal(db_holder.StatementInfoPlanSummary(originalPlanBytes), &fields)
	fields["outfile"] = receiptJSON
	encoded, err = json.Marshal(fields)
	if err != nil || len(encoded) > db_holder.StatementInfoTextLimit {
		return receiptOnlyJSON(receiptJSON)
	}
	return encoded
}

func receiptOnlyJSON(receiptJSON json.RawMessage) []byte {
	encoded, err := json.Marshal(map[string]json.RawMessage{"outfile": receiptJSON})
	if err != nil {
		return noExecPlan
	}
	return encoded
}
