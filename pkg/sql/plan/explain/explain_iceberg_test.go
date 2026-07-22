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

package explain

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestGetExtraInfoIcebergScan(t *testing.T) {
	node := &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		ExternScan: &plan.ExternScan{
			Type: int32(plan.ExternType_ICEBERG_TB),
			IcebergScan: &plan.IcebergScan{
				MappingId:         101,
				CatalogId:         7,
				Namespace:         "sales.prod",
				Table:             "orders",
				Ref:               "main",
				SnapshotId:        42,
				TimestampAsOf:     1767225600000,
				ReadMode:          "append_only",
				ProjectedFieldIds: []int32{1, 4},
				FilterDigest:      "filter-hash",
			},
		},
	}
	lines, err := NewNodeDescriptionImpl(node).GetExtraInfo(context.Background(), &ExplainOptions{Format: EXPLAIN_FORMAT_TEXT})
	if err != nil {
		t.Fatalf("get iceberg extra info: %v", err)
	}
	if len(lines) != 1 {
		t.Fatalf("expected one Iceberg info line, got %+v", lines)
	}
	got := lines[0]
	for _, want := range []string{
		"Iceberg:",
		"catalog_id=7",
		"mapping_id=101",
		"namespace=sales.prod",
		"table=orders",
		"ref=main",
		"snapshot_id=42",
		"timestamp_as_of_ms=1767225600000",
		"read_mode=append_only",
		"projected_field_ids=[1 4]",
		"filter_digest=filter-hash",
		"residual_filter=true",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected %q in Iceberg EXPLAIN line: %s", want, got)
		}
	}
}
