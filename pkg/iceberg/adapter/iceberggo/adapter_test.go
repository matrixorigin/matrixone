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

package iceberggo

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	"github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

func TestNewManifestCommitAdapterIsExplicitlyUnsupported(t *testing.T) {
	adapter := NewManifestCommitAdapter()
	if adapter.AdapterName() != AdapterName {
		t.Fatalf("adapter name = %s, want %s", adapter.AdapterName(), AdapterName)
	}
	_, err := adapter.BuildAppendManifests(context.Background(), write.AppendManifestRequest{
		Append: api.AppendRequest{
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			IdempotencyKey: "idem-1",
			DataFiles: []api.DataFile{{
				FilePath:        "s3://warehouse/sales/orders/data/part-1.parquet",
				FileFormat:      "parquet",
				RecordCount:     1,
				FileSizeInBytes: 1,
			}},
		},
		ManifestPath:     "s3://warehouse/sales/orders/metadata/m-1.avro",
		ManifestListPath: "s3://warehouse/sales/orders/metadata/snap-1.avro",
		SnapshotID:       1,
		SequenceNumber:   1,
	})
	if err == nil || !strings.Contains(err.Error(), string(api.ErrUnsupportedFeature)) {
		t.Fatalf("expected unsupported feature error, got %v", err)
	}
}

func TestRowDeltaCapabilityProbeKeepsNativeFallback(t *testing.T) {
	capability := ProbeRowDeltaCapability()
	if capability.AdapterName != AdapterName || capability.Supported || !capability.RequiresBuildTag {
		t.Fatalf("unexpected RowDelta capability: %+v", capability)
	}
	if !strings.Contains(capability.Reason, "not enabled") {
		t.Fatalf("capability reason should explain fallback: %+v", capability)
	}

	adapter := NewRowDeltaAdapter()
	if adapter.Name() != AdapterName {
		t.Fatalf("adapter name = %s, want %s", adapter.Name(), AdapterName)
	}
	if adapter.SupportsRowDelta() {
		t.Fatalf("iceberg-go RowDelta must remain disabled until production facade support exists")
	}
	stream, err := (dml.Planner{Adapter: adapter}).PlanDelete(context.Background(), dml.DeleteRequest{
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			TargetRef:      "main",
			BaseSnapshotID: 7,
			IdempotencyKey: "idem-1",
		},
		Targets: []dml.DeleteTarget{{
			DataFile:           api.DataFile{FilePath: "s3://warehouse/orders/data.parquet", FileFormat: "parquet", RecordCount: 1, FileSizeInBytes: 1},
			EqualityFieldIDs:   []int{1},
			PredicateStable:    true,
			EqualityDeleteFile: api.DataFile{FilePath: "s3://warehouse/orders/delete.parquet", FileFormat: "parquet", RecordCount: 1, FileSizeInBytes: 1},
		}},
	})
	if err != nil {
		t.Fatalf("native fallback should plan delete when iceberg-go RowDelta is disabled: %v", err)
	}
	if stream.Profile.UsedAdapter {
		t.Fatalf("disabled RowDelta adapter should not be marked used: %+v", stream.Profile)
	}
	if len(stream.Actions) != 1 || stream.Actions[0].Kind != dml.ActionAddEqualityDelete {
		t.Fatalf("unexpected fallback actions: %+v", stream.Actions)
	}
}
