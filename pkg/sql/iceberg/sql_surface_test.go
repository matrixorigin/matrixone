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

package iceberg

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestParseTableMappingSpec(t *testing.T) {
	spec, err := ParseTableMappingSpec(context.Background(), tree.NewIcebergTableParam(tree.IcebergOptions{
		tree.NewIcebergOption("catalog", "ksa_gold"),
		tree.NewIcebergOption("namespace", "sales"),
		tree.NewIcebergOption("table", "orders"),
		tree.NewIcebergOption("ref", "main"),
		tree.NewIcebergOption("read_mode", model.ReadModeAppendOnly),
		tree.NewIcebergOption("write_mode", model.WriteModeReadOnly),
	}))
	if err != nil {
		t.Fatalf("parse spec: %v", err)
	}
	if spec.CatalogName != "ksa_gold" || spec.Mapping.Namespace != "sales" || spec.Mapping.TableName != "orders" || spec.Mapping.DefaultRef != "main" {
		t.Fatalf("unexpected spec: %+v", spec)
	}
	if spec.Mapping.ReadMode != model.ReadModeAppendOnly || spec.Mapping.WriteMode != model.WriteModeReadOnly {
		t.Fatalf("unexpected modes: %+v", spec.Mapping)
	}
}

func TestParseTableMappingSpecDefaults(t *testing.T) {
	spec, err := ParseTableMappingSpec(context.Background(), tree.NewIcebergTableParam(tree.IcebergOptions{
		tree.NewIcebergOption("catalog", "ksa_gold"),
		tree.NewIcebergOption("namespace", "sales"),
		tree.NewIcebergOption("table", "orders"),
	}))
	if err != nil {
		t.Fatalf("parse spec: %v", err)
	}
	if spec.Mapping.DefaultRef != model.DefaultRefMain || spec.Mapping.ReadMode != model.ReadModeAppendOnly || spec.Mapping.WriteMode != model.WriteModeReadOnly {
		t.Fatalf("defaults were not applied: %+v", spec.Mapping)
	}
}

func TestParseTableMappingSpecAcceptsWritableModes(t *testing.T) {
	for _, writeMode := range []string{model.WriteModeAppendOnly, model.WriteModeMergeOnRead} {
		t.Run(writeMode, func(t *testing.T) {
			spec, err := ParseTableMappingSpec(context.Background(), tree.NewIcebergTableParam(tree.IcebergOptions{
				tree.NewIcebergOption("catalog", "ksa_gold"),
				tree.NewIcebergOption("namespace", "sales"),
				tree.NewIcebergOption("table", "orders"),
				tree.NewIcebergOption("read_mode", model.ReadModeMergeOnRead),
				tree.NewIcebergOption("write_mode", writeMode),
			}))
			if err != nil {
				t.Fatalf("parse writable spec: %v", err)
			}
			if spec.Mapping.WriteMode != writeMode {
				t.Fatalf("unexpected write mode: %+v", spec.Mapping)
			}
		})
	}
}

func TestParseTableMappingSpecRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name string
		opts tree.IcebergOptions
	}{
		{
			name: "missing required",
			opts: tree.IcebergOptions{tree.NewIcebergOption("catalog", "ksa_gold")},
		},
		{
			name: "duplicate",
			opts: tree.IcebergOptions{
				tree.NewIcebergOption("catalog", "ksa_gold"),
				tree.NewIcebergOption("catalog", "ksa_silver"),
			},
		},
		{
			name: "bad read mode",
			opts: tree.IcebergOptions{
				tree.NewIcebergOption("catalog", "ksa_gold"),
				tree.NewIcebergOption("namespace", "sales"),
				tree.NewIcebergOption("table", "orders"),
				tree.NewIcebergOption("read_mode", "passthrough"),
			},
		},
		{
			name: "bad write mode",
			opts: tree.IcebergOptions{
				tree.NewIcebergOption("catalog", "ksa_gold"),
				tree.NewIcebergOption("namespace", "sales"),
				tree.NewIcebergOption("table", "orders"),
				tree.NewIcebergOption("write_mode", "passthrough"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseTableMappingSpec(context.Background(), tree.NewIcebergTableParam(tt.opts)); err == nil {
				t.Fatalf("expected error")
			}
		})
	}
}
