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

package metadata

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const sampleMetadataJSON = `{
  "format-version": 2,
  "table-uuid": "1b4f2a11-3ef8-4e66-93cc-3d43d44f7b11",
  "location": "s3://warehouse/sales/orders",
  "last-sequence-number": 7,
  "last-updated-ms": 1710000200000,
  "current-schema-id": 1,
  "schemas": [
    {
      "schema-id": 1,
      "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "name", "required": false, "type": "string"},
        {"id": 3, "name": "price", "required": false, "type": "decimal(12, 2)"},
        {"id": 4, "name": "created_at", "required": false, "type": "timestamptz"}
      ],
      "identifier-field-ids": [1]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [
    {
      "spec-id": 0,
      "fields": [
        {"source-id": 4, "field-id": 1000, "name": "created_day", "transform": "day"}
      ]
    }
  ],
  "current-snapshot-id": 22,
  "snapshots": [
    {"snapshot-id": 11, "sequence-number": 6, "timestamp-ms": 1710000000000, "manifest-list": "s3://warehouse/sales/orders/metadata/snap-11.avro", "schema-id": 1},
    {"snapshot-id": 22, "parent-snapshot-id": 11, "sequence-number": 7, "timestamp-ms": 1710000200000, "manifest-list": "s3://warehouse/sales/orders/metadata/snap-22.avro", "schema-id": 1}
  ],
  "snapshot-log": [
    {"timestamp-ms": 1710000000000, "snapshot-id": 11},
    {"timestamp-ms": 1710000200000, "snapshot-id": 22}
  ],
  "refs": {
    "main": {"snapshot-id": 22, "type": "branch"},
    "audit": {"snapshot-id": 11, "type": "tag"}
  },
  "properties": {
    "owner": "matrixone"
  }
}`

const emptySnapshotMetadataJSON = `{
  "format-version": 2,
  "table-uuid": "1b4f2a11-3ef8-4e66-93cc-3d43d44f7b11",
  "location": "s3://warehouse/sales/empty_orders",
  "last-sequence-number": 0,
  "last-updated-ms": 1710000200000,
  "current-schema-id": 0,
  "schemas": [
    {
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"}
      ],
      "identifier-field-ids": []
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [
    {"spec-id": 0, "fields": []}
  ],
  "current-snapshot-id": -1,
  "snapshots": [],
  "snapshot-log": [],
  "refs": {},
  "properties": {}
}`

func TestParseTableMetadataAndResolveSnapshot(t *testing.T) {
	meta, err := ParseTableMetadata([]byte(sampleMetadataJSON), "s3://warehouse/sales/orders/metadata/v2.metadata.json")
	if err != nil {
		t.Fatalf("parse metadata: %v", err)
	}
	if meta.FormatVersion != 2 || meta.Location != "s3://warehouse/sales/orders" {
		t.Fatalf("unexpected metadata: %+v", meta)
	}
	if meta.MetadataLocationHash == "" || strings.Contains(meta.MetadataLocationRed, "warehouse") {
		t.Fatalf("metadata location should be hashed/redacted: hash=%q red=%q", meta.MetadataLocationHash, meta.MetadataLocationRed)
	}
	if meta.Refs["main"].Name != "main" {
		t.Fatalf("ref name should be populated: %+v", meta.Refs["main"])
	}
	current, err := ResolveSnapshot(meta, SnapshotSelector{})
	if err != nil || current.SnapshotID != 22 {
		t.Fatalf("resolve current snapshot: snapshot=%+v err=%v", current, err)
	}
	byID, err := ResolveSnapshot(meta, SnapshotSelector{SnapshotID: 11, HasSnapshotID: true})
	if err != nil || byID.SnapshotID != 11 {
		t.Fatalf("resolve snapshot id: snapshot=%+v err=%v", byID, err)
	}
	byTime, err := ResolveSnapshot(meta, SnapshotSelector{TimestampMS: 1710000100000, HasTimestampMS: true})
	if err != nil || byTime.SnapshotID != 11 {
		t.Fatalf("resolve timestamp snapshot: snapshot=%+v err=%v", byTime, err)
	}
	byRef, err := ResolveSnapshot(meta, SnapshotSelector{RefName: "audit"})
	if err != nil || byRef.SnapshotID != 11 {
		t.Fatalf("resolve ref snapshot: snapshot=%+v err=%v", byRef, err)
	}
	schema, ok := meta.CurrentSchema()
	if !ok {
		t.Fatalf("current schema not found")
	}
	mapped, err := MapP0SchemaToMO(schema)
	if err != nil {
		t.Fatalf("map schema: %v", err)
	}
	got := make([]string, 0, len(mapped))
	for _, typ := range mapped {
		got = append(got, typ.String())
	}
	want := []string{"BIGINT", "TEXT", "DECIMAL(12,2)", "TIMESTAMP(6)"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("mapped types mismatch got=%v want=%v", got, want)
	}
}

func TestParseTableMetadataAllowsEmptySnapshotSentinel(t *testing.T) {
	meta, err := ParseTableMetadata([]byte(emptySnapshotMetadataJSON), "s3://warehouse/sales/empty_orders/metadata/v1.metadata.json")
	if err != nil {
		t.Fatalf("parse empty snapshot metadata: %v", err)
	}
	if HasCurrentSnapshot(meta) {
		t.Fatalf("empty Iceberg table must not report a current snapshot: %+v", meta.CurrentSnapshotID)
	}
	_, err = ResolveSnapshot(meta, SnapshotSelector{})
	if err == nil || !strings.Contains(err.Error(), "has no current snapshot") {
		t.Fatalf("expected no current snapshot error, got %v", err)
	}
}

func TestParseTableMetadataRejectsBrokenShape(t *testing.T) {
	_, err := ParseTableMetadata([]byte(`{"format-version":2,"location":"s3://t","current-schema-id":44,"schemas":[{"schema-id":1,"fields":[]}]}`), "s3://t/metadata.json")
	if err == nil || !strings.Contains(err.Error(), string(api.ErrMetadataInvalid)) {
		t.Fatalf("expected metadata invalid error, got %v", err)
	}
}

func TestParseTableMetadataRedactsMetadataLocationOnError(t *testing.T) {
	location := "s3://warehouse/sales/orders/metadata/broken.metadata.json"
	_, err := ParseTableMetadata([]byte(`{"format-version":`), location)
	if err == nil {
		t.Fatalf("expected invalid metadata error")
	}
	if strings.Contains(err.Error(), "warehouse") || strings.Contains(err.Error(), "orders") || !strings.Contains(err.Error(), "<redacted:path:") {
		t.Fatalf("metadata parse error should redact metadata location, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = NativeFacade{}.ParseTableMetadata(ctx, []byte(sampleMetadataJSON), location)
	if err == nil {
		t.Fatalf("expected canceled parse error")
	}
	if strings.Contains(err.Error(), "warehouse") || strings.Contains(err.Error(), "orders") || !strings.Contains(err.Error(), "<redacted:path:") {
		t.Fatalf("metadata facade error should redact metadata location, got %v", err)
	}
}

func TestP0TypeMappingRejectsUnsupportedTypes(t *testing.T) {
	cases := []string{
		"time",
		"fixed[16]",
		"uuid",
		"decimal(39,0)",
	}
	for _, raw := range cases {
		typ, parseErr := api.ParseIcebergTypeString(raw)
		if parseErr != nil && raw != "decimal(39,0)" {
			t.Fatalf("parse %s: %v", raw, parseErr)
		}
		if _, err := MapP0TypeToMO(typ, 1); err == nil {
			t.Fatalf("expected unsupported type error for %s", raw)
		}
	}
	var nested api.IcebergType
	if err := json.Unmarshal([]byte(`{"type":"struct","fields":[{"id":2,"name":"n","required":false,"type":"int"}]}`), &nested); err != nil {
		t.Fatalf("parse nested: %v", err)
	}
	if _, err := MapP0TypeToMO(nested, 1); err == nil {
		t.Fatalf("expected nested type to be rejected")
	}
}

func TestP0TypeMappingMatrix(t *testing.T) {
	success := map[string]string{
		"boolean":        "BOOL",
		"int":            "INT",
		"long":           "BIGINT",
		"float":          "FLOAT",
		"double":         "DOUBLE",
		"decimal(38, 6)": "DECIMAL(38,6)",
		"date":           "DATE",
		"timestamp":      "DATETIME(6)",
		"timestamptz":    "TIMESTAMP(6)",
		"string":         "TEXT",
		"binary":         "VARBINARY",
	}
	for raw, want := range success {
		typ, err := api.ParseIcebergTypeString(raw)
		if err != nil {
			t.Fatalf("parse %s: %v", raw, err)
		}
		got, err := MapP0TypeToMO(typ, 7)
		if err != nil {
			t.Fatalf("map %s: %v", raw, err)
		}
		if got.String() != want || got.IcebergID != 7 {
			t.Fatalf("map %s got=%s id=%d want=%s id=7", raw, got.String(), got.IcebergID, want)
		}
	}
	fail := []string{
		"time",
		"fixed[4]",
		"uuid",
		"variant",
		"geometry",
		"geography",
		"timestamp_ns",
	}
	for _, raw := range fail {
		typ, err := api.ParseIcebergTypeString(raw)
		if err != nil {
			t.Fatalf("parse %s: %v", raw, err)
		}
		if _, err := MapP0TypeToMO(typ, 8); err == nil {
			t.Fatalf("expected %s to fail mapping", raw)
		}
	}
}

func TestDetectUnsupportedP0Features(t *testing.T) {
	raw := `{
	  "format-version": 3,
	  "location": "s3://warehouse/t",
	  "current-schema-id": 1,
	  "schemas": [
	    {"schema-id": 1, "fields": [
	      {"id": 1, "name": "v", "required": false, "type": "variant"},
	      {"id": 2, "name": "d", "required": false, "type": "int", "write-default": 0}
	    ]}
	  ],
	  "partition-specs": [{"spec-id": 0, "fields": []}],
	  "default-spec-id": 0,
	  "properties": {"write.deletion-vector.enabled": "true"}
	}`
	meta, err := ParseTableMetadata([]byte(raw), "s3://warehouse/t/metadata.json")
	if err != nil {
		t.Fatalf("parse metadata: %v", err)
	}
	features := DetectUnsupportedP0Table(meta)
	if len(features) < 3 {
		t.Fatalf("expected multiple unsupported features, got %+v", features)
	}
	if err := ValidateP0Table(meta); err == nil || !strings.Contains(err.Error(), string(api.ErrUnsupportedFeature)) {
		t.Fatalf("expected unsupported feature error, got %v", err)
	}
}

func TestDetectUnsupportedP0FeatureCategories(t *testing.T) {
	raw := `{
	  "format-version": 4,
	  "location": "s3://warehouse/t",
	  "current-schema-id": 1,
	  "schemas": [
	    {"schema-id": 1, "fields": [
	      {"id": 1, "name": "variant_col", "required": false, "type": "variant"},
	      {"id": 2, "name": "defaulted_col", "required": false, "type": "int", "initial-default": 0},
	      {"id": 3, "name": "struct_col", "required": false, "type": {
	        "type": "struct",
	        "fields": [{"id": 4, "name": "nested_value", "required": false, "type": "string"}]
	      }},
	      {"id": 5, "name": "list_col", "required": false, "type": {
	        "type": "list",
	        "element-id": 6,
	        "element": "long"
	      }},
	      {"id": 7, "name": "map_col", "required": false, "type": {
	        "type": "map",
	        "key-id": 8,
	        "key": "string",
	        "value-id": 9,
	        "value": "long"
	      }}
	    ]}
	  ],
	  "partition-specs": [{"spec-id": 0, "fields": []}],
	  "default-spec-id": 0,
	  "properties": {
	    "metadata.puffin.enabled": "true",
	    "write.deletion-vector.enabled": "true",
	    "write.encryption.enabled": "true"
	  }
	}`
	meta, err := ParseTableMetadata([]byte(raw), "s3://warehouse/t/metadata-v4.json")
	if err != nil {
		t.Fatalf("parse metadata: %v", err)
	}
	features := DetectUnsupportedP0Table(meta)
	for _, want := range []string{
		"format-version",
		"puffin",
		"deletion-vector",
		"encryption",
		"variant",
		"field-default",
		"struct",
		"list",
		"map",
	} {
		if !hasUnsupportedFeature(features, want) {
			t.Fatalf("expected unsupported feature %q in %+v", want, features)
		}
	}
}

func TestValidateP0ManifestAndDataFile(t *testing.T) {
	if err := ValidateP0ManifestFile(api.ManifestFile{Content: api.ManifestContentDeletes, ManifestPathRedacted: api.RedactPath("s3://warehouse/t/delete.avro")}); err == nil {
		t.Fatalf("expected delete manifest to be rejected")
	}
	if err := ValidateP0DataFile(api.DataFile{Content: api.DataFileContentData, FileFormat: "parquet"}); err != nil {
		t.Fatalf("parquet data file should be accepted: %v", err)
	}
	if err := ValidateP0DataFile(api.DataFile{Content: api.DataFileContentData, FileFormat: "orc"}); err == nil {
		t.Fatalf("expected ORC data file to be rejected")
	}
	if err := ValidateP0DataFile(api.DataFile{Content: api.DataFileContentPositionDelete, FileFormat: "parquet"}); err == nil {
		t.Fatalf("expected position delete file to be rejected")
	}
}

func TestValidateP0ManifestFeatureDetection(t *testing.T) {
	firstRowID := int64(42)
	err := ValidateP0ManifestFile(api.ManifestFile{
		Content:              api.ManifestContentDeletes,
		KeyMetadata:          []byte("encrypted"),
		FirstRowID:           &firstRowID,
		ManifestPathRedacted: api.RedactPath("s3://warehouse/t/delete.avro"),
	})
	if err == nil {
		t.Fatalf("expected manifest unsupported feature error")
	}
	for _, want := range []string{"delete-manifest", "encrypted-manifest", "row-lineage"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected manifest error to mention %q, got %v", want, err)
		}
	}
}

func TestValidateP0DataFileFeatureDetection(t *testing.T) {
	err := ValidateP0DataFile(api.DataFile{
		Content:               api.DataFileContentEqualityDelete,
		FileFormat:            "avro",
		KeyMetadata:           []byte("key"),
		EncryptionKeyMetadata: []byte("wrapped-key"),
		DeletionVectorPath:    "s3://warehouse/t/data.dv",
		FilePathRedacted:      api.RedactPath("s3://warehouse/t/data.avro"),
	})
	if err == nil {
		t.Fatalf("expected data file unsupported feature error")
	}
	for _, want := range []string{"delete-file", "file-format", "encrypted-data", "deletion-vector"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected data file error to mention %q, got %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "warehouse") || strings.Contains(err.Error(), "data.dv") {
		t.Fatalf("data file feature error should redact paths, got %v", err)
	}
}

func hasUnsupportedFeature(features []api.UnsupportedFeature, want string) bool {
	for _, feature := range features {
		if feature.Feature == want {
			return true
		}
	}
	return false
}
