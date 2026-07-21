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
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func ParseTableMetadata(data []byte, metadataLocation string) (*api.TableMetadata, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg metadata JSON is empty", nil)
	}
	var meta api.TableMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg metadata JSON is invalid", map[string]string{"metadata_location": api.RedactPath(metadataLocation)}, err)
	}
	meta.RawJSON = append([]byte(nil), data...)
	meta.MetadataLocation = metadataLocation
	meta.MetadataLocationHash = api.PathHash(metadataLocation)
	meta.MetadataLocationRed = api.RedactPath(metadataLocation)
	if meta.Refs == nil {
		meta.Refs = make(map[string]api.SnapshotRef)
	}
	for name, ref := range meta.Refs {
		ref.Name = name
		meta.Refs[name] = ref
	}
	if err := validateTableMetadataShape(&meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func validateTableMetadataShape(meta *api.TableMetadata) error {
	if meta.FormatVersion <= 0 {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg metadata is missing format-version", nil)
	}
	if strings.TrimSpace(meta.Location) == "" {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg metadata is missing table location", nil)
	}
	if len(meta.Schemas) == 0 {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg metadata is missing schemas", nil)
	}
	if _, ok := meta.CurrentSchema(); !ok {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg metadata current schema id was not found", map[string]string{"schema_id": strconv.Itoa(meta.CurrentSchemaID)})
	}
	if len(meta.PartitionSpecs) > 0 {
		if _, ok := meta.DefaultSpec(); !ok {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg metadata default partition spec id was not found", map[string]string{"spec_id": strconv.Itoa(meta.DefaultSpecID)})
		}
	}
	if HasCurrentSnapshot(meta) {
		if _, ok := FindSnapshot(meta, *meta.CurrentSnapshotID); !ok {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg metadata current snapshot id was not found", map[string]string{"snapshot_id": strconv.FormatInt(*meta.CurrentSnapshotID, 10)})
		}
	}
	return nil
}

const NoCurrentSnapshotID int64 = -1

func HasCurrentSnapshot(meta *api.TableMetadata) bool {
	return meta != nil && meta.CurrentSnapshotID != nil && !isNoCurrentSnapshotID(*meta.CurrentSnapshotID)
}

func isNoCurrentSnapshotID(snapshotID int64) bool {
	return snapshotID == NoCurrentSnapshotID
}

func FindSnapshot(meta *api.TableMetadata, snapshotID int64) (api.Snapshot, bool) {
	if meta == nil {
		return api.Snapshot{}, false
	}
	for _, snapshot := range meta.Snapshots {
		if snapshot.SnapshotID == snapshotID {
			return snapshot, true
		}
	}
	return api.Snapshot{}, false
}

func ResolveSnapshot(meta *api.TableMetadata, selector SnapshotSelector) (api.Snapshot, error) {
	if meta == nil {
		return api.Snapshot{}, api.NewError(api.ErrMetadataInvalid, "Iceberg metadata is nil", nil)
	}
	if selector.HasSnapshotID {
		return resolveSnapshotID(meta, selector.SnapshotID)
	}
	if selector.HasTimestampMS {
		return resolveSnapshotAtTimestamp(meta, selector.TimestampMS)
	}
	if strings.TrimSpace(selector.RefName) != "" {
		return resolveSnapshotRef(meta, strings.TrimSpace(selector.RefName), selector.AllowMainFallback)
	}
	if !HasCurrentSnapshot(meta) {
		return api.Snapshot{}, noCurrentSnapshotError()
	}
	return resolveSnapshotID(meta, *meta.CurrentSnapshotID)
}

func resolveSnapshotID(meta *api.TableMetadata, snapshotID int64) (api.Snapshot, error) {
	if snapshot, ok := FindSnapshot(meta, snapshotID); ok {
		return snapshot, nil
	}
	return api.Snapshot{}, api.NewError(api.ErrTableNotFound, "Iceberg snapshot was not found", map[string]string{"snapshot_id": strconv.FormatInt(snapshotID, 10)})
}

func resolveSnapshotRef(meta *api.TableMetadata, refName string, allowMainFallback bool) (api.Snapshot, error) {
	if ref, ok := meta.Refs[refName]; ok {
		if isNoCurrentSnapshotID(ref.SnapshotID) {
			return api.Snapshot{}, noCurrentSnapshotError()
		}
		return resolveSnapshotID(meta, ref.SnapshotID)
	}
	if refName == model.DefaultRefMain && allowMainFallback && HasCurrentSnapshot(meta) {
		return resolveSnapshotID(meta, *meta.CurrentSnapshotID)
	}
	return api.Snapshot{}, api.NewError(api.ErrTableNotFound, "Iceberg snapshot ref was not found", map[string]string{"ref": refName})
}

func noCurrentSnapshotError() error {
	return api.NewError(api.ErrTableNotFound, "Iceberg table has no current snapshot", nil)
}

func resolveSnapshotAtTimestamp(meta *api.TableMetadata, timestampMS int64) (api.Snapshot, error) {
	var chosen *api.SnapshotLogEntry
	for i := range meta.SnapshotLog {
		entry := &meta.SnapshotLog[i]
		if entry.TimestampMS <= timestampMS && (chosen == nil || entry.TimestampMS > chosen.TimestampMS) {
			chosen = entry
		}
	}
	if chosen != nil {
		return resolveSnapshotID(meta, chosen.SnapshotID)
	}
	var snapshots []api.Snapshot
	snapshots = append(snapshots, meta.Snapshots...)
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].TimestampMS < snapshots[j].TimestampMS })
	for i := len(snapshots) - 1; i >= 0; i-- {
		if snapshots[i].TimestampMS <= timestampMS {
			return snapshots[i], nil
		}
	}
	return api.Snapshot{}, api.NewError(api.ErrTableNotFound, "Iceberg snapshot timestamp was not found", map[string]string{"timestamp_ms": strconv.FormatInt(timestampMS, 10)})
}

func DetectUnsupportedP0Table(meta *api.TableMetadata) []api.UnsupportedFeature {
	if meta == nil {
		return []api.UnsupportedFeature{{Feature: "metadata", Reason: "metadata is nil"}}
	}
	var features []api.UnsupportedFeature
	if meta.FormatVersion > 2 {
		features = append(features, api.UnsupportedFeature{
			Feature: "format-version",
			Reason:  "P0 supports Iceberg format v1/v2 only",
			Path:    "format-version",
		})
	}
	for key, value := range meta.Properties {
		features = append(features, detectUnsupportedProperty(key, value)...)
	}
	for _, schema := range meta.Schemas {
		for _, field := range schema.Fields {
			features = append(features, detectUnsupportedField(field, "schema."+strconv.Itoa(schema.SchemaID)+"."+field.Name)...)
		}
	}
	return features
}

func ValidateP0Table(meta *api.TableMetadata) error {
	return unsupportedFeaturesError(DetectUnsupportedP0Table(meta))
}

func ValidateP0ManifestFile(manifest api.ManifestFile) error {
	var features []api.UnsupportedFeature
	if manifest.Content == api.ManifestContentDeletes {
		features = append(features, api.UnsupportedFeature{Feature: "delete-manifest", Reason: "P0 read path does not apply delete manifests", Path: manifest.ManifestPathRedacted})
	}
	if len(manifest.KeyMetadata) > 0 {
		features = append(features, api.UnsupportedFeature{Feature: "encrypted-manifest", Reason: "P0 read path does not support encrypted manifest metadata", Path: manifest.ManifestPathRedacted})
	}
	if manifest.FirstRowID != nil {
		features = append(features, api.UnsupportedFeature{Feature: "row-lineage", Reason: "P0 read path does not support v3 row lineage", Path: manifest.ManifestPathRedacted})
	}
	return unsupportedFeaturesError(features)
}

func ValidateP0DataFile(file api.DataFile) error {
	if file.RecordCount < 0 {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg data file record count is negative", map[string]string{"path": file.FilePathRedacted})
	}
	if file.FileSizeInBytes < 0 {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg data file size is negative", map[string]string{"path": file.FilePathRedacted})
	}
	if file.RecordCount > 0 && file.FileSizeInBytes == 0 {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg data file with rows is missing file size", map[string]string{"path": file.FilePathRedacted})
	}
	var features []api.UnsupportedFeature
	if file.Content != api.DataFileContentData {
		features = append(features, api.UnsupportedFeature{Feature: "delete-file", Reason: "P0 read path does not apply delete files", Path: file.FilePathRedacted})
	}
	if strings.ToLower(strings.TrimSpace(file.FileFormat)) != "parquet" {
		features = append(features, api.UnsupportedFeature{Feature: "file-format", Reason: "P0 read path supports Parquet data files only", Path: file.FilePathRedacted})
	}
	if len(file.KeyMetadata) > 0 || len(file.EncryptionKeyMetadata) > 0 {
		features = append(features, api.UnsupportedFeature{Feature: "encrypted-data", Reason: "P0 read path does not support encrypted data files", Path: file.FilePathRedacted})
	}
	if strings.TrimSpace(file.DeletionVectorPath) != "" {
		features = append(features, api.UnsupportedFeature{Feature: "deletion-vector", Reason: "P0 read path does not support deletion vectors", Path: api.RedactPath(file.DeletionVectorPath)})
	}
	return unsupportedFeaturesError(features)
}

func detectUnsupportedProperty(key, value string) []api.UnsupportedFeature {
	k := strings.ToLower(strings.TrimSpace(key))
	v := strings.ToLower(strings.TrimSpace(value))
	combined := k + "=" + v
	var out []api.UnsupportedFeature
	if k == "format-version" {
		if n, err := strconv.Atoi(v); err == nil && n > 2 {
			out = append(out, api.UnsupportedFeature{Feature: "format-version", Reason: "P0 supports Iceberg format v1/v2 only", Path: "properties." + key})
		}
	}
	for _, marker := range []string{"puffin", "deletion-vector", "deletion_vector", "encryption", "encrypted", "variant", "geometry", "geography", "timestamp-ns", "timestamp_ns", "nanosecond"} {
		if strings.Contains(combined, marker) {
			out = append(out, api.UnsupportedFeature{Feature: marker, Reason: "P0 unsupported Iceberg feature flag", Path: "properties." + key})
		}
	}
	return out
}

func detectUnsupportedField(field api.SchemaField, path string) []api.UnsupportedFeature {
	var features []api.UnsupportedFeature
	if len(field.InitialDefault) > 0 || len(field.WriteDefault) > 0 {
		features = append(features, api.UnsupportedFeature{Feature: "field-default", Reason: "P0 rejects Iceberg v3 default value semantics", Path: path})
	}
	if _, err := MapP0TypeToMO(field.Type, field.ID); err != nil {
		if icebergErr, ok := err.(*api.IcebergError); ok {
			features = append(features, api.UnsupportedFeature{Feature: icebergErr.Fields["type"], Reason: icebergErr.Message, Path: path})
		} else {
			features = append(features, api.UnsupportedFeature{Feature: field.Type.String(), Reason: err.Error(), Path: path})
		}
	}
	for _, nested := range field.Type.Fields {
		features = append(features, detectUnsupportedField(nested, path+"."+nested.Name)...)
	}
	return features
}

func MapP0SchemaToMO(schema api.Schema) ([]api.MOType, error) {
	out := make([]api.MOType, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		typ, err := MapP0TypeToMO(field.Type, field.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, typ)
	}
	return out, nil
}

func MapP0TypeToMO(typ api.IcebergType, fieldID int) (api.MOType, error) {
	switch typ.Kind {
	case api.TypeBoolean:
		return api.MOType{Name: "BOOL", IcebergID: fieldID}, nil
	case api.TypeInt:
		return api.MOType{Name: "INT", IcebergID: fieldID}, nil
	case api.TypeLong:
		return api.MOType{Name: "BIGINT", IcebergID: fieldID}, nil
	case api.TypeFloat:
		return api.MOType{Name: "FLOAT", IcebergID: fieldID}, nil
	case api.TypeDouble:
		return api.MOType{Name: "DOUBLE", IcebergID: fieldID}, nil
	case api.TypeDecimal:
		if typ.Precision <= 0 || typ.Precision > api.MaxMODecimalPrecision || typ.Scale < 0 || typ.Scale > typ.Precision {
			return api.MOType{}, api.NewError(api.ErrUnsupportedFeature, "Iceberg decimal type exceeds MO P0 mapping", map[string]string{"type": typ.String(), "field_id": strconv.Itoa(fieldID)})
		}
		return api.MOType{Name: "DECIMAL", Width: typ.Precision, Scale: typ.Scale, IcebergID: fieldID}, nil
	case api.TypeDate:
		return api.MOType{Name: "DATE", IcebergID: fieldID}, nil
	case api.TypeTimestamp:
		return api.MOType{Name: "DATETIME(6)", IcebergID: fieldID}, nil
	case api.TypeTimestampTZ:
		return api.MOType{Name: "TIMESTAMP(6)", IcebergID: fieldID}, nil
	case api.TypeString:
		return api.MOType{Name: "TEXT", IcebergID: fieldID}, nil
	case api.TypeBinary:
		return api.MOType{Name: "VARBINARY", Width: types.MaxVarBinaryLen, IcebergID: fieldID}, nil
	default:
		return api.MOType{}, api.NewError(api.ErrUnsupportedFeature, "Iceberg type is not supported by P0 read mapping", map[string]string{"type": typ.String(), "field_id": strconv.Itoa(fieldID)})
	}
}

func unsupportedFeaturesError(features []api.UnsupportedFeature) error {
	if len(features) == 0 {
		return nil
	}
	featureNames := make([]string, 0, len(features))
	for _, feature := range features {
		if strings.TrimSpace(feature.Feature) != "" {
			featureNames = append(featureNames, feature.Feature)
		}
	}
	sort.Strings(featureNames)
	if len(featureNames) > 6 {
		featureNames = featureNames[:6]
	}
	return api.NewError(api.ErrUnsupportedFeature, "Iceberg table uses unsupported P0 feature", map[string]string{"features": strings.Join(featureNames, ","), "count": strconv.Itoa(len(features))})
}
