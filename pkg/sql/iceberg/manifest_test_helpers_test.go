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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

func encodeSQLIcebergTestManifest(entries []api.ManifestEntry) ([]byte, error) {
	content := api.ManifestContentData
	specID := 0
	if len(entries) > 0 {
		specID = entries[0].DataFile.SpecID
		if entries[0].DataFile.Content != api.DataFileContentData {
			content = api.ManifestContentDeletes
		}
	}
	names := make([]string, 0)
	values := make(map[string]any)
	fieldIDs := make(map[string]int)
	for _, entry := range entries {
		for name, value := range entry.DataFile.Partition {
			if _, ok := values[name]; !ok {
				names = append(names, name)
				values[name] = value
			}
			if id := entry.DataFile.PartitionFieldIDs[name]; id != 0 {
				fieldIDs[name] = id
			}
		}
	}
	sort.Strings(names)
	schemaFields := []api.SchemaField{{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}}}
	partitionFields := make([]api.PartitionField, 0, len(names))
	for idx, name := range names {
		sourceID := 100 + idx
		fieldID := fieldIDs[name]
		if fieldID == 0 {
			fieldID = 1000 + idx
		}
		schemaFields = append(schemaFields, api.SchemaField{ID: sourceID, Name: name, Type: sqlIcebergTestPartitionType(values[name])})
		partitionFields = append(partitionFields, api.PartitionField{SourceID: sourceID, FieldID: fieldID, Name: name, Transform: "identity"})
	}
	return metadata.EncodeManifest(entries, metadata.ManifestWriteOptions{
		FormatVersion: 2,
		Schema:        api.Schema{SchemaID: 1, Fields: schemaFields},
		PartitionSpec: api.PartitionSpec{SpecID: specID, Fields: partitionFields},
		Content:       content,
	})
}

func encodeSQLIcebergTestManifestList(manifests []api.ManifestFile) ([]byte, error) {
	parentSnapshotID := int64(29)
	return metadata.EncodeManifestList(manifests, metadata.ManifestListWriteOptions{
		FormatVersion:    2,
		SnapshotID:       30,
		ParentSnapshotID: &parentSnapshotID,
		SequenceNumber:   30,
	})
}

func sqlIcebergTestPartitionType(value any) api.IcebergType {
	switch value.(type) {
	case bool:
		return api.IcebergType{Kind: api.TypeBoolean}
	case int64:
		return api.IcebergType{Kind: api.TypeLong}
	case float32:
		return api.IcebergType{Kind: api.TypeFloat}
	case float64:
		return api.IcebergType{Kind: api.TypeDouble}
	case string, nil:
		return api.IcebergType{Kind: api.TypeString}
	default:
		return api.IcebergType{Kind: api.TypeInt}
	}
}

func withSQLTestDMLExecutorMetadata(executor DMLActionExecutor, specIDs ...int) DMLActionExecutor {
	executor.FormatVersion = 2
	executor.Schema = api.Schema{SchemaID: 9, Fields: []api.SchemaField{{
		ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong},
	}}}
	if len(specIDs) == 0 {
		specIDs = []int{0}
	}
	executor.PartitionSpecs = make([]api.PartitionSpec, 0, len(specIDs))
	for _, specID := range specIDs {
		executor.PartitionSpecs = append(executor.PartitionSpecs, api.PartitionSpec{SpecID: specID})
	}
	return executor
}
