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
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type deleteManifestEntry struct {
	manifestPath string
	file         api.DataFile
}

func ValidateP1DeleteFile(file api.DataFile) error {
	if file.RecordCount < 0 {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg delete file record count is negative", map[string]string{"path": file.FilePathRedacted})
	}
	if file.FileSizeInBytes < 0 {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg delete file size is negative", map[string]string{"path": file.FilePathRedacted})
	}
	if !strings.EqualFold(strings.TrimSpace(file.FileFormat), "parquet") {
		return api.NewError(api.ErrUnsupportedFeature, "Iceberg delete apply supports Parquet delete files only", map[string]string{
			"path":   file.FilePathRedacted,
			"format": file.FileFormat,
		})
	}
	switch file.Content {
	case api.DataFileContentPositionDelete:
		// referenced_data_file is optional in Iceberg metadata. When it is absent,
		// the row-level file_path column in the delete file is the source of truth.
	case api.DataFileContentEqualityDelete:
		if len(file.EqualityIDs) == 0 {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg equality delete requires equality field ids", map[string]string{
				"path": file.FilePathRedacted,
			})
		}
	default:
		return api.NewError(api.ErrUnsupportedFeature, "Iceberg delete manifest contained an unsupported file content type", map[string]string{
			"path":    file.FilePathRedacted,
			"content": strconv.Itoa(int(file.Content)),
		})
	}
	if len(file.KeyMetadata) > 0 || len(file.EncryptionKeyMetadata) > 0 {
		return api.NewError(api.ErrUnsupportedFeature, "Iceberg delete apply does not support encrypted delete files", map[string]string{
			"path": file.FilePathRedacted,
		})
	}
	if strings.TrimSpace(file.DeletionVectorPath) != "" {
		return api.NewError(api.ErrUnsupportedFeature, "Iceberg delete apply does not support deletion vectors", map[string]string{
			"path": api.RedactPath(file.DeletionVectorPath),
		})
	}
	return nil
}

func pairDeleteTasks(dataTasks []api.DataFileTask, deleteEntries []deleteManifestEntry, credentialScope string) ([]api.DeleteFileTask, error) {
	if len(deleteEntries) == 0 {
		return nil, nil
	}
	out := make([]api.DeleteFileTask, 0)
	dataByPath := make(map[string]api.DataFileTask, len(dataTasks))
	for _, task := range dataTasks {
		path := strings.TrimSpace(task.DataFile.FilePath)
		if path != "" {
			dataByPath[path] = task
		}
	}
	seen := make(map[string]struct{})
	appendDeleteTask := func(entry deleteManifestEntry, file api.DataFile, dataPath string) {
		dataPath = strings.TrimSpace(dataPath)
		if dataPath == "" {
			return
		}
		key := deleteTaskIdentity(entry.manifestPath, file.FilePath, dataPath)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, api.DeleteFileTask{
			DataFile:        file,
			ManifestPath:    entry.manifestPath,
			AppliesToPath:   dataPath,
			CredentialScope: credentialScope,
			DeleteSchemaID:  file.DeleteSchemaID,
			SequenceNumber:  file.SequenceNumber,
		})
	}
	for _, entry := range deleteEntries {
		file := normalizeDataFile(entry.file)
		switch file.Content {
		case api.DataFileContentPositionDelete:
			if referenced := strings.TrimSpace(file.ReferencedDataFile); referenced != "" {
				dataTask, ok := dataByPath[referenced]
				if !ok || !deleteSequenceApplies(dataTask.DataFile, file) {
					continue
				}
				appendDeleteTask(entry, file, referenced)
				continue
			}
			for _, dataTask := range dataTasks {
				if !deleteSequenceApplies(dataTask.DataFile, file) {
					continue
				}
				if file.SpecID != 0 && dataTask.DataFile.SpecID != 0 && file.SpecID != dataTask.DataFile.SpecID {
					continue
				}
				if !samePartitionScope(file.Partition, dataTask.DataFile.Partition) {
					continue
				}
				appendDeleteTask(entry, file, dataTask.DataFile.FilePath)
			}
		case api.DataFileContentEqualityDelete:
			for _, dataTask := range dataTasks {
				if !deleteSequenceApplies(dataTask.DataFile, file) {
					continue
				}
				if file.SpecID != 0 && dataTask.DataFile.SpecID != 0 && file.SpecID != dataTask.DataFile.SpecID {
					continue
				}
				if !samePartitionScope(file.Partition, dataTask.DataFile.Partition) {
					continue
				}
				appendDeleteTask(entry, file, dataTask.DataFile.FilePath)
			}
		default:
			return nil, ValidateP1DeleteFile(file)
		}
	}
	return out, nil
}

func deleteTaskIdentity(parts ...string) string {
	var out strings.Builder
	for _, part := range parts {
		out.WriteString(strconv.Itoa(len(part)))
		out.WriteByte(':')
		out.WriteString(part)
	}
	return out.String()
}

func addHiddenDeleteColumnMappings(mappings []api.IcebergColumnMapping, schema api.Schema, deleteTasks []api.DeleteFileTask) ([]api.IcebergColumnMapping, error) {
	if len(deleteTasks) == 0 {
		return mappings, nil
	}
	needs := make(map[int]struct{})
	for _, task := range deleteTasks {
		if task.DataFile.Content != api.DataFileContentEqualityDelete {
			continue
		}
		for _, fieldID := range task.DataFile.EqualityIDs {
			if fieldID > 0 {
				needs[fieldID] = struct{}{}
			}
		}
	}
	if len(needs) == 0 {
		return mappings, nil
	}
	for i := range mappings {
		if _, ok := needs[mappings[i].FieldID]; !ok {
			continue
		}
		if !mappings[i].Projected {
			mappings[i].Projected = true
			mappings[i].Hidden = true
		}
		delete(needs, mappings[i].FieldID)
	}
	if len(needs) == 0 {
		return mappings, nil
	}
	fields := make(map[int]api.SchemaField, len(schema.Fields))
	for _, field := range schema.Fields {
		fields[field.ID] = field
	}
	for fieldID := range needs {
		field, ok := fields[fieldID]
		if !ok {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg equality delete references an unknown field id", map[string]string{
				"field_id": strconv.Itoa(fieldID),
			})
		}
		moType, err := MapP0TypeToMO(field.Type, field.ID)
		if err != nil {
			return nil, err
		}
		mappings = append(mappings, api.IcebergColumnMapping{
			FieldID:        field.ID,
			ColumnName:     field.Name,
			MOType:         moType,
			Required:       field.Required,
			Projected:      true,
			ParquetFieldID: field.ID,
			Hidden:         true,
		})
	}
	return mappings, nil
}

func deleteSequenceApplies(dataFile, deleteFile api.DataFile) bool {
	if deleteFile.SequenceNumber == 0 || dataFile.SequenceNumber == 0 {
		return true
	}
	if deleteFile.Content == api.DataFileContentPositionDelete {
		return deleteFile.SequenceNumber >= dataFile.SequenceNumber
	}
	return deleteFile.SequenceNumber > dataFile.SequenceNumber
}

func samePartitionScope(deletePartition, dataPartition map[string]any) bool {
	if len(deletePartition) == 0 {
		return true
	}
	if len(deletePartition) != len(dataPartition) {
		return false
	}
	for key, deleteValue := range deletePartition {
		dataValue, ok := dataPartition[key]
		if !ok {
			return false
		}
		if partitionValueToken(deleteValue) != partitionValueToken(dataValue) {
			return false
		}
	}
	return true
}

func partitionValueToken(value any) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case bool:
		return "b:" + strconv.FormatBool(v)
	case int:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int8:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int16:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int32:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int64:
		return "i:" + strconv.FormatInt(v, 10)
	case uint:
		return unsignedPartitionValueToken(uint64(v))
	case uint8:
		return unsignedPartitionValueToken(uint64(v))
	case uint16:
		return unsignedPartitionValueToken(uint64(v))
	case uint32:
		return unsignedPartitionValueToken(uint64(v))
	case uint64:
		return unsignedPartitionValueToken(v)
	case float32:
		return "f:" + strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		return "f:" + strconv.FormatFloat(v, 'g', -1, 64)
	case string:
		return "s:" + v
	case []byte:
		return "bytes:" + string(v)
	default:
		return fmt.Sprintf("%T:%#v", value, value)
	}
}

func unsignedPartitionValueToken(value uint64) string {
	if value <= math.MaxInt64 {
		return "i:" + strconv.FormatInt(int64(value), 10)
	}
	return "u:" + strconv.FormatUint(value, 10)
}

func firstNonZeroInt(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}
