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

package write

import (
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergref "github.com/matrixorigin/matrixone/pkg/iceberg/ref"
)

func NormalizeAppendTargetRef(req api.AppendRequest) (api.AppendRequest, error) {
	spec, err := appendRefSpec(req)
	if err != nil {
		return api.AppendRequest{}, err
	}
	if err := icebergref.ValidateWrite(spec, req.CatalogCapabilities, req.AllowTagMove); err != nil {
		return api.AppendRequest{}, err
	}
	if strings.TrimSpace(spec.Name) != "" {
		req.TargetRef = spec.Name
	}
	if spec.Type != "" {
		req.TargetRefType = string(spec.Type)
	}
	return req, nil
}

func ValidateAppendPreflight(req api.AppendRequest) error {
	if strings.TrimSpace(req.Table) == "" || len(req.Namespace) == 0 {
		return api.NewError(api.ErrConfigInvalid, "Iceberg append requires namespace and table", nil)
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return api.NewError(api.ErrConfigInvalid, "Iceberg append requires an idempotency key", nil)
	}
	if _, err := NormalizeAppendTargetRef(req); err != nil {
		return err
	}
	if err := validateWriterOwner(req); err != nil {
		return err
	}
	fieldsByID, err := validateBaseSchema(req)
	if err != nil {
		return err
	}
	_, err = validatePartitionSpecs(req, fieldsByID)
	return err
}

func appendRefSpec(req api.AppendRequest) (icebergref.Spec, error) {
	targetRef := strings.TrimSpace(req.TargetRef)
	if targetRef == "" {
		targetRef = "main"
	}
	refType := strings.ToLower(strings.TrimSpace(req.TargetRefType))
	if refType == "" {
		return icebergref.ParseNessieRef(targetRef, nil)
	}
	return icebergref.Spec{Name: targetRef, Type: icebergref.Type(refType)}, nil
}

func ValidateAppendRequest(req api.AppendRequest) error {
	if err := ValidateAppendPreflight(req); err != nil {
		return err
	}
	if len(req.DataFiles) == 0 {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg append requires at least one data file", nil)
	}
	fieldsByID, _ := validateBaseSchema(req)
	specsByID, _ := validatePartitionSpecs(req, fieldsByID)
	return validateAppendDataFiles(req, specsByID)
}

func validateWriterOwner(req api.AppendRequest) error {
	accountID := req.Catalog.AccountID
	if accountID == 0 {
		return nil
	}
	if req.WriterOwnerAccountID == 0 {
		return api.NewError(api.ErrConfigInvalid, "Iceberg append requires a single-writer owner", map[string]string{
			"account_id": strconv.FormatUint(uint64(accountID), 10),
			"table":      req.Table,
		})
	}
	if req.WriterOwnerAccountID != accountID {
		return api.NewError(api.ErrConfigInvalid, "Iceberg append writer owner does not match account", map[string]string{
			"account_id":              strconv.FormatUint(uint64(accountID), 10),
			"writer_owner_account_id": strconv.FormatUint(uint64(req.WriterOwnerAccountID), 10),
			"table":                   req.Table,
		})
	}
	return nil
}

func validateBaseSchema(req api.AppendRequest) (map[int]api.SchemaField, error) {
	if req.BaseSchemaID == 0 && len(req.BaseSchema.Fields) == 0 {
		return nil, nil
	}
	if req.BaseSchema.SchemaID != req.BaseSchemaID || len(req.BaseSchema.Fields) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg append requires the current table schema", map[string]string{
			"schema_id": strconv.Itoa(req.BaseSchemaID),
			"table":     req.Table,
		})
	}
	fieldsByID := make(map[int]api.SchemaField, len(req.BaseSchema.Fields))
	for _, field := range req.BaseSchema.Fields {
		if field.ID == 0 || strings.TrimSpace(field.Name) == "" {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg append schema has an invalid field", map[string]string{
				"schema_id": strconv.Itoa(req.BaseSchemaID),
				"table":     req.Table,
			})
		}
		if _, exists := fieldsByID[field.ID]; exists {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg append schema has duplicate field ids", map[string]string{
				"field_id": strconv.Itoa(field.ID),
				"table":    req.Table,
			})
		}
		fieldsByID[field.ID] = field
	}
	return fieldsByID, nil
}

func validatePartitionSpecs(req api.AppendRequest, fieldsByID map[int]api.SchemaField) (map[int]api.PartitionSpec, error) {
	specsByID := make(map[int]api.PartitionSpec, len(req.KnownPartitionSpecs)+1)
	if req.BaseSpecID != 0 || len(req.BaseSpec.Fields) > 0 {
		if req.BaseSpec.SpecID != req.BaseSpecID {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg append requires the current partition spec", map[string]string{
				"spec_id": strconv.Itoa(req.BaseSpecID),
				"table":   req.Table,
			})
		}
		if err := validatePartitionSpec(req, req.BaseSpec, fieldsByID); err != nil {
			return nil, err
		}
		specsByID[req.BaseSpec.SpecID] = req.BaseSpec
	}
	for _, spec := range req.KnownPartitionSpecs {
		if _, exists := specsByID[spec.SpecID]; exists {
			continue
		}
		if err := validatePartitionSpec(req, spec, fieldsByID); err != nil {
			return nil, err
		}
		specsByID[spec.SpecID] = spec
	}
	if len(specsByID) == 0 {
		return nil, nil
	}
	return specsByID, nil
}

func validatePartitionSpec(req api.AppendRequest, spec api.PartitionSpec, fieldsByID map[int]api.SchemaField) error {
	if len(spec.Fields) == 0 {
		return nil
	}
	seenFieldIDs := make(map[int]struct{}, len(spec.Fields))
	for _, field := range spec.Fields {
		if field.FieldID == 0 || field.SourceID == 0 || strings.TrimSpace(field.Name) == "" || strings.TrimSpace(field.Transform) == "" {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg append partition spec has an invalid field", map[string]string{
				"spec_id": strconv.Itoa(spec.SpecID),
				"table":   req.Table,
			})
		}
		if _, exists := seenFieldIDs[field.FieldID]; exists {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg append partition spec has duplicate field ids", map[string]string{
				"field_id": strconv.Itoa(field.FieldID),
				"table":    req.Table,
			})
		}
		seenFieldIDs[field.FieldID] = struct{}{}
		if fieldsByID != nil {
			if _, exists := fieldsByID[field.SourceID]; !exists {
				return api.NewError(api.ErrMetadataInvalid, "Iceberg append partition spec references an unknown source field", map[string]string{
					"source_id": strconv.Itoa(field.SourceID),
					"table":     req.Table,
				})
			}
		}
	}
	return nil
}

func validateAppendDataFiles(req api.AppendRequest, specsByID map[int]api.PartitionSpec) error {
	var totalRows, totalBytes int64
	for _, file := range req.DataFiles {
		if strings.TrimSpace(file.FilePath) == "" {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg append data file requires a path", map[string]string{"table": req.Table})
		}
		if file.Content != api.DataFileContentData {
			return api.NewError(api.ErrUnsupportedFeature, "Iceberg append supports only data files", map[string]string{
				"path": api.RedactPath(file.FilePath),
			})
		}
		if !strings.EqualFold(strings.TrimSpace(file.FileFormat), "parquet") {
			return api.NewError(api.ErrUnsupportedFeature, "Iceberg append supports only Parquet data files", map[string]string{
				"path":   api.RedactPath(file.FilePath),
				"format": file.FileFormat,
			})
		}
		if file.RecordCount < 0 || file.FileSizeInBytes < 0 {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg append data file has negative metrics", map[string]string{
				"path": api.RedactPath(file.FilePath),
			})
		}
		if file.RecordCount > 0 && file.FileSizeInBytes == 0 {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg append data file with rows requires a file size", map[string]string{
				"path": api.RedactPath(file.FilePath),
			})
		}
		if file.RecordCount > math.MaxInt64-totalRows || file.FileSizeInBytes > math.MaxInt64-totalBytes {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg append data file metrics overflow", map[string]string{
				"path": api.RedactPath(file.FilePath),
			})
		}
		totalRows += file.RecordCount
		totalBytes += file.FileSizeInBytes
		if len(specsByID) != 0 {
			spec, exists := specsByID[file.SpecID]
			if !exists {
				return api.NewError(api.ErrMetadataInvalid, "Iceberg append data file uses an unknown partition spec id", map[string]string{
					"path":    api.RedactPath(file.FilePath),
					"spec_id": strconv.Itoa(file.SpecID),
				})
			}
			if err := validateDataFilePartition(req, spec, file); err != nil {
				return err
			}
		}
		if err := validateDataFileMetrics(req, file); err != nil {
			return err
		}
	}
	return nil
}

func validateDataFilePartition(req api.AppendRequest, spec api.PartitionSpec, file api.DataFile) error {
	if len(spec.Fields) == 0 {
		return nil
	}
	if file.Partition == nil {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg append data file is missing partition tuple", map[string]string{
			"path": api.RedactPath(file.FilePath),
		})
	}
	for _, field := range spec.Fields {
		if _, exists := file.Partition[field.Name]; !exists {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg append data file partition tuple is missing a field", map[string]string{
				"path":            api.RedactPath(file.FilePath),
				"partition_field": field.Name,
			})
		}
	}
	return nil
}

func validateDataFileMetrics(req api.AppendRequest, file api.DataFile) error {
	for name, counts := range map[string]map[int]int64{
		"column_size": countsOrNil(file.ColumnSizes),
		"value_count": countsOrNil(file.ValueCounts),
		"null_count":  countsOrNil(file.NullValueCounts),
		"nan_count":   countsOrNil(file.NaNValueCounts),
	} {
		for fieldID, count := range counts {
			if count < 0 {
				return api.NewError(api.ErrMetadataInvalid, "Iceberg append data file has negative column metrics", map[string]string{
					"path":       api.RedactPath(file.FilePath),
					"metric":     name,
					"field_id":   strconv.Itoa(fieldID),
					"metric_val": strconv.FormatInt(count, 10),
				})
			}
		}
	}
	for _, field := range req.BaseSchema.Fields {
		if !field.Required {
			continue
		}
		nullCount := file.NullValueCounts[field.ID]
		if nullCount > 0 {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg append required column contains nulls", map[string]string{
				"path":     api.RedactPath(file.FilePath),
				"field_id": strconv.Itoa(field.ID),
			})
		}
		if valueCount, exists := file.ValueCounts[field.ID]; exists {
			if valueCount < nullCount {
				return api.NewError(api.ErrMetadataInvalid, "Iceberg append data file metrics are inconsistent", map[string]string{
					"path":     api.RedactPath(file.FilePath),
					"field_id": strconv.Itoa(field.ID),
				})
			}
		}
	}
	return nil
}

func countsOrNil(counts map[int]int64) map[int]int64 {
	if len(counts) == 0 {
		return nil
	}
	return counts
}
