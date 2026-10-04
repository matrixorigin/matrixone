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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	optionCatalog    = "catalog"
	optionNamespace  = "namespace"
	optionTable      = "table"
	optionRef        = "ref"
	optionDefaultRef = "default_ref"
	optionReadMode   = "read_mode"
	optionWriteMode  = "write_mode"
)

type TableMappingSpec struct {
	CatalogName string
	Mapping     model.TableMapping
}

func ParseTableMappingSpec(ctx context.Context, param *tree.IcebergTableParam) (TableMappingSpec, error) {
	if param == nil {
		return TableMappingSpec{}, moerr.NewInvalidInput(ctx, "iceberg table mapping requires ENGINE = ICEBERG")
	}
	values := make(map[string]string, len(param.Options))
	for _, opt := range param.Options {
		if opt == nil {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(string(opt.Key)))
		value := strings.TrimSpace(opt.Val)
		if key == "" {
			return TableMappingSpec{}, moerr.NewInvalidInput(ctx, "iceberg table mapping option key cannot be empty")
		}
		if value == "" {
			return TableMappingSpec{}, moerr.NewInvalidInput(ctx, "iceberg table mapping option value cannot be empty")
		}
		if _, ok := values[key]; ok {
			return TableMappingSpec{}, moerr.NewInvalidInputf(ctx, "duplicate iceberg table mapping option %s", key)
		}
		values[key] = value
	}

	catalogName := values[optionCatalog]
	namespace := values[optionNamespace]
	tableName := values[optionTable]
	if catalogName == "" || namespace == "" || tableName == "" {
		return TableMappingSpec{}, moerr.NewInvalidInput(ctx, "iceberg table mapping requires catalog, namespace, and table options")
	}

	defaultRef := firstNonEmpty(values[optionRef], values[optionDefaultRef], model.DefaultRefMain)
	readMode := firstNonEmpty(values[optionReadMode], model.ReadModeAppendOnly)
	writeMode := firstNonEmpty(values[optionWriteMode], model.WriteModeReadOnly)
	if readMode != model.ReadModeAppendOnly && readMode != model.ReadModeMergeOnRead {
		return TableMappingSpec{}, moerr.NewInvalidInputf(ctx, "unsupported iceberg read_mode %s", readMode)
	}
	if writeMode != model.WriteModeReadOnly &&
		writeMode != model.WriteModeAppendOnly &&
		writeMode != model.WriteModeMergeOnRead {
		return TableMappingSpec{}, moerr.NewInvalidInputf(ctx, "unsupported iceberg write_mode %s", writeMode)
	}

	return TableMappingSpec{
		CatalogName: catalogName,
		Mapping: model.TableMapping{
			Namespace:  namespace,
			TableName:  tableName,
			DefaultRef: defaultRef,
			ReadMode:   readMode,
			WriteMode:  writeMode,
		},
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
