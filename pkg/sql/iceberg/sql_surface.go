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
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
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

// IsBuiltinProcedure identifies an unqualified Iceberg builtin CALL.
func IsBuiltinProcedure(call *tree.CallStmt) bool {
	if call == nil || call.Name == nil || !call.Name.HasNoNameQualifier() {
		return false
	}
	name := strings.ToLower(strings.TrimSpace(tree.String(call.Name, dialect.MYSQL)))
	return strings.HasPrefix(name, "iceberg_")
}

// BuiltinProcedureArgs validates and extracts the common Iceberg CALL arguments.
// The caller is responsible for checking that call is a recognized builtin.
func BuiltinProcedureArgs(ctx context.Context, call *tree.CallStmt) (name, target, options string, err error) {
	name = strings.TrimSpace(tree.String(call.Name, dialect.MYSQL))
	if len(call.Args) < 1 || len(call.Args) > 2 {
		return "", "", "", moerr.NewInvalidInputf(ctx, "Iceberg builtin procedure %s requires target and optional options string arguments", name)
	}
	target, err = builtinProcedureStringArg(ctx, name, "target", call.Args[0])
	if err != nil {
		return "", "", "", err
	}
	if len(call.Args) == 2 {
		options, err = builtinProcedureStringArg(ctx, name, "options", call.Args[1])
		if err != nil {
			return "", "", "", err
		}
	}
	return name, target, options, nil
}

func builtinProcedureStringArg(ctx context.Context, procedure, argName string, expr tree.Expr) (string, error) {
	var out string
	switch value := expr.(type) {
	case *tree.StrVal:
		out = strings.TrimSpace(value.String())
	case *tree.NumVal:
		if value.Kind() == tree.Str {
			out = strings.TrimSpace(value.String())
		}
	}
	if out == "" {
		return "", moerr.NewInvalidInputf(ctx, "Iceberg builtin procedure %s requires %s as a string literal", procedure, argName)
	}
	return out, nil
}

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

	defaultRef := api.FirstNonBlankPreserveWhitespace(values[optionRef], values[optionDefaultRef], model.DefaultRefMain)
	readMode := api.FirstNonBlankPreserveWhitespace(values[optionReadMode], model.ReadModeAppendOnly)
	writeMode := api.FirstNonBlankPreserveWhitespace(values[optionWriteMode], model.WriteModeReadOnly)
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
