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
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

const (
	CreateSQLEnvelopePrefix = "MO_ICEBERG:"
	CreateSQLKindIceberg    = "iceberg_table"
	CreateSQLKindLegacy     = "legacy_external"
)

type CreateSQLEnvelope struct {
	Version    int
	Kind       string
	CatalogID  uint64
	Catalog    string
	Namespace  string
	Table      string
	DefaultRef string
	ReadMode   string
	WriteMode  string
}

func BuildCreateSQLEnvelope(mapping model.TableMapping, catalogName string) string {
	ref := defaultString(mapping.DefaultRef, model.DefaultRefMain)
	readMode := defaultString(mapping.ReadMode, model.ReadModeAppendOnly)
	writeMode := defaultString(mapping.WriteMode, model.WriteModeReadOnly)
	return fmt.Sprintf(
		"/* %s version=1; kind=%s; catalog=%s; namespace=%s; table=%s; default_ref=%s; read_mode=%s; write_mode=%s */",
		CreateSQLEnvelopePrefix,
		CreateSQLKindIceberg,
		url.QueryEscape(catalogName),
		url.QueryEscape(mapping.Namespace),
		url.QueryEscape(mapping.TableName),
		url.QueryEscape(ref),
		url.QueryEscape(readMode),
		url.QueryEscape(writeMode),
	)
}

func ParseCreateSQLEnvelope(ctx context.Context, createSQL string) (CreateSQLEnvelope, bool, error) {
	idx := strings.Index(createSQL, CreateSQLEnvelopePrefix)
	if idx < 0 {
		return CreateSQLEnvelope{Kind: CreateSQLKindLegacy}, false, nil
	}
	start := idx + len(CreateSQLEnvelopePrefix)
	end := strings.Index(createSQL[start:], "*/")
	if end < 0 {
		return CreateSQLEnvelope{}, true, moerr.NewInvalidInput(ctx, "iceberg rel_createsql envelope is not closed")
	}
	fields, err := parseEnvelopeFields(ctx, createSQL[start:start+end])
	if err != nil {
		return CreateSQLEnvelope{}, true, err
	}
	version, err := strconv.Atoi(fields["version"])
	if err != nil || version != 1 {
		return CreateSQLEnvelope{}, true, moerr.NewInvalidInput(ctx, "iceberg rel_createsql envelope version must be 1")
	}
	var catalogID uint64
	if rawCatalogID := fields["catalog_id"]; rawCatalogID != "" {
		var err error
		catalogID, err = strconv.ParseUint(rawCatalogID, 10, 64)
		if err != nil || catalogID == 0 {
			return CreateSQLEnvelope{}, true, moerr.NewInvalidInput(ctx, "iceberg rel_createsql envelope catalog_id must be positive when present")
		}
	}
	env := CreateSQLEnvelope{
		Version:    version,
		Kind:       fields["kind"],
		CatalogID:  catalogID,
		Catalog:    fields["catalog"],
		Namespace:  fields["namespace"],
		Table:      fields["table"],
		DefaultRef: defaultString(fields["default_ref"], model.DefaultRefMain),
		ReadMode:   defaultString(fields["read_mode"], model.ReadModeAppendOnly),
		WriteMode:  defaultString(fields["write_mode"], model.WriteModeReadOnly),
	}
	if env.Kind != CreateSQLKindIceberg || env.Catalog == "" || env.Namespace == "" || env.Table == "" || env.DefaultRef == "" {
		return CreateSQLEnvelope{}, true, moerr.NewInvalidInput(ctx, "iceberg rel_createsql envelope missing required fields")
	}
	return env, true, nil
}

func parseEnvelopeFields(ctx context.Context, body string) (map[string]string, error) {
	fields := make(map[string]string)
	for _, part := range strings.Split(body, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return nil, moerr.NewInvalidInput(ctx, "iceberg rel_createsql envelope field must be key=value")
		}
		decoded, err := url.QueryUnescape(strings.TrimSpace(value))
		if err != nil {
			return nil, moerr.NewInvalidInput(ctx, "iceberg rel_createsql envelope field is not url-escaped")
		}
		fields[strings.ToLower(strings.TrimSpace(key))] = decoded
	}
	return fields, nil
}
