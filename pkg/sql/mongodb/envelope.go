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

package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	CreateSQLEnvelopePrefix = "MO_MONGODB:"
	CreateSQLKindMongoDB    = "mongodb_table"
)

type CreateSQLEnvelope struct {
	Version        int
	Kind           string
	Connection     string
	Database       string
	Collection     string
	SchemaMode     string
	ConversionMode string
	SplitKey       string
	MaxParallelism int32
	Columns        []ColumnMapping
}

func BuildCreateSQLEnvelope(mapping TableMapping) string {
	columns, _ := json.Marshal(mapping.Columns)
	return fmt.Sprintf(
		"/* %s version=2; kind=%s; connection=%s; database=%s; collection=%s; schema_mode=%s; conversion_mode=%s; split_key=%s; max_parallelism=%d; columns=%s */",
		CreateSQLEnvelopePrefix,
		CreateSQLKindMongoDB,
		url.QueryEscape(mapping.Connection),
		url.QueryEscape(mapping.Database),
		url.QueryEscape(mapping.Collection),
		url.QueryEscape(defaultString(mapping.SchemaMode, SchemaExplicit)),
		url.QueryEscape(defaultString(mapping.Conversion, ConversionStrict)),
		url.QueryEscape(mapping.SplitKey),
		defaultParallelism(mapping.MaxParallelism),
		url.QueryEscape(string(columns)),
	)
}

func ParseCreateSQLEnvelope(ctx context.Context, createSQL string) (CreateSQLEnvelope, bool, error) {
	// rel_createsql for a generic external table is user-controlled JSON. Only
	// recognize the planner-owned leading comment envelope; searching the whole
	// string would let a filepath inject this marker and cross the MongoDB
	// account-admin boundary.
	createSQL = strings.TrimSpace(createSQL)
	prefix := "/* " + CreateSQLEnvelopePrefix
	if !strings.HasPrefix(createSQL, prefix) {
		return CreateSQLEnvelope{}, false, nil
	}
	start := len(prefix)
	end := strings.Index(createSQL[start:], "*/")
	if end < 0 {
		return CreateSQLEnvelope{}, true, moerr.NewInvalidInput(ctx, "MongoDB rel_createsql envelope is not closed")
	}
	fields := make(map[string]string)
	for _, part := range strings.Split(createSQL[start:start+end], ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return CreateSQLEnvelope{}, true, moerr.NewInvalidInput(ctx, "MongoDB rel_createsql envelope field must be key=value")
		}
		decoded, err := url.QueryUnescape(strings.TrimSpace(value))
		if err != nil {
			return CreateSQLEnvelope{}, true, moerr.NewInvalidInput(ctx, "MongoDB rel_createsql envelope field is not url-escaped")
		}
		fields[strings.ToLower(strings.TrimSpace(key))] = decoded
	}
	version, err := strconv.Atoi(fields["version"])
	if err != nil || version < 1 || version > 2 {
		return CreateSQLEnvelope{}, true, moerr.NewInvalidInput(ctx, "MongoDB rel_createsql envelope version must be 1 or 2")
	}
	kind := fields["kind"]
	if version == 1 && kind == "" {
		// Version 1 predates the explicit kind field. Its anchored envelope is
		// still unambiguous and remains readable for tables created before the
		// discriminator was added.
		kind = CreateSQLKindMongoDB
	}
	if kind != CreateSQLKindMongoDB {
		return CreateSQLEnvelope{}, true, moerr.NewInvalidInput(ctx, "MongoDB rel_createsql envelope kind must be mongodb_table")
	}
	parallelism, err := strconv.ParseInt(fields["max_parallelism"], 10, 32)
	if err != nil || parallelism != 1 {
		return CreateSQLEnvelope{}, true, moerr.NewInvalidInput(ctx, "MongoDB MVP max_parallelism must be 1")
	}
	var columns []ColumnMapping
	if err := json.Unmarshal([]byte(fields["columns"]), &columns); err != nil || len(columns) == 0 {
		return CreateSQLEnvelope{}, true, moerr.NewInvalidInput(ctx, "MongoDB rel_createsql envelope requires explicit columns")
	}
	env := CreateSQLEnvelope{
		Version:        version,
		Kind:           kind,
		Connection:     fields["connection"],
		Database:       fields["database"],
		Collection:     fields["collection"],
		SchemaMode:     defaultString(fields["schema_mode"], SchemaExplicit),
		ConversionMode: defaultString(fields["conversion_mode"], ConversionStrict),
		SplitKey:       fields["split_key"],
		MaxParallelism: int32(parallelism),
		Columns:        columns,
	}
	if env.Connection == "" || env.Database == "" || env.Collection == "" {
		return CreateSQLEnvelope{}, true, moerr.NewInvalidInput(ctx, "MongoDB rel_createsql envelope missing connection, database, or collection")
	}
	return env, true, nil
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func defaultParallelism(value int32) int32 {
	if value == 0 {
		return 1
	}
	return value
}
