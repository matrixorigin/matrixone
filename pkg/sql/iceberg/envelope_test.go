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
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestParseCreateSQLEnvelope(t *testing.T) {
	header := BuildCreateSQLEnvelope(model.TableMapping{
		CatalogID:  9,
		Namespace:  "prod.db",
		TableName:  "orders",
		DefaultRef: "main",
		ReadMode:   model.ReadModeAppendOnly,
		WriteMode:  model.WriteModeReadOnly,
	}, "ksa rest")
	env, found, err := ParseCreateSQLEnvelope(context.Background(), header+" create table t(a int)")
	if err != nil {
		t.Fatalf("parse envelope: %v", err)
	}
	if !found || env.Kind != CreateSQLKindIceberg || env.CatalogID != 0 || env.Catalog != "ksa rest" || env.Namespace != "prod.db" || env.Table != "orders" || env.ReadMode != model.ReadModeAppendOnly || env.WriteMode != model.WriteModeReadOnly {
		t.Fatalf("unexpected envelope: %+v found=%v", env, found)
	}
}

func TestParseCreateSQLEnvelopeCompatCatalogID(t *testing.T) {
	header := "/* MO_ICEBERG: version=1; kind=iceberg_table; catalog_id=9; catalog=ksa; namespace=prod; table=orders; default_ref=main */"
	env, found, err := ParseCreateSQLEnvelope(context.Background(), header)
	if err != nil {
		t.Fatalf("parse envelope: %v", err)
	}
	if !found || env.CatalogID != 9 {
		t.Fatalf("expected optional catalog_id compatibility, got %+v found=%v", env, found)
	}
}

func TestParseCreateSQLLegacyExternal(t *testing.T) {
	env, found, err := ParseCreateSQLEnvelope(context.Background(), "create external table t(...)")
	if err != nil {
		t.Fatalf("legacy parse should not error: %v", err)
	}
	if found || env.Kind != CreateSQLKindLegacy {
		t.Fatalf("legacy SQL should remain compatible: %+v found=%v", env, found)
	}
}

func TestParseCreateSQLEnvelopeRejectsMalformedMetadata(t *testing.T) {
	for _, tc := range []struct {
		name           string
		createSQL      string
		wantErrMessage string
	}{
		{
			name:           "unclosed",
			createSQL:      "/* MO_ICEBERG: version=1; kind=iceberg_table",
			wantErrMessage: "envelope is not closed",
		},
		{
			name:           "field without assignment",
			createSQL:      "/* MO_ICEBERG: version=1; kind */",
			wantErrMessage: "field must be key=value",
		},
		{
			name:           "bad escape",
			createSQL:      "/* MO_ICEBERG: version=1; catalog=%zz */",
			wantErrMessage: "field is not url-escaped",
		},
		{
			name:           "bad version",
			createSQL:      "/* MO_ICEBERG: version=2; kind=iceberg_table; catalog=ksa; namespace=prod; table=orders */",
			wantErrMessage: "version must be 1",
		},
		{
			name:           "bad catalog id",
			createSQL:      "/* MO_ICEBERG: version=1; kind=iceberg_table; catalog_id=0; catalog=ksa; namespace=prod; table=orders */",
			wantErrMessage: "catalog_id must be positive",
		},
		{
			name:           "missing required fields",
			createSQL:      "/* MO_ICEBERG: version=1; kind=iceberg_table; catalog=ksa; namespace=prod */",
			wantErrMessage: "missing required fields",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, found, err := ParseCreateSQLEnvelope(context.Background(), tc.createSQL)
			if !found {
				t.Fatalf("malformed envelope should still be detected")
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErrMessage) {
				t.Fatalf("expected %q error, got %v", tc.wantErrMessage, err)
			}
		})
	}
}

func TestParseCreateSQLLegacyExternParamJSON(t *testing.T) {
	legacy, err := json.Marshal(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE,
			Filepath: "/data/orders/*.parquet",
			Format:   tree.PARQUET,
			Option:   []string{"format", "parquet"},
		},
	})
	if err != nil {
		t.Fatalf("marshal legacy extern param: %v", err)
	}
	env, found, err := ParseCreateSQLEnvelope(context.Background(), string(legacy))
	if err != nil {
		t.Fatalf("legacy extern param JSON should not error: %v", err)
	}
	if found || env.Kind != CreateSQLKindLegacy {
		t.Fatalf("legacy extern param JSON should remain compatible: %+v found=%v", env, found)
	}
}
