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
