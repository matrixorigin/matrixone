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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqldatastream "github.com/matrixorigin/matrixone/pkg/sql/datastream"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/stretchr/testify/require"
)

func TestIsDataStreamTableDef(t *testing.T) {
	ctx := context.Background()
	cfg := sqldatastream.Config{Server: "h", Port: 4444, Table: "src", Recheck: true}
	envelope := sqldatastream.BuildCreateSQLEnvelope(cfg)

	// nil / non-external
	_, found, err := IsDataStreamTableDef(ctx, nil)
	require.NoError(t, err)
	require.False(t, found)
	_, found, err = IsDataStreamTableDef(ctx, &plan.TableDef{TableType: catalog.SystemOrdinaryRel, Createsql: envelope})
	require.NoError(t, err)
	require.False(t, found)

	// envelope + feature bit
	def := &plan.TableDef{
		TableType:   catalog.SystemExternalRel,
		Createsql:   envelope,
		FeatureFlag: features.DataStreamExternal,
	}
	got, found, err := IsDataStreamTableDef(ctx, def)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, cfg, got)

	// envelope forged without the feature bit is rejected
	def.FeatureFlag = 0
	_, _, err = IsDataStreamTableDef(ctx, def)
	require.Error(t, err)

	// feature bit without envelope is corrupt
	def.FeatureFlag = features.DataStreamExternal
	def.Createsql = `{"ScanType":0}`
	_, _, err = IsDataStreamTableDef(ctx, def)
	require.Error(t, err)

	// generic external table JSON is simply not a datastream table
	def.FeatureFlag = 0
	_, found, err = IsDataStreamTableDef(ctx, def)
	require.NoError(t, err)
	require.False(t, found)
}

func TestFormatDataStreamTableOptionsForShowCreate(t *testing.T) {
	cfg := sqldatastream.Config{Server: "10.1.2.3", Port: 4444, Table: "src_t", Recheck: false}
	got := formatDataStreamTableOptionsForShowCreate(cfg, "")
	require.Equal(t,
		` ENGINE = DATASTREAM WITH ("server" = '10.1.2.3', "port" = '4444', "table" = 'src_t', "recheck" = 'false')`,
		got)
}
