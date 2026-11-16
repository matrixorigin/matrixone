// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	sqlkafka "github.com/matrixorigin/matrixone/pkg/sql/kafka"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// Every fixture here is json.Marshal of the tree.ExternParam the create path
// durably stores: the INFILE{'filepath'=...} option-only dialect of issue
// #31920, the normalized INFILE string dialect, the stage/S3 shapes, and the
// writable option shape. No hand-written JSON and no hand-set top-level
// Filepath for the reported shape.

// newTruncateScopeForStub builds a TRUNCATE plan scope whose planned table id
// matches the live stub relation, so Scope.TruncateTable reaches the live
// relation type decision (the prepared/cached-plan execution path).
func newTruncateScopeForStub(liveTableID uint64) *Scope {
	return &Scope{Plan: &plan.Plan{
		Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
			DdlType: plan.DataDefinition_TRUNCATE_TABLE,
			Definition: &plan.DataDefinition_TruncateTable{
				TruncateTable: &plan.TruncateTable{
					Database: "test",
					Table:    "t",
					TableId:  liveTableID,
				},
			},
		}},
	}}
}

// newExternalStubRelation is a live SystemExternalRel relation carrying the
// durable rel_createsql of the underlying mapping.
func newExternalStubRelation(relName string, tableID uint64, createSQL string, featureFlag uint64) *stubRelation {
	return &stubRelation{
		name:    relName,
		tableID: tableID,
		tableDef: &plan.TableDef{
			Name:        relName,
			TableType:   catalog.SystemExternalRel,
			Createsql:   createSQL,
			FeatureFlag: featureFlag,
		},
	}
}

// newTruncateCompile mirrors the prepared-plan execution harness: a stubbed
// engine/database where the planned name resolves to the live relation.
func newTruncateCompile(t *testing.T, rel *stubRelation) *Compile {
	t.Helper()
	eng := newStubEngine()
	db := newStubDatabase("test")
	db.rels["t"] = rel
	eng.dbs["test"] = db

	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 0)
	c := NewCompile("test", "test", "truncate table t", "", "", eng, proc, nil, false, nil, time.Now())
	return c
}

// durableTerminalShapes returns the durable read-only file-mapping shapes the
// compiled terminal guard must reject, mirroring the option-only dialect the
// create path actually persists for each format.
func durableTerminalShapes(t *testing.T) []struct {
	name      string
	createSQL string
} {
	t.Helper()
	shapes := make([]struct {
		name      string
		createSQL string
	}, 0, 6)
	for _, format := range []string{"csv", "jsonline", "parquet"} {
		shapes = append(shapes, struct {
			name      string
			createSQL string
		}{
			name: "infile-option/" + format,
			createSQL: marshalTerminalExternParam(t, &tree.ExternParam{ExParamConst: tree.ExParamConst{
				Option: []string{"filepath", "/data/source.flat", "format", format},
			}}),
		})
	}
	shapes = append(shapes, struct {
		name      string
		createSQL string
	}{
		name: "infile-string/csv",
		createSQL: marshalTerminalExternParam(t, &tree.ExternParam{ExParamConst: tree.ExParamConst{
			Filepath:     "/data/source.flat",
			CompressType: tree.AUTO,
			Format:       tree.CSV,
		}}),
	})
	shapes = append(shapes, struct {
		name      string
		createSQL string
	}{
		name: "stage/url",
		createSQL: marshalTerminalExternParam(t, &tree.ExternParam{ExParamConst: tree.ExParamConst{
			StageName: "t_stage",
		}}),
	})
	shapes = append(shapes, struct {
		name      string
		createSQL string
	}{
		name: "s3/url",
		createSQL: marshalTerminalExternParam(t, &tree.ExternParam{ExParamConst: tree.ExParamConst{
			ScanType: tree.S3,
			Option: []string{
				"filepath", "bucket-key.flat",
				"endpoint", "http://127.0.0.1",
				"region", "r",
				"bucket", "b",
				"access_key_id", "k",
				"secret_access_key", "s",
				"format", "csv",
			},
		}}),
	})
	return shapes
}

func marshalTerminalExternParam(t *testing.T, param *tree.ExternParam) string {
	t.Helper()
	raw, err := json.Marshal(param)
	require.NoError(t, err)
	return string(raw)
}

// TestTruncateTerminalGuardRejectsReadOnlyFileExternalAcrossDurableShapes
// closes the terminal no-op of issue #31920 at the compiled-scope level for
// every legal durable read-only file shape: a TRUNCATE scope resolving a live
// read-only file external mapping must fail with the explicit not-supported
// decision, independent of the planner admission.
func TestTruncateTerminalGuardRejectsReadOnlyFileExternalAcrossDurableShapes(t *testing.T) {
	for _, shape := range durableTerminalShapes(t) {
		t.Run(shape.name, func(t *testing.T) {
			rel := newExternalStubRelation("t", 7, shape.createSQL, 0)
			c := newTruncateCompile(t, rel)
			defer c.Release()

			err := newTruncateScopeForStub(7).TruncateTable(c)
			require.Error(t, err,
				"live read-only %s file mapping must not be silently acknowledged", shape.name)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			require.Contains(t, err.Error(), "truncate read-only external table 't'")
			require.Contains(t, err.Error(), "its files are not managed by the table")
		})
	}
}

// TestTruncateTerminalGuardRejectsWritableFileExternalOptionShape keeps the
// writable-file terminal decision typed as well in the option dialect: a
// cached or synthesized truncate scope must not silently acknowledge a
// writable mapping; its files in the stage are not managed by the table.
func TestTruncateTerminalGuardRejectsWritableFileExternalOptionShape(t *testing.T) {
	rel := newExternalStubRelation("t", 7, marshalTerminalExternParam(t, &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Option: []string{
				"filepath", "/data/source.flat",
				"format", "csv",
				"write_file_pattern", "stage://bkt/out/%U.csv",
			},
		},
	}), 0)
	c := newTruncateCompile(t, rel)
	defer c.Release()

	err := newTruncateScopeForStub(7).TruncateTable(c)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
	require.Contains(t, err.Error(), "truncate writable external table 't'; its files in the stage are not managed by the table")
}

// TestTruncateTerminalGuardKeepsEnvelopeMappingNoOp pins the neighboring
// control: Kafka envelope mappings keep the existing generic external no-op
// contract at the terminal, unchanged by the file fix.
func TestTruncateTerminalGuardKeepsEnvelopeMappingNoOp(t *testing.T) {
	for _, env := range []struct {
		name        string
		createSQL   string
		featureFlag uint64
	}{
		{
			name: "kafka envelope",
			createSQL: sqlkafka.BuildCreateSQLEnvelope(sqlkafka.Config{
				Brokers: "kafka.invalid:9092",
				Topic:   "t",
				Format:  "csv",
			}),
			featureFlag: features.KafkaExternal,
		},
		{
			name:        "generic non-file mapping URL",
			createSQL:   "https://example.invalid/stream",
			featureFlag: 0,
		},
	} {
		t.Run(env.name, func(t *testing.T) {
			rel := newExternalStubRelation("t", 7, env.createSQL, env.featureFlag)
			c := newTruncateCompile(t, rel)
			defer c.Release()

			err := newTruncateScopeForStub(7).TruncateTable(c)
			require.NoError(t, err)
		})
	}
}
