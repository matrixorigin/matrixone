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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	sqlkafka "github.com/matrixorigin/matrixone/pkg/sql/kafka"
	"github.com/stretchr/testify/require"
)

func TestIsKafkaTableDef(t *testing.T) {
	ctx := context.Background()
	env := sqlkafka.BuildCreateSQLEnvelope(sqlkafka.Config{
		Brokers: "h:9092", Topic: "t", Group: "g", Format: sqlkafka.FormatCSV, Separator: ",",
	})

	// envelope + feature bit agree
	def := &TableDef{TableType: catalog.SystemExternalRel, Createsql: env,
		FeatureFlag: features.KafkaExternal}
	cfg, found, err := IsKafkaTableDef(ctx, def)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "t", cfg.Topic)

	// envelope without the bit is an error (forgeable rel_createsql)
	def = &TableDef{TableType: catalog.SystemExternalRel, Createsql: env}
	_, _, err = IsKafkaTableDef(ctx, def)
	require.ErrorContains(t, err, "feature flag")

	// bit without the envelope is an error
	def = &TableDef{TableType: catalog.SystemExternalRel, Createsql: "{}",
		FeatureFlag: features.KafkaExternal}
	_, _, err = IsKafkaTableDef(ctx, def)
	require.ErrorContains(t, err, "missing its catalog envelope")

	// plain external tables and nil are simply not kafka
	_, found, err = IsKafkaTableDef(ctx, &TableDef{TableType: catalog.SystemExternalRel, Createsql: "{}"})
	require.NoError(t, err)
	require.False(t, found)
	_, found, err = IsKafkaTableDef(ctx, nil)
	require.NoError(t, err)
	require.False(t, found)
}

func TestFormatKafkaTableOptionsForShowCreate(t *testing.T) {
	got := formatKafkaTableOptionsForShowCreate(sqlkafka.Config{
		Brokers: "h1:9092,h2:9092", Topic: "t'1", Partition: 3, Autocommit: true,
		Group: "g1", Format: sqlkafka.FormatCSV, Separator: "|",
	}, "")
	require.Equal(t, ` ENGINE = KAFKA WITH ("brokers" = 'h1:9092,h2:9092', "topic" = 't''1', "partition" = '3', "autocommit" = 'true', "group" = 'g1', "format" = 'csv', "separator" = '|')`, got)

	// jsonl omits the separator
	got = formatKafkaTableOptionsForShowCreate(sqlkafka.Config{
		Brokers: "h:9092", Topic: "t", Group: "g", Format: sqlkafka.FormatJSONL,
	}, "")
	require.NotContains(t, got, "separator")
	require.Contains(t, got, `"format" = 'jsonl'`)
}

func TestBuildCreateKafkaTable(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		`create external table k1 (a int, b varchar(10)) engine = kafka with ('brokers'='h:9092', 'topic'='t1')`,
		`create external table k2 (a int) engine = kafka with ('brokers'='h:9092', 'topic'='t', 'partition'='2', 'autocommit'='true', 'group'='g', 'format'='jsonl')`,
	}
	runTestShouldPass(mock, t, sqls, false, false)
	errSqls := []string{
		// missing required options
		`create external table k3 (a int) engine = kafka`,
		`create external table k4 (a int) engine = kafka with ('brokers'='h:9092')`,
		// unknown / invalid options
		`create external table k5 (a int) engine = kafka with ('brokers'='h:9092','topic'='t','bogus'='v')`,
		`create external table k6 (a int) engine = kafka with ('brokers'='h:9092','topic'='t','format'='xml')`,
		`create external table k7 (a int) engine = kafka with ('brokers'='h:9092','topic'='t','format'='jsonl','separator'='|')`,
		// reserved synthetic column names
		`create external table k8 (a int, __mo_message_id bigint) engine = kafka with ('brokers'='h:9092','topic'='t')`,
	}
	runTestShouldError(mock, t, errSqls)
}

// TestSelectAndAlterKafkaTable injects a kafka external table into the mock
// catalog and drives SELECT-side recognition (KAFKA_TB dispatch + synthetic
// columns) and the ALTER guard.
func TestSelectAndAlterKafkaTable(t *testing.T) {
	mock := NewMockOptimizer(false)
	mcc := mock.CurrentContext().(*MockCompilerContext)
	env := sqlkafka.BuildCreateSQLEnvelope(sqlkafka.Config{
		Brokers: "h:9092", Topic: "t", Group: "g", Format: sqlkafka.FormatCSV, Separator: ",",
	})
	mcc.tables["kafka_t"] = &TableDef{
		TableType:   catalog.SystemExternalRel,
		TblId:       990101,
		Name:        "kafka_t",
		Createsql:   env,
		FeatureFlag: features.KafkaExternal,
		Cols: []*plan.ColDef{
			{Name: "a", ColId: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", ColId: 2, Typ: plan.Type{Id: int32(types.T_varchar), Width: 64}},
		},
	}
	mcc.objects["kafka_t"] = &ObjectRef{SchemaName: "tpch", ObjName: "kafka_t", Obj: 990101}

	// SELECT * : KAFKA_TB extern scan, synthetic columns appended but hidden
	p, err := runOneStmt(mock, t, `select * from kafka_t`)
	require.NoError(t, err)
	q := p.GetQuery()
	var scan *plan.Node
	for _, n := range q.Nodes {
		if n.NodeType == plan.Node_EXTERNAL_SCAN {
			scan = n
		}
	}
	require.NotNil(t, scan)
	require.Equal(t, int32(plan.ExternType_KAFKA_TB), scan.ExternScan.Type)
	require.NotNil(t, scan.ExternScan.KafkaScan)
	require.Equal(t, "t", scan.ExternScan.KafkaScan.Topic)
	// projection of the root must NOT include synthetic columns
	root := q.Nodes[q.Steps[len(q.Steps)-1]]
	require.Len(t, root.ProjectList, 2, "SELECT * hides the synthetic columns")

	// the synthetic columns are directly selectable and usable in WHERE
	_, err = runOneStmt(mock, t,
		`select __mo_message_id, __mo_message_key, a from kafka_t where __mo_read_start_id = 100 and __mo_read_size = 10`)
	require.NoError(t, err)

	// ALTER is cleanly rejected
	runTestShouldError(mock, t, []string{`alter table kafka_t add column c int`})

	// reserved names rejected on ordinary CREATE / ALTER ADD
	runTestShouldError(mock, t, []string{
		`create table t_res (a int, __mo_message_id bigint)`,
		`create table t_res2 (a int, __mo_read_start_id bigint)`,
		`alter table nation add column __mo_message_ts timestamp`,
	})
}

// TestPreexistingKafkaNameColumnsStayVisible: a REAL column that happens to
// use one of the kafka synthetic names in a pre-existing ordinary table keeps
// working (ColId scoping), mirroring the __mo_query compatibility rule.
func TestPreexistingKafkaNameColumnsStayVisible(t *testing.T) {
	mock := NewMockOptimizer(false)
	mcc := mock.CurrentContext().(*MockCompilerContext)
	mcc.tables["legacy_k"] = &TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     990102,
		Name:      "legacy_k",
		Cols: []*plan.ColDef{
			{Name: "a", ColId: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: catalog.KafkaMessageID, ColId: 2, // REAL column, ordinary ColId
				Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	mcc.objects["legacy_k"] = &ObjectRef{SchemaName: "tpch", ObjName: "legacy_k", Obj: 990102}

	p, err := runOneStmt(mock, t, `select * from legacy_k`)
	require.NoError(t, err)
	q := p.GetQuery()
	root := q.Nodes[q.Steps[len(q.Steps)-1]]
	require.Len(t, root.ProjectList, 2, "a real same-named column stays visible in SELECT *")
	_, err = runOneStmt(mock, t, `select __mo_message_id from legacy_k`)
	require.NoError(t, err)
}
