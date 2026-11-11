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

package plan

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestPlanDescriptorMergedContracts(t *testing.T) {
	// The compressed descriptor must agree with both generated Go contracts.
	// Resolving a descriptor-only conflict by choosing either side loses one.
	want := map[string]map[string]int32{
		"Query":                    {"correlation": 19, "execution_dependent_access": 22, "select_lock_references": 21, "local_runtime_filter_domains": 23, "enum_set_result_types": 24},
		"Type":                     {"numeric_attributes": 14},
		"Expr":                     {"candidate_filter": 27},
		"CreateDatabase":           {"defaults": 5},
		"AlterDatabase":            {"defaults": 3},
		"CreateTable":              {"database_defaults": 18, "ctas_execution_protocol_version": 19},
		"DatabaseDefaults":         {"collation_revision": 6},
		"ColDef":                   {"result_column_kind": 23, "enum_set_result_type": 25},
		"UpdateCtx":                {"partition_index_ctx": 20},
		"PreInsertCtx":             {"preserve_input": 18},
		"PartitionIndexCtx":        {"parent_ref": 1, "parent_table": 2, "partition_col": 3},
		"LocalRuntimeFilterTarget": {"scan_node_id": 1, "column_name": 2, "probe_ndv": 3},
		"LocalRuntimeFilterDomain": {"source_scan_node_id": 1, "source_column_name": 2, "key_type": 3, "targets": 4},
	}
	file := decodePlanDescriptor(t, proto.FileDescriptor("plan.proto"))
	for _, message := range file.GetMessageType() {
		if message.GetName() == "Expr" || message.GetName() == "FoldVal" {
			for _, field := range message.GetField() {
				require.NotEqual(t, "temporal_candidate_guard", field.GetName())
			}
		}
		fields, ok := want[message.GetName()]
		if !ok {
			continue
		}
		for _, field := range message.GetField() {
			if number, exists := fields[field.GetName()]; exists {
				require.Equal(t, number, field.GetNumber(), "%s.%s", message.GetName(), field.GetName())
				delete(fields, field.GetName())
			}
		}
		require.Empty(t, fields, "missing fields in %s", message.GetName())
		delete(want, message.GetName())
	}
	require.Empty(t, want, "missing merged message descriptors")
}

func TestPlanConflictFieldsRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		original proto.Message
		decoded  proto.Message
	}{
		{"numeric attributes", &Type{NumericAttributes: 3}, &Type{}},
		{"partition index context", &UpdateCtx{PartitionIndexCtx: &PartitionIndexCtx{}}, &UpdateCtx{}},
		{"preserve input", &PreInsertCtx{PreserveInput: true}, &PreInsertCtx{}},
		{"local runtime filter", &Query{LocalRuntimeFilterDomains: []*LocalRuntimeFilterDomain{{SourceScanNodeId: 1, SourceColumnName: "source", KeyType: 2, Targets: []*LocalRuntimeFilterTarget{{ScanNodeId: 3, ColumnName: "target", ProbeNdv: 4}}}}}, &Query{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := proto.Marshal(test.original)
			require.NoError(t, err)
			require.NoError(t, proto.Unmarshal(encoded, test.decoded))
			require.True(t, proto.Equal(test.original, test.decoded))
		})
	}
}

func TestMergedCTASResultColumnKindRoundTrip(t *testing.T) {
	for _, kind := range []ResultColumnKind{
		ResultColumnKind_RESULT_COLUMN_KIND_UNKNOWN,
		ResultColumnKind_RESULT_COLUMN_KIND_STORED,
		ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED,
		ResultColumnKind(99), // Preserve a future producer's enum value.
	} {
		t.Run(kind.String(), func(t *testing.T) {
			original := &CreateTable{
				CtasExecutionProtocolVersion: 83,
				TableDef: &TableDef{Cols: []*ColDef{{
					Name: "flag", ResultColumnKind: kind,
				}}},
			}
			encoded, err := original.Marshal()
			require.NoError(t, err)
			require.Len(t, encoded, original.ProtoSize())
			var decoded CreateTable
			require.NoError(t, decoded.Unmarshal(encoded))
			require.Equal(t, original.CtasExecutionProtocolVersion, decoded.CtasExecutionProtocolVersion)
			require.Len(t, decoded.GetTableDef().GetCols(), 1)
			require.Equal(t, kind, decoded.TableDef.Cols[0].ResultColumnKind)
		})
	}
}

func TestMergedNumericAndTemporalFieldsRoundTrip(t *testing.T) {
	// Physical OID 32 is decimal64; numeric attribute mask 3 is normalized ZEROFILL.
	original := &Query{SelectLockReferences: []*SelectLockReference{{ObjectRef: &ObjectRef{Obj: 123, ObjName: "locked"}}}, ExecutionDependentAccess: true, Nodes: []*Node{{IndexScanInfo: IndexScanInfo{IsIndexScan: true, RequiresCapturedExecution: true}, FilterList: []*Expr{{
		Typ:             Type{Id: 32, Width: 10, Scale: 2, NumericAttributes: 3},
		CandidateFilter: true,
		Expr:            &Expr_Fold{Fold: &FoldVal{IsConst: true, Data: []byte{1}}},
	}}}}}
	encoded, err := original.Marshal()
	require.NoError(t, err)
	require.Len(t, encoded, original.ProtoSize())
	var decoded Query
	require.NoError(t, decoded.Unmarshal(encoded))
	require.Equal(t, original, &decoded)
}

func TestMergedPlanDescriptorMessageIndices(t *testing.T) {
	tests := []struct {
		name    string
		message interface{ Descriptor() ([]byte, []int) }
	}{
		{"PartitionIndexCtx", &PartitionIndexCtx{}},
		{"LocalRuntimeFilterTarget", &LocalRuntimeFilterTarget{}},
		{"LocalRuntimeFilterDomain", &LocalRuntimeFilterDomain{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, indices := test.message.Descriptor()
			require.Len(t, indices, 1)
			file := decodePlanDescriptor(t, data)
			require.GreaterOrEqual(t, indices[0], 0)
			require.Less(t, indices[0], len(file.GetMessageType()))
			require.Equal(t, test.name, file.GetMessageType()[indices[0]].GetName())
		})
	}
}
