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
		"Query":            {"correlation": 19},
		"CreateDatabase":   {"defaults": 5},
		"AlterDatabase":    {"defaults": 3},
		"CreateTable":      {"database_defaults": 18, "ctas_execution_protocol_version": 19},
		"DatabaseDefaults": {"collation_revision": 6},
		"ColDef":           {"result_column_kind": 23},
	}
	file := decodePlanDescriptor(t, proto.FileDescriptor("plan.proto"))
	for _, message := range file.GetMessageType() {
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
