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
		"CreateTable":      {"database_defaults": 18},
		"DatabaseDefaults": {"collation_revision": 6},
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
