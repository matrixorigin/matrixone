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
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestCTASPopulationScalarAdmission(t *testing.T) {
	for _, tc := range []struct {
		name     string
		required int64
		failure  string
	}{
		{"unaffected", 0, ""},
		{"required", 122, ""},
		{"invalid_requirement", -1, "requires rebinding"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{DdlType: planpb.DataDefinition_CREATE_TABLE, Definition: &planpb.DataDefinition_CreateTable{CreateTable: &planpb.CreateTable{CreateAsSelectSql: "select 1", CtasExecutionProtocolVersion: tc.required}}}}}
			err := validateTemporalResultContracts(p)
			if tc.failure == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.failure)
			}
		})
	}
}
