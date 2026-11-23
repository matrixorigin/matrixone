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

package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexMaintenanceOptionsIsolationAndValidation(t *testing.T) {
	spec := &IndexMaintenanceSpec{Function: "build", AccountID: 0, Database: "db", DatabaseID: 1, BaseName: "base", BaseID: 2,
		Targets: []IndexMaintenanceTarget{{Role: "meta", Name: "hidden", ID: 3}, {Role: "store", Name: "other", ID: 4}}}
	require.NoError(t, spec.Validate())
	base := Options{}
	generated := base.WithIndexMaintenanceSpec(spec)
	restricted := base.WithIndexRelationPolicy(&IndexRelationPolicy{Spec: spec})
	spec.Targets[0].Name = "mutated"
	require.Equal(t, "hidden", generated.IndexMaintenanceSpec().Targets[0].Name)
	require.Equal(t, "hidden", restricted.IndexRelationPolicy().Spec.Targets[0].Name)
	require.Nil(t, base.IndexMaintenanceSpec())
	require.Nil(t, base.IndexRelationPolicy())
	read := generated.IndexMaintenanceSpec()
	read.Targets[0].ID = 0
	require.Error(t, read.Validate())
	require.NoError(t, generated.IndexMaintenanceSpec().Validate())
	read = generated.IndexMaintenanceSpec()
	read.Targets[1].Role = read.Targets[0].Role
	require.Error(t, read.Validate())
	read = generated.IndexMaintenanceSpec()
	read.Targets[1].ID = read.Targets[0].ID
	require.Error(t, read.Validate())
	read = generated.IndexMaintenanceSpec()
	read.Targets[1].Name = "HIDDEN"
	require.Error(t, read.Validate())
	require.Equal(t, "alice", StatementOption{}.WithSessionUser("alice").SessionUser())
}
