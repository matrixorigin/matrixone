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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func encodeIndexMaintenanceSpec(spec *executor.IndexMaintenanceSpec) (*pipeline.IndexMaintenanceSpec, error) {
	if spec == nil {
		return nil, nil
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	wire := &pipeline.IndexMaintenanceSpec{
		Function: spec.Function, AccountId: spec.AccountID, Database: spec.Database,
		DatabaseId: spec.DatabaseID, BaseName: spec.BaseName, BaseId: spec.BaseID,
		Targets: make([]*pipeline.IndexMaintenanceTarget, 0, len(spec.Targets)),
	}
	for _, target := range spec.Targets {
		wire.Targets = append(wire.Targets, &pipeline.IndexMaintenanceTarget{Role: target.Role, Name: target.Name, Id: target.ID})
	}
	return wire, nil
}

func decodeIndexMaintenanceSpec(wire *pipeline.IndexMaintenanceSpec) (*executor.IndexMaintenanceSpec, error) {
	if wire == nil {
		return nil, nil
	}
	if len(wire.Targets) > 16 {
		return nil, moerr.NewInvalidInputNoCtx("too many index maintenance targets")
	}
	spec := &executor.IndexMaintenanceSpec{
		Function: wire.Function, AccountID: wire.AccountId, Database: wire.Database,
		DatabaseID: wire.DatabaseId, BaseName: wire.BaseName, BaseID: wire.BaseId,
		Targets: make([]executor.IndexMaintenanceTarget, 0, len(wire.Targets)),
	}
	for _, target := range wire.Targets {
		if target == nil {
			return nil, moerr.NewInvalidInputNoCtx("missing index maintenance target")
		}
		spec.Targets = append(spec.Targets, executor.IndexMaintenanceTarget{Role: target.Role, Name: target.Name, ID: target.Id})
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return spec, nil
}
