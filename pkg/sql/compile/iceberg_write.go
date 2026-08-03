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

package compile

import (
	"context"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const IcebergAppendCoordinatorFactoryRuntimeKey = "iceberg.append.coordinator.factory"

func icebergAppendCoordinatorFactoryForCompile(ctx context.Context, proc *process.Process) (icebergwrite.CoordinatorFactory, error) {
	if proc == nil {
		return nil, nil
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return nil, nil
	}
	value, ok := rt.GetGlobalVariables(IcebergAppendCoordinatorFactoryRuntimeKey)
	if !ok || value == nil {
		return nil, nil
	}
	factory, ok := value.(icebergwrite.CoordinatorFactory)
	if !ok {
		return nil, icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrConfigInvalid, "Iceberg append coordinator factory runtime variable has invalid type", nil))
	}
	return factory, nil
}
