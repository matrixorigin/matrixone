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

package iceberg

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
)

type WriteRuntimeCoordinatorFactory struct {
	Append icebergwrite.CoordinatorFactory
	DML    icebergwrite.CoordinatorFactory
}

func (f WriteRuntimeCoordinatorFactory) NewCoordinator(ctx context.Context, req icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
	switch req.Operation {
	case "", icebergwrite.OperationAppend:
		if f.Append == nil {
			return nil, nil
		}
		return f.Append.NewCoordinator(ctx, req)
	case icebergwrite.OperationDelete, icebergwrite.OperationUpdate, icebergwrite.OperationMerge, icebergwrite.OperationOverwrite:
		if f.DML == nil {
			return nil, nil
		}
		return f.DML.NewCoordinator(ctx, req)
	default:
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg write operation is unsupported", map[string]string{
			"operation": req.Operation,
		}))
	}
}

var _ icebergwrite.CoordinatorFactory = WriteRuntimeCoordinatorFactory{}
