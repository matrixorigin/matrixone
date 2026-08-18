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

package ctl

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type refreshViewMetadataFunc func(*process.Process, string) (int, error)

var refreshViewMetadataHandlers sync.Map

// RegisterRefreshViewMetadataHandler binds the command to the CN-local engine
// owner without introducing a dependency from the function package to compile.
func RegisterRefreshViewMetadataHandler(serviceID string, handler refreshViewMetadataFunc) {
	if handler == nil {
		refreshViewMetadataHandlers.Delete(serviceID)
		return
	}
	refreshViewMetadataHandlers.Store(serviceID, handler)
}

func handleRefreshViewMetadata(
	proc *process.Process,
	service serviceType,
	parameter string,
	_ requestSender,
) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewWrongServiceNoCtx("CN", string(service))
	}
	handler, ok := refreshViewMetadataHandlers.Load(proc.GetService())
	if !ok {
		handler, ok = refreshViewMetadataHandlers.Load("")
	}
	if !ok {
		return Result{}, moerr.NewInternalErrorNoCtx("View metadata refresh handler is not registered")
	}
	count, err := handler.(refreshViewMetadataFunc)(proc, parameter)
	if err != nil {
		return Result{}, err
	}
	return Result{Method: RefreshViewMetadata, Data: count}, nil
}
