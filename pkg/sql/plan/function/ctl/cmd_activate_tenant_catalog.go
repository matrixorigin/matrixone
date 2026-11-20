// Copyright 2024 Matrix Origin
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

package ctl

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// handleActivateTenantCatalog activates the catalog cache for a specific
// tenant account on the current CN. This is only callable by the sys account.
//
// Usage: select mo_ctl('cn', 'ActivateTenantCatalog', '<account_id>');
func handleActivateTenantCatalog(
	proc *process.Process,
	service serviceType,
	parameter string,
	_ requestSender,
) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewNotSupportedf(
			proc.Ctx, "ActivateTenantCatalog only supports CN service")
	}

	callerID, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return Result{}, err
	}
	if callerID != 0 {
		return Result{}, moerr.NewInternalError(
			proc.Ctx, "ActivateTenantCatalog can only be called by sys account")
	}

	targetID, err := strconv.ParseUint(strings.TrimSpace(parameter), 10, 32)
	if err != nil {
		return Result{}, moerr.NewInvalidInput(
			proc.Ctx, "invalid account id: "+parameter)
	}

	eng := proc.GetSessionInfo().StorageEngine
	activator, ok := eng.(engine.TenantCatalogActivator)
	if !ok {
		// Engine does not support lazy catalog (e.g. unit-test engine).
		return Result{
			Method: ActivateTenantCatalogMethod,
			Data:   "not supported by engine, skipped",
		}, nil
	}

	if err = activator.ActivateTenantCatalog(proc.Ctx, uint32(targetID)); err != nil {
		return Result{}, err
	}

	return Result{
		Method: ActivateTenantCatalogMethod,
		Data:   fmt.Sprintf("account %d activated on this CN", targetID),
	}, nil
}
