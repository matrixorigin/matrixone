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

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ValidateLifecycleRestoreTableAccess reserves the Restore staging namespace
// from frontend SQL. Internal SQLExecutor calls have IsFrontend=false and use
// the ordinary table implementation without a Lifecycle-specific data path.
func ValidateLifecycleRestoreTableAccess(
	ctx context.Context,
	isFrontend bool,
	tableName string,
) error {
	if !isFrontend || !catalog.IsLifecycleRestoreStagingTable(tableName) {
		return nil
	}
	return moerr.NewNotSupportedf(
		ctx,
		"access to internal Lifecycle Restore staging table %s",
		tableName,
	)
}

func compilerContextIsFrontend(ctx CompilerContext) bool {
	proc := ctx.GetProcess()
	return proc != nil && proc.Base != nil && proc.Base.IsFrontend
}
