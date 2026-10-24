//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
)

func m0AfterApplyAddColumn(ctx context.Context) error {
	_, message, injected := fault.TriggerFaultWithContext(ctx, "fj/m0/alter/tn-after-apply")
	if !injected {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if message != "" {
		return moerr.NewInternalError(ctx, message)
	}
	return nil
}
