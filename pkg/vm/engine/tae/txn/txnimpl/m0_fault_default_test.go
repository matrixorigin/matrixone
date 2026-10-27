//go:build !mo_m0

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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestM0TNAppliedFaultInertByDefault(t *testing.T) {
	const point = "fj/m0/alter/tn-after-apply"
	wasEnabled := fault.Status()
	fault.Enable()
	defer func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), point)
		if !wasEnabled {
			fault.Disable()
		}
	}()
	require.NoError(t, fault.AddFaultPoint(t.Context(), point, ":::", "echo", 0, "must remain inert", false))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, m0AfterApplyAddColumn(ctx))
}
