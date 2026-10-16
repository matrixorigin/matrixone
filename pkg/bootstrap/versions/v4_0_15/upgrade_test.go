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

package v4_0_15

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewUpgrade(t *testing.T) {
	require.Equal(t, "4.0.15", Handler.Metadata().Version)
	require.Equal(t, defines.MORPCVersion116, Handler.Metadata().RequiredProtocolVersion)
	for _, failure := range []bool{false, true} {
		calls := 0
		injected := errors.New("catalog creation failed")
		tx := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			calls++
			require.True(t, strings.HasPrefix(sql, "create cluster table if not exists mo_catalog.mo_"))
			if failure && calls == 2 {
				return executor.Result{}, injected
			}
			return executor.Result{}, nil
		}, nil)
		err := Handler.HandleClusterUpgrade(context.Background(), tx)
		if failure {
			require.ErrorIs(t, err, injected)
			require.Equal(t, 2, calls)
		} else {
			require.NoError(t, err)
			require.Equal(t, 4, calls)
		}
	}
}
