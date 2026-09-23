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

package catalog

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLockViewMetadataLifecycleOrderAndErrors(t *testing.T) {
	for _, failAt := range []int{0, 1, 2} {
		t.Run([]string{"success", "snapshot failure", "view failure"}[failAt], func(t *testing.T) {
			var sqls []string
			failure := errors.New("lock canceled")
			err := LockViewMetadataLifecycle(func(sql string) error {
				sqls = append(sqls, sql)
				if len(sqls) == failAt {
					return failure
				}
				return nil
			})
			if failAt == 0 {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, failure)
			}
			if failAt == 1 {
				require.Equal(t, []string{SnapshotLifecycleGateSQL}, sqls)
			} else {
				require.Equal(t, []string{SnapshotLifecycleGateSQL, ViewMetadataLifecycleGateSQL}, sqls)
			}
		})
	}
}
