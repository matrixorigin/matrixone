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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/stretchr/testify/require"
)

func TestImportMemoryCapacityDoesNotForceWholeTransactionRollback(t *testing.T) {
	err := &rscthrottler.WorkingSetCapacityError{Minimum: 300, Available: 200}
	require.False(t, isErrorRollbackWholeTxn(err), "the existing statement rollback path must preserve prior successful explicit transaction statements")
}
