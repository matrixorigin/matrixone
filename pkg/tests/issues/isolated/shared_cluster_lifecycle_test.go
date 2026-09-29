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

package isolated

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// releaseSharedSingleCNCluster lets a specialized scenario acquire the
// process-wide test-cluster admission after a shared one-CN scenario. A
// successful release leaves the fixture reusable, so this helper is safe when
// test order is shuffled or -count runs the package again. It is a no-op when
// the shared fixture was not selected by -run.
func releaseSharedSingleCNCluster(t *testing.T) {
	t.Helper()
	require.NoError(t, embed.CloseSingleCNBaseClusterTests())
}
