// Copyright 2021 Matrix Origin
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

package checkpointtool

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

// TestOpenWithKind verifies the offline checkpoint reader honors WithKind (the
// --local/--s3/--local2 selector) for both DISK and DISK-V2 formats. An empty
// directory has no checkpoint metadata, so Open succeeds with zero entries and
// the reader records the requested on-disk format.
func TestOpenWithKind(t *testing.T) {
	ctx := context.Background()

	for _, kind := range []string{objectio.OfflineKindLocal, objectio.OfflineKindLocal2} {
		r, err := Open(ctx, t.TempDir(), WithKind(kind))
		require.NoErrorf(t, err, "kind=%s", kind)
		require.Equal(t, kind, r.Kind())
		require.Empty(t, r.Entries())
		require.NoError(t, r.Close())
	}

	// default (no WithKind) is the legacy local DISK format
	r, err := Open(ctx, t.TempDir())
	require.NoError(t, err)
	require.Equal(t, objectio.OfflineKindLocal, r.Kind())
	require.NoError(t, r.Close())
}
