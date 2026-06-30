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

package object

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

// TestOfflineKindFlags verifies the object command tree builds without a cobra
// flag conflict and exposes the --local / --s3 / --local2 format flags
// (persistent, so the view/info subcommands inherit them at runtime).
func TestOfflineKindFlags(t *testing.T) {
	cmd := PrepareCommand()

	for _, name := range []string{"local", "s3", "local2"} {
		require.NotNilf(t, cmd.PersistentFlags().Lookup(name), "object --%s", name)
	}

	// resolver maps the flags to the right offline kind
	require.Equal(t, objectio.OfflineKindLocal, objectio.OfflineKind(false, false, false, objectio.OfflineKindLocal))
	require.Equal(t, objectio.OfflineKindLocal2, objectio.OfflineKind(false, false, true, objectio.OfflineKindLocal))
	require.Equal(t, objectio.OfflineKindS3, objectio.OfflineKind(false, true, false, objectio.OfflineKindLocal))
}
