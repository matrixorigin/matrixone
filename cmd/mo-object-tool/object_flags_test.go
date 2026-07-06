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
	"path/filepath"
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

	// no flag set -> error (no silent default)
	_, err := kindFromFlags(cmd)
	require.Error(t, err)

	// resolver: each single flag maps to its kind; none/multiple -> error
	for _, tc := range []struct {
		local, s3, local2 bool
		want              string
		wantErr           bool
	}{
		{true, false, false, objectio.OfflineKindLocal, false},
		{false, true, false, objectio.OfflineKindS3, false},
		{false, false, true, objectio.OfflineKindLocal2, false},
		{false, false, false, "", true}, // none
		{true, false, true, "", true},   // conflicting
		{true, true, true, "", true},    // all
	} {
		got, err := objectio.OfflineKindStrict(tc.local, tc.s3, tc.local2)
		if tc.wantErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		}
	}
}

// TestObjectInfoViewCommands drives the info/view subcommands. `info` with a
// format flag on a missing object file opens the fs then fails to read the
// object (error, no TUI). `view` with no format flag fails at the resolver,
// before the interactive viewer launches.
func TestObjectInfoViewCommands(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.obj")

	c := PrepareCommand()
	c.SetArgs([]string{"info", "--local", missing})
	require.Error(t, c.Execute())

	c2 := PrepareCommand()
	c2.SetArgs([]string{"view", missing})
	require.Error(t, c2.Execute())
}
