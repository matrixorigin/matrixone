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
// flag conflict and exposes the --local / --local2 format flags. This branch
// keeps --s3 as remote storage arguments, so local DISK remains the default.
func TestOfflineKindFlags(t *testing.T) {
	cmd := PrepareCommand()

	for _, name := range []string{"local", "local2"} {
		require.NotNilf(t, cmd.PersistentFlags().Lookup(name), "object --%s", name)
	}

	kind, err := kindFromFlags(cmd)
	require.NoError(t, err)
	require.Equal(t, objectio.OfflineKindLocal, kind)

	// resolver: local/local2 map to their kind; both together -> error
	for _, tc := range []struct {
		local, local2 bool
		want          string
		wantErr       bool
	}{
		{true, false, objectio.OfflineKindLocal, false},
		{false, true, objectio.OfflineKindLocal2, false},
		{false, false, objectio.OfflineKindLocal, false},
		{true, true, "", true},
	} {
		c := PrepareCommand()
		if tc.local {
			require.NoError(t, c.ParseFlags([]string{"--local"}))
		}
		if tc.local2 {
			require.NoError(t, c.ParseFlags([]string{"--local2"}))
		}
		got, err := kindFromFlags(c)
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
// object (error, no TUI).
func TestObjectInfoViewCommands(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.obj")

	c := PrepareCommand()
	c.SetArgs([]string{"info", "--local", missing})
	require.Error(t, c.Execute())
}
