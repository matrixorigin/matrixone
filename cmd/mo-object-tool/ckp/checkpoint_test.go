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

package ckp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCkpOfflineKindFlags checks the ckp command exposes the persistent
// --local/--s3/--local2 format flags and that kindFromFlags is strict: with no
// flag set it returns an error (no silent default). The single call exercises
// the whole resolver body; the success path is covered by TestCkpInfoEmptyDir.
func TestCkpOfflineKindFlags(t *testing.T) {
	cmd := PrepareCommand()
	for _, name := range []string{"local", "s3", "local2"} {
		require.NotNilf(t, cmd.PersistentFlags().Lookup(name), "ckp --%s", name)
	}

	_, err := kindFromFlags(cmd)
	require.Error(t, err) // none set
}

// TestCkpRequiresFormatFlag drives the root and view RunE with no format flag;
// both must fail at the resolver, before runViewer touches the data dir or the
// interactive TUI.
func TestCkpRequiresFormatFlag(t *testing.T) {
	for _, args := range [][]string{
		{t.TempDir()},         // root command
		{"view", t.TempDir()}, // view subcommand
	} {
		c := PrepareCommand()
		c.SetArgs(args)
		require.Error(t, c.Execute())
	}
}

// TestCkpInfoEmptyDir runs `info` against an empty dir: the offline fs opens,
// finds zero checkpoint metas, prints the summary and returns nil. It exercises
// infoCommand/setupLogFile/checkpointtool.Open without launching the TUI.
func TestCkpInfoEmptyDir(t *testing.T) {
	cmd := PrepareCommand()
	cmd.SetArgs([]string{"info", "--local", t.TempDir()})
	require.NoError(t, cmd.Execute())
}
