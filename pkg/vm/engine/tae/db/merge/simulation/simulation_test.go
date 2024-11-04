// Copyright 2024 Matrix Origin
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

package simulation

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimulation(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "example")
	require.NoError(t, err)

	defer os.Remove(tmpFile.Name()) // clean up

	_, err = tmpFile.Write([]byte("w\na\np\nq\n"))
	require.NoError(t, err)

	_, err = tmpFile.Seek(0, 0)
	require.NoError(t, err)

	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }() // Restore original Stdin

	os.Stdin = tmpFile

	MergeSimulationCmd.Run(MergeSimulationCmd, nil)
}
