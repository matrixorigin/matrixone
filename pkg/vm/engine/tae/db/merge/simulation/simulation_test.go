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
	"github.com/stretchr/testify/require"
	"math/rand/v2"
	"os"
	"testing"
	"time"
)

func TestSimulation(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "example")
	require.NoError(t, err)

	defer os.Remove(tmpFile.Name()) // clean up

	_, err = tmpFile.Write([]byte("w\n"))
	require.NoError(t, err)

	_, err = tmpFile.Write([]byte("a\n"))
	require.NoError(t, err)

	_, err = tmpFile.Write([]byte("p\n"))
	require.NoError(t, err)

	_, err = tmpFile.Write([]byte("q\n"))
	require.NoError(t, err)

	_, err = tmpFile.Seek(0, 0)
	require.NoError(t, err)

	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }() // Restore original Stdin

	os.Stdin = tmpFile

	sim(10000, 100*time.Millisecond, func() time.Duration {
		return time.Duration(rand.ExpFloat64()*100) * time.Millisecond
	})
}
