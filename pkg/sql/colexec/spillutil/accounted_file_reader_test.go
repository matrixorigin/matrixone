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

package spillutil

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccountedFileReaderDirectWithoutAccount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spill.bin")
	require.NoError(t, os.WriteFile(path, []byte("payload"), 0o600))
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	reader, err := NewAccountedFileReader(nil, nil, file)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), data)
	require.Zero(t, reader.Buffered())
	reader.Free()
}
