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

package objectio

import (
	"context"
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

// TestReadHeaderMagic covers Header.Magic() and the ReadHeader magic guard added
// for the DISK-V2 raw on-disk format.
//
// A valid object header carries Magic in its first 8 bytes; a file whose header
// does not — e.g. a legacy DISK/CRC-framed file opened through the raw DISK-V2
// backend, or on-disk corruption at the header — is rejected by ReadHeader
// instead of silently returning wrong bytes. The happy path (magic matches, no
// error) is exercised by the object round-trip tests in writer_test.go.
func TestReadHeaderMagic(t *testing.T) {
	// Accessor: BuildHeader stores Magic in the first 8 bytes.
	require.Equal(t, uint64(Magic), BuildHeader().Magic())

	ctx := context.Background()
	mp := mpool.MustNewZero()

	dir := path.Join(InitTestEnv(ModuleName, t.Name()), "local")
	service, err := fileservice.NewFileService(ctx, fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
		Cache:   fileservice.DisabledCacheConfig,
	}, nil)
	require.NoError(t, err)
	defer service.Close(ctx)

	// A file whose first bytes are NOT the object magic (all-zero here → magic 0,
	// which != Magic) stands in for a legacy DISK/CRC file read raw as DISK-V2,
	// or corruption of the header region.
	const badName = "bad.blk"
	bad := make([]byte, HeaderSize)
	require.NoError(t, service.Write(ctx, fileservice.IOVector{
		FilePath: badName,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(bad)), Data: bad}},
	}))

	r, err := NewObjectReaderWithStr(badName, service)
	require.NoError(t, err)

	// ReadAllMeta -> ReadHeader must reject the bad magic rather than proceed.
	_, err = r.ReadAllMeta(ctx, mp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad header magic")
}
