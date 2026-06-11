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

package toolfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

const objectIOMagic = 0xFFFFFFFF

func TestLazyCacheFSReadsMagicPrefixedRemoteObjectsWithoutLocalChecksum(t *testing.T) {
	ctx := context.Background()
	remote, err := fileservice.NewMemoryFS("SHARED", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	payload := make([]byte, 4, 32)
	binary.LittleEndian.PutUint32(payload, objectIOMagic)
	payload = append(payload, []byte("checkpoint-object-bytes")...)

	require.NoError(t, remote.Write(ctx, fileservice.IOVector{
		FilePath: "ckp/meta",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(payload)),
			Data:   payload,
		}},
	}))

	fs, _, err := newLazyCacheFS(ctx, remote)
	require.NoError(t, err)
	defer fs.Close(ctx)

	vec := &fileservice.IOVector{
		FilePath: "SHARED:ckp/meta",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   -1,
		}},
	}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, payload, vec.Entries[0].Data)

	var buf bytes.Buffer
	cacheVec := &fileservice.IOVector{
		FilePath: "ckp/meta",
		Entries: []fileservice.IOEntry{{
			Offset:        4,
			Size:          10,
			WriterForRead: &buf,
		}},
	}
	require.NoError(t, fs.ReadCache(ctx, cacheVec))
	require.Equal(t, payload[4:14], buf.Bytes())
}
