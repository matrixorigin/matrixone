// Copyright 2025 Matrix Origin
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

package native

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestExpandDeletePathsWithLocators(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: "obj_001",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   1,
			Data:   []byte("o"),
		}},
	}))
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: "obj_001.fts.a.seg",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   1,
			Data:   []byte("a"),
		}},
	}))
	require.NoError(t, WriteSidecarLocator(ctx, fs, "obj_001", []SidecarLocatorEntry{
		{IndexTable: "__idx_body", FilePath: "obj_001.fts.a.seg"},
		{IndexTable: "__idx_body_missing", FilePath: "obj_001.fts.missing.seg"},
	}))

	expanded := ExpandDeletePathsWithLocators(ctx, fs, []string{"obj_001"})
	require.Equal(t, []string{
		"obj_001",
		SidecarLocatorPath("obj_001"),
		"obj_001.fts.a.seg",
	}, expanded)
}

func TestExpandDeletePathsWithBrokenLocatorDoesNotBlockBaseDelete(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: "obj_002",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   1,
			Data:   []byte("o"),
		}},
	}))
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: SidecarLocatorPath("obj_002"),
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len("{bad-json")),
			Data:   []byte("{bad-json"),
		}},
	}))

	expanded := ExpandDeletePathsWithLocators(ctx, fs, []string{"obj_002"})
	require.Equal(t, []string{
		"obj_002",
		SidecarLocatorPath("obj_002"),
	}, expanded)
}
