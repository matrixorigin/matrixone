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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestGetObjectMetaFileServicePreferShared(t *testing.T) {
	local, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	shared, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	fs, err := fileservice.NewFileServices(defines.LocalFileServiceName, local, shared)
	require.NoError(t, err)

	got, err := getObjectMetaFileService(fs)
	require.NoError(t, err)
	require.Equal(t, defines.SharedFileServiceName, got.Name())
}

func TestGetObjectMetaFileServiceFallbackToDefault(t *testing.T) {
	local, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	fs, err := fileservice.NewFileServices(defines.LocalFileServiceName, local)
	require.NoError(t, err)

	got, err := getObjectMetaFileService(fs)
	require.NoError(t, err)
	require.Equal(t, defines.LocalFileServiceName, got.Name())
	require.Same(t, local, got)
}
