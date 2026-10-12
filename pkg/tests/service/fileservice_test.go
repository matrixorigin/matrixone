// Copyright 2026 Matrix Origin
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

package service

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type trackedFileService struct {
	fileservice.FileService
	closes int
}

func (f *trackedFileService) Close(context.Context) { f.closes++ }

func TestFileServicesCloseEachOwnerOnce(t *testing.T) {
	shared := &trackedFileService{}
	etl := &trackedFileService{}
	owned := &fileServices{
		tnLocalFSs: []fileservice.FileService{shared},
		cnLocalFSs: []fileservice.FileService{shared, etl},
		s3FS:       shared,
		etlFS:      etl,
	}

	owned.Close(context.Background())
	owned.Close(context.Background())

	require.Equal(t, 1, shared.closes)
	require.Equal(t, 1, etl.closes)
}

func TestClusterCloseBeforeStartRetiresFileServices(t *testing.T) {
	shared := &trackedFileService{}
	etl := &trackedFileService{}
	owned := &fileServices{
		tnLocalFSs: []fileservice.FileService{shared},
		s3FS:       shared,
		etlFS:      etl,
	}
	c := &testCluster{
		logger:       zap.NewNop(),
		stopper:      stopper.NewStopper(t.Name()),
		fileservices: owned,
	}

	require.NoError(t, c.Close())
	require.Equal(t, 1, shared.closes)
	require.Equal(t, 1, etl.closes)
	require.NoError(t, c.Close())
	require.Equal(t, 1, shared.closes)
	require.Equal(t, 1, etl.closes)
}
