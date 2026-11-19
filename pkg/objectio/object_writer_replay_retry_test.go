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

package objectio

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

type replayRetryFileService struct {
	fileservice.FileService
	writes, deletes int
	policies        []fileservice.Policy
}

func (f *replayRetryFileService) Write(_ context.Context, vector fileservice.IOVector) error {
	f.writes++
	f.policies = append(f.policies, vector.Policy)
	if f.writes == 1 {
		return moerr.NewFileAlreadyExistsNoCtx(vector.FilePath)
	}
	return nil
}

func (f *replayRetryFileService) Delete(_ context.Context, _ ...string) error {
	f.deletes++
	return nil
}

func TestObjectWriterReplayPolicyOnRetry(t *testing.T) {
	fs := new(replayRetryFileService)
	writer, err := NewObjectWriterSpecial(WriterNormal, "object", fs)
	require.NoError(t, err)
	writer.buffer.Write([]byte("payload"))
	require.NoError(t, writer.Sync(context.Background()))
	require.Equal(t, 2, fs.writes)
	require.Equal(t, 1, fs.deletes)
	require.Len(t, fs.policies, 2)
	for _, policy := range fs.policies {
		require.True(t, policy.Any(fileservice.ReplayDataForDiskCache))
	}
}
