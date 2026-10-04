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

package fileservice

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type testBatchObjectCopier struct {
	dummyFileService
	results []ObjectCopyResult
	err     error
}

func (c *testBatchObjectCopier) CopyObjects(
	context.Context,
	FileService,
	[]ObjectCopy,
	ObjectCopyOptions,
) ([]ObjectCopyResult, error) {
	return c.results, c.err
}

func TestCopyObjectsCapabilitySelection(t *testing.T) {
	ctx := context.Background()
	copies := []ObjectCopy{{SourcePath: "a", DestinationPath: "b"}}

	results, err := CopyObjects(ctx, nil, nil, nil, ObjectCopyOptions{})
	require.NoError(t, err)
	require.Empty(t, results)

	results, err = CopyObjects(ctx, nil, dummyFileService{}, copies, ObjectCopyOptions{})
	require.NoError(t, err)
	require.Equal(t, []ObjectCopyResult{{}}, results)

	expectedErr := errors.New("batch failed")
	batch := &testBatchObjectCopier{results: []ObjectCopyResult{{Copied: true}}, err: expectedErr}
	results, err = CopyObjects(ctx, nil, batch, copies, ObjectCopyOptions{})
	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, batch.results, results)
}
