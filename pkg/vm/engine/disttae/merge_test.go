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

package disttae

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCNMergeTaskReleaseClosesDataSource(t *testing.T) {
	source := &stubSnapshotDataSource{}
	task := &cnMergeTask{ds: source}

	task.Release()
	require.True(t, source.closed)
	require.Nil(t, task.ds)

	// Release is part of task-owner cleanup and must remain idempotent.
	task.Release()
}
