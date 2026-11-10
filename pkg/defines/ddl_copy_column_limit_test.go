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

package defines

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAlterCopyColumnLimitOwnership(t *testing.T) {
	original, cancel := context.WithTimeout(context.WithValue(t.Context(), TenantIDKey{}, uint32(7)), time.Minute)
	defer cancel()
	parent := WithAlterCopyColumnLimit(original, "db", "Copy", 4097)
	require.Equal(t, 4097, AlterCopyColumnLimit(parent, "db", "copy"))
	require.Zero(t, AlterCopyColumnLimit(parent, "other_db", "copy"))
	require.Zero(t, AlterCopyColumnLimit(parent, "db", "copy_suffix"))
	names := []string{"%!%p0%!%copy", "%!%p1%!%copy"}
	children, err := WithAlterCopyPartitionColumnLimit(parent, "db", "copy", names)
	require.NoError(t, err)
	names[0] = "unrelated"
	require.Equal(t, 4097, AlterCopyColumnLimit(children, "db", "%!%p0%!%copy"))
	require.Zero(t, AlterCopyColumnLimit(children, "db", "unrelated"))
	require.Zero(t, AlterCopyColumnLimit(children, "db", "copy"))
	require.Zero(t, AlterCopyColumnLimit(children, "other_db", "%!%p0%!%copy"))
	require.Zero(t, AlterCopyColumnLimit(parent, "db", "%!%p0%!%copy"))
	require.Equal(t, uint32(7), children.Value(TenantIDKey{}))
	wantDeadline, _ := original.Deadline()
	gotDeadline, _ := children.Deadline()
	require.Equal(t, wantDeadline, gotDeadline)
	_, err = WithAlterCopyPartitionColumnLimit(children, "db", "copy", []string{"grandchild"})
	require.Error(t, err)
	_, err = WithAlterCopyPartitionColumnLimit(parent, "other_db", "copy", []string{"child"})
	require.Error(t, err)
	_, err = WithAlterCopyPartitionColumnLimit(parent, "db", "another", []string{"child"})
	require.Error(t, err)
	for _, bad := range [][]string{{""}, {"copy"}, {"child", "CHILD"}} {
		_, err = WithAlterCopyPartitionColumnLimit(parent, "db", "copy", bad)
		require.Error(t, err)
	}
	require.Zero(t, AlterCopyColumnLimit(WithoutAlterCopyColumnLimit(children), "db", "%!%p0%!%copy"))
	smaller := WithAlterCopyColumnLimit(parent, "db", "copy", 4096)
	require.Equal(t, 4096, AlterCopyColumnLimit(smaller, "db", "copy"))
	require.Equal(t, 4097, AlterCopyColumnLimit(parent, "db", "copy"))
	cancel()
	require.ErrorIs(t, children.Err(), context.Canceled)
}

func TestAlterCopyColumnLimitAbsent(t *testing.T) {
	require.Zero(t, AlterCopyColumnLimit(nil, "db", "copy"))
	require.Nil(t, WithoutAlterCopyColumnLimit(nil))
	ctx := t.Context()
	derived, err := WithAlterCopyPartitionColumnLimit(ctx, "db", "copy", []string{"child"})
	require.NoError(t, err)
	require.Same(t, ctx, derived)
	parent := WithAlterCopyColumnLimit(ctx, "db", "copy", 4097)
	require.Zero(t, AlterCopyColumnLimit(WithAlterCopyColumnLimit(parent, "", "copy", 4097), "db", "copy"))
}
