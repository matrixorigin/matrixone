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

//go:build race || typecheck

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestTypedReadTypecheckGuard(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	vec := NewVec(types.T_int64.ToType())
	defer vec.Free(mp)
	require.NoError(t, AppendFixedList(vec, []int64{0, 0, 0}, nil, mp))
	require.Panics(t, func() { _ = GetFixedAtNoTypeCheck[int32](vec, 0) })
	require.Panics(t, func() { _ = vec.GetBytesAt(0) })
}
