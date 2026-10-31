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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/require"
)

func TestCatalogReinitReappliesSystemCreatedTime(t *testing.T) {
	sid := t.Name()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(sid, rt)
	t.Cleanup(func() { runtime.CompareAndDeleteServiceBasedRuntime(sid, rt) })
	catalog.SetupDefines(sid)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	e := &Engine{service: sid, mp: mp, packerPool: fileservice.NewPool(
		1, func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() }, func(p *types.Packer) { p.Close() },
	)}
	require.NoError(t, e.init(context.Background()))

	aid := catalog.MO_TABLES_ACCOUNT_ID_IDX + cache.MO_OFF
	name := catalog.MO_TABLES_REL_NAME_IDX + cache.MO_OFF
	created := catalog.MO_TABLES_CREATED_TIME_IDX + cache.MO_OFF
	b := batch.NewWithSize(max(aid, name, created) + 1)
	b.Vecs[aid] = vector.NewVec(types.T_uint32.ToType())
	b.Vecs[name] = vector.NewVec(types.T_varchar.ToType())
	b.Vecs[created] = vector.NewVec(types.T_timestamp.ToType())
	defer b.Clean(mp)
	require.NoError(t, vector.AppendFixed(b.Vecs[aid], uint32(0), false, mp))
	require.NoError(t, vector.AppendBytes(b.Vecs[name], []byte("mo_user"), false, mp))
	want := types.Timestamp(123456)
	require.NoError(t, vector.AppendFixed(b.Vecs[created], want, false, mp))
	b.SetRowCount(1)
	e.tryAdjustSysTablesCreatedTimeWithBatch(b)
	require.True(t, e.timeFixed)
	for _, v := range e.sysTablesCreatedTime {
		require.Equal(t, want, vector.GetFixedAtWithTypeCheck[types.Timestamp](v, 0))
	}

	// A later catalog replay can fail after mo_tables has been applied.
	// Reinitializing creates new vectors which need the correction again.
	require.NoError(t, e.init(context.Background()))
	require.False(t, e.timeFixed)
	e.tryAdjustSysTablesCreatedTimeWithBatch(b)
	require.True(t, e.timeFixed)
	for _, v := range e.sysTablesCreatedTime {
		require.Equal(t, want, vector.GetFixedAtWithTypeCheck[types.Timestamp](v, 0))
	}
}
