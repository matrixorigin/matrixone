// Copyright 2022 Matrix Origin
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

package testengine

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestTestEngine(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			catalog.SetupDefines("")
			engine, client, compilerCtx := New(context.Background())
			_ = engine
			_ = client
			_ = compilerCtx
		},
	)
}

func BenchmarkNew(b *testing.B) {
	for i := 0; i < b.N; i++ {
		New(context.Background())
	}
}

func TestRelationExists(t *testing.T) {
	catalog.SetupDefines("")
	ctx := context.Background()
	ctx = defines.AttachAccountId(ctx, 0)
	eng, _, _ := New(ctx)
	db, err := eng.Database(ctx, "test", nil)
	require.NoError(t, err)

	exist, err := db.RelationExists(ctx, "NotExist", nil)
	require.NoError(t, err)
	require.False(t, exist)

	exist, err = db.RelationExists(ctx, "r", nil)
	require.NoError(t, err)
	require.True(t, exist)
}
