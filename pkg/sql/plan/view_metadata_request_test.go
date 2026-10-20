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

package plan

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

type viewMetadataTestCompiler struct {
	*viewSchemaTestCompiler
	ctx context.Context
}

func (c *viewMetadataTestCompiler) GetContext() context.Context    { return c.ctx }
func (c *viewMetadataTestCompiler) SetContext(ctx context.Context) { c.ctx = ctx }

func TestViewMetadataRequestSharesBudgetAndAuthorizesMemoHits(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "current_v", "select n_nationkey from nation")
	r := NewViewMetadataRequest(t.Context(), f.provider)
	defer r.Close()
	require.False(t, HasViewMetadataRequest(t.Context()))
	require.True(t, HasViewMetadataRequest(r.Context()))
	for i := 0; i < 2; i++ {
		func() {
			result, err := DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
			require.NoError(t, err)
			defer result.Release()
			columns, err := result.Columns()
			require.NoError(t, err)
			require.Equal(t, "n_nationkey", columns[0].Name)
			require.Equal(t, int32(types.T_int32), columns[0].Typ.Id)
		}()
	}
	require.Equal(t, int64(2), f.authorizes.Load())
	require.Equal(t, uint64(1), r.schema.hits)
	r.schema.roots = viewSchemaWorkLimit
	_, err := DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
	require.ErrorIs(t, err, ErrViewSchemaLimit)
	r.Close()
	require.Zero(t, f.generation.Used())
	require.Equal(t, int64(1), f.closes.Load())
	_, err = DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
	require.ErrorIs(t, err, ErrViewSchemaClosed)
	_, err = DescribeViewMetadata(t.Context(), "tpch", "current_v", nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
}

func TestViewMetadataRequestCancellationWhileWaitingDoesNotOwnBinder(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "current_v", "select n_nationkey from nation")
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	f.binding.Authorize = func(ctx context.Context, _, _ string, _ *Snapshot) error {
		close(entered)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-resume:
			return nil
		}
	}
	r := NewViewMetadataRequest(t.Context(), f.provider)
	defer r.Close()
	var firstErr error
	go func() {
		defer close(done)
		result, err := DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
		firstErr = err
		result.Release()
	}()
	viewSchemaTestWait(t, entered)
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	_, err := DescribeViewMetadata(ctx, "tpch", "current_v", nil)
	require.ErrorIs(t, err, context.Canceled)
	close(resume)
	viewSchemaTestWait(t, done)
	require.NoError(t, firstErr)
}

func TestViewMetadataRequestCloseCancelsActiveBinderAndWaiters(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	entered, done, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	f.binding.Authorize = func(ctx context.Context, _, _ string, _ *Snapshot) error {
		close(entered)
		<-ctx.Done()
		return context.Cause(ctx)
	}
	r := NewViewMetadataRequest(t.Context(), f.provider)
	defer r.Close()
	var firstErr error
	go func() {
		defer close(done)
		result, err := DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
		firstErr = err
		result.Release()
	}()
	viewSchemaTestWait(t, entered)
	go func() { r.Close(); close(closed) }()
	_, err := DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
	require.ErrorIs(t, err, ErrViewSchemaClosed)
	viewSchemaTestWait(t, done)
	viewSchemaTestWait(t, closed)
	require.ErrorIs(t, firstErr, ErrViewSchemaClosed)
	require.Zero(t, f.generation.Used())
}

func TestViewMetadataShowUsesCommonRequest(t *testing.T) {
	for _, sql := range []string{"describe current_v", "show columns from current_v", "show full columns from current_v", "show column_number from current_v", "show table_values from current_v"} {
		t.Run(sql, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			def := f.addView(t, "current_v", "select n_nationkey from nation")
			r := NewViewMetadataRequest(t.Context(), f.provider)
			defer r.Close()
			ctx := &viewMetadataTestCompiler{viewSchemaTestCompiler: f.compiler, ctx: r.Context()}
			p, err := buildViewMetadataConsumerPlan(t, ctx, sql, false)
			require.NoError(t, err)
			require.Positive(t, f.authorizes.Load())
			deps := p.GetQuery().CatalogDependencies
			require.True(t, p.GetQuery().ViewMetadataDependsOnUdf, "untracked non-relation dependencies force SHOW rebind")
			ids := make(map[int64]bool)
			for _, dep := range deps {
				ids[dep.Obj] = true
			}
			require.True(t, ids[int64(def.TblId)])
			require.True(t, ids[10], "the VALUES plan must retain the base relation identity")
			require.Equal(t, "stale", def.Cols[0].Name, "description cannot publish into the physical catalog")
			f.authErr = errors.New("denied")
			_, err = buildViewMetadataConsumerPlan(t, ctx, sql, false)
			require.ErrorIs(t, err, f.authErr)
		})
	}
}

func TestViewMetadataDependencyRefsPreserveDomain(t *testing.T) {
	deps := []ViewDependency{
		{AccountID: 7, DatabaseID: 3, RelationID: 9, Version: 4, DatabaseName: "pub", RelationName: "v", SubscriptionName: "sub"},
		{AccountID: 7, DatabaseID: 3, RelationID: 10, Version: 5, DatabaseName: "pub", RelationName: "t"},
		{AccountID: 42, DatabaseID: 4, RelationID: 11, Version: 6, DatabaseName: "local", RelationName: "t"},
		{AccountID: 7, DatabaseID: 3, RelationID: 9, Version: 4, DatabaseName: "pub", RelationName: "v", SubscriptionName: "another_sub"},
	}
	refs := viewMetadataDependencyRefs(deps, 42)
	require.Len(t, refs, 4)
	require.Equal(t, "another_sub", refs[3].SubscriptionName)
	require.Equal(t, int32(7), refs[0].PubInfo.TenantId)
	require.Equal(t, "sub", refs[0].SubscriptionName)
	require.Equal(t, int32(7), refs[1].PubInfo.TenantId)
	require.Nil(t, refs[2].PubInfo)
	require.Equal(t, int64(6), refs[2].Server)
}

func TestViewMetadataRejectsChangedConsumerIdentity(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "current_v", "select n_name from nation")
	r := NewViewMetadataRequest(defines.AttachAccount(t.Context(), 42, 7, 9), f.provider)
	defer r.Close()
	for _, ids := range [][3]uint32{{43, 7, 9}, {42, 8, 9}, {42, 7, 10}} {
		_, err := DescribeViewMetadata(defines.AttachAccount(r.Context(), ids[0], ids[1], ids[2]), "tpch", "current_v", nil)
		require.ErrorIs(t, err, ErrViewSchemaChanged)
	}
	require.Zero(t, f.authorizes.Load())
	result, err := DescribeViewMetadata(defines.AttachAccount(r.Context(), 42, 7, 9), "tpch", "current_v", nil)
	require.NoError(t, err)
	result.Release()
}

func TestViewMetadataBranchesShareWorkAndReleaseCancellation(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "current_v", "select n_name from nation")
	r := NewViewMetadataRequest(t.Context(), f.provider)
	defer r.Close()
	for i := 0; i < 2; i++ {
		branch, cancel := context.WithCancel(r.Context())
		result, err := DescribeViewMetadata(branch, "tpch", "current_v", nil)
		require.NoError(t, err)
		result.Release()
		cancel() // completed branch cleanup must not cancel later sibling work
		if i == 0 {
			r.schema.work = viewSchemaWorkLimit - 1
		}
	}
	require.Equal(t, viewSchemaWorkLimit, r.schema.work)
	_, err := DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
	require.ErrorIs(t, err, ErrViewSchemaLimit)
	require.Zero(t, f.generation.Used())
}

func TestViewMetadataCandidateFailureProvenance(t *testing.T) {
	for _, stage := range []string{"authorize", "open", "root", "definition"} {
		t.Run(stage, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			f.addView(t, "current_v", "select n_nationkey from nation")
			missing := moerr.NewNoSuchTable(t.Context(), "tpch", "missing")
			switch stage {
			case "authorize":
				f.authErr = missing
			case "open":
				f.provider = func(context.Context) (*ViewSchemaBinding, error) { return nil, missing }
			default:
				f.compiler.resolve = func(_ context.Context, _, name string, _ *Snapshot) error {
					if (stage == "root" && name == "current_v") || (stage == "definition" && name == "nation") {
						return missing
					}
					return nil
				}
			}
			r := NewViewMetadataRequest(t.Context(), f.provider)
			defer r.Close()
			result, invalid, err := DescribeViewMetadataCandidate(r.Context(), "tpch", "current_v", nil)
			require.Nil(t, result)
			require.ErrorIs(t, err, missing)
			require.Equal(t, stage == "definition", invalid)
			_, err = r.schema.Describe("tpch", "current_v", nil)
			require.Same(t, missing, err, "existing S2 callers retain the original error identity/code")
			r.Close()
			require.Zero(t, f.generation.Used())
		})
	}
}
