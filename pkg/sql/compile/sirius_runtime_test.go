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

package compile

import (
	"context"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/sidecarflight"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/stretchr/testify/require"
)

type siriusRuntimeTestProtector struct{}

func (siriusRuntimeTestProtector) Begin(context.Context) (
	func(context.Context, []byte, []string, time.Time) error,
	func(context.Context, []byte) error,
	func(),
	error,
) {
	return func(context.Context, []byte, []string, time.Time) error { return nil },
		func(context.Context, []byte) error { return nil }, func() {}, nil
}

func (siriusRuntimeTestProtector) Unregister(context.Context, []byte) error { return nil }

func TestSiriusRuntimeValidationAndLookup(t *testing.T) {
	require.Error(t, (*SiriusRuntime)(nil).Validate())
	require.NoError(t, (*SiriusRuntime)(nil).Close(context.Background()))

	valid := &SiriusRuntime{
		Flight:   &sidecarflight.Runtime{},
		Leases:   substrait.NewLeaseManager(1, siriusRuntimeTestProtector{}),
		Resolver: &substrait.ResolverServer{}, AuthorizedClientSPKIHash: make([]byte, 32),
		DataDir: t.TempDir(), LeaseTTL: time.Minute, CleanupTimeout: time.Second,
	}
	require.NoError(t, valid.Validate())

	service := "sirius-runtime-lookup-test"
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
	moruntime.SetupServiceBasedRuntime(service, rt)
	_, ok := lookupSiriusRuntime("sirius-runtime-missing")
	require.False(t, ok)
	rt.SetGlobalVariables(SiriusRuntimeKey, "wrong type")
	_, ok = lookupSiriusRuntime(service)
	require.False(t, ok)
	rt.SetGlobalVariables(SiriusRuntimeKey, &SiriusRuntime{})
	_, ok = lookupSiriusRuntime(service)
	require.False(t, ok)
	rt.SetGlobalVariables(SiriusRuntimeKey, valid)
	actual, ok := lookupSiriusRuntime(service)
	require.True(t, ok)
	require.Same(t, valid, actual)
	require.True(t, rt.CompareAndDeleteGlobalVariables(SiriusRuntimeKey, valid))
}

func TestSiriusCompileFastRejections(t *testing.T) {
	require.True(t, siriusStatementEligible(&tree.Select{}))
	require.False(t, siriusStatementEligible(&tree.Select{IsPerform: true}))
	require.False(t, siriusStatementEligible(&tree.Select{Ep: &tree.ExportParam{}}))
	require.False(t, siriusStatementEligible(nil))

	requested := WithSiriusOffload(context.Background())
	for _, c := range []*Compile{
		nil,
		{},
		{isPrepare: true, stmt: &tree.Select{}},
		{isInternal: true, stmt: &tree.Select{}},
		{stmt: &tree.Select{IsPerform: true}},
	} {
		offloaded, err := c.tryCompileSiriusRead(requested, nil)
		require.NoError(t, err)
		require.False(t, offloaded)
	}
	offloaded, err := (&Compile{}).tryCompileSiriusRead(context.Background(), nil)
	require.NoError(t, err)
	require.False(t, offloaded)

	require.NoError(t, (*siriusReadOwner)(nil).finish(context.Background(), false))
	err = (&Compile{}).runSiriusRead(context.Background())
	require.ErrorContains(t, err, "missing Sirius execution owner")
}
