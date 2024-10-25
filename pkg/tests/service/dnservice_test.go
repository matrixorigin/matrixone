// Copyright 2024 Matrix Origin
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

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
)

func Test_buildTNOptions(t *testing.T) {
	//opts := buildTNOptions(&tnservice.Config{}, nil)
	//for i, opt := range opts {
	//
	//}

	moruntime.SetupServiceBasedRuntime("", moruntime.DefaultRuntime())
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	// initialize cluster
	c, err := NewCluster(ctx, t, DefaultOptions().
		WithCNServiceNum(1).
		WithTNServiceNum(1).
		WithTNShardNum(1))
	require.NoError(t, err)

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())
}
