// Copyright 2021 - 2024 Matrix Origin
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

package partition

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

var (
	once            sync.Once
	shardingCluster embed.Cluster
	mu              sync.Mutex
)

func runPartitionClusterTest(
	t *testing.T,
	fn func(embed.Cluster),
) error {
	return runPartitionClusterTestWithReuse(
		t,
		fn,
		true,
	)
}

func runPartitionClusterTestWithReuse(
	t *testing.T,
	fn func(embed.Cluster),
	reuse bool,
) error {
	mu.Lock()
	defer mu.Unlock()

	var err error
	var cluster embed.Cluster
	var c embed.Cluster
	createFunc := func() {
		c, err = embed.NewCluster(
			embed.WithCNCount(3),
			embed.WithTesting(),
			embed.WithPreStart(
				func(op embed.ServiceOperator) {
					op.Adjust(
						func(sc *embed.ServiceConfig) {
							if op.ServiceType() == metadata.ServiceType_CN {
								sc.CN.PartitionService.Enable = true
							}
						},
					)
				},
			),
		)
		if err != nil {
			return
		}
		err = c.Start()
		if err != nil {
			return
		}
		cluster = c
		if reuse {
			shardingCluster = cluster
		}
	}

	if err != nil {
		return err
	}

	if reuse {
		once.Do(createFunc)
		cluster = shardingCluster
	} else {
		createFunc()
	}

	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	if !testutils.TableExists(t, catalog.MO_CATALOG, catalog.MOPartitionMetadata, cn) {
		testutils.ExecSQL(
			t,
			catalog.MO_CATALOG,
			cn,
			partitionservice.InitSQLs...,
		)
	}
	fn(cluster)
	return nil
}
