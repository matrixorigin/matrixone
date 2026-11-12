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
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

var (
	sharedPartitionCluster partitionClusterFixture
	mu                     sync.Mutex
)

type partitionClusterFixture struct {
	once    sync.Once
	cluster embed.Cluster
	err     error
}

func (f *partitionClusterFixture) get(create func() (embed.Cluster, error)) (embed.Cluster, error) {
	f.once.Do(func() { f.cluster, f.err = startPartitionCluster(create) })
	if f.err != nil {
		return nil, f.err
	}
	return f.cluster, nil
}

func startPartitionCluster(create func() (embed.Cluster, error)) (embed.Cluster, error) {
	c, err := create()
	if err != nil {
		return nil, err
	}
	if err = c.Start(); err != nil {
		if closeErr := c.Close(); closeErr != nil {
			return c, errors.Join(err, closeErr)
		}
		return nil, err
	}
	return c, nil
}

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedPartitionCluster.cluster != nil {
		// m.Run has stopped the test alarm. Bound package-owned cleanup too.
		timeout := time.Minute
		if value := flag.Lookup("test.timeout"); value != nil {
			if d, err := time.ParseDuration(value.Value.String()); err == nil && d > 0 && d < timeout {
				timeout = d
			}
		}
		watchdog := time.AfterFunc(timeout, func() {
			debug.SetTraceback("all")
			panic("timed out closing shared partition cluster")
		})
		err := sharedPartitionCluster.cluster.Close()
		watchdog.Stop()
		if err != nil {
			fmt.Fprintf(os.Stderr, "close shared partition cluster: %v\n", err)
			code = 1
		}
	}
	os.Exit(code)
}

func runPartitionTableCreateAndDeleteTestsWithAware(
	t *testing.T,
	prepare func(c embed.Cluster) int32,
	sql string,
	method partition.PartitionMethod,
	validPartition func(idx int, p partition.Partition),
	beforeDrop func(string, string, embed.ServiceOperator, partition.PartitionMetadata),
	afterDrop func(embed.ServiceOperator, partition.PartitionMetadata),
) {
	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			account := prepare(c)

			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabaseWithAccount(t, account, db, cn)

			testutils.ExecSQLWithReadResultAndAccount(
				t,
				account,
				db,
				cn,
				func(i int, s string, r executor.Result) {

				},
				fmt.Sprintf(sql, t.Name()),
			)

			metadata := getMetadata(
				t,
				uint32(account),
				db,
				t.Name(),
				cn,
			)
			require.NotEqual(t, 0, len(metadata.Partitions))
			require.Equal(t, method, metadata.Method)

			var tables []string
			for idx, p := range metadata.Partitions {
				tables = append(tables, p.PartitionTableName)
				require.NotEqual(t, uint64(0), p.PartitionID)
				require.Equal(t, metadata.TableID, p.PrimaryTableID)
				require.Equal(t, uint32(idx), p.Position)
				require.Equal(t, partitionservice.GetPartitionTableName(metadata.TableName, p.Name), p.PartitionTableName)
				validPartition(idx, p)
			}

			beforeDrop(db, t.Name(), cn, metadata)

			testutils.ExecSQLWithAccount(
				t,
				account,
				db,
				cn,
				fmt.Sprintf("drop table %s", t.Name()),
			)
			metadata = getMetadata(
				t,
				uint32(account),
				db,
				t.Name(),
				cn,
			)
			require.Equal(t, partition.PartitionMetadata{}, metadata)

			for _, name := range tables {
				require.False(t, testutils.TableExistsWithAccount(t, account, db, name, cn))
			}

			afterDrop(cn, metadata)
		},
	)
}

func runPartitionTableCreateAndDeleteTests(
	t *testing.T,
	sql string,
	method partition.PartitionMethod,
	validPartition func(idx int, p partition.Partition),
) {
	runPartitionTableCreateAndDeleteTestsWithPrepare(
		t,
		func(c embed.Cluster) int32 { return 0 },
		sql,
		method,
		validPartition,
	)
}

func runPartitionTableCreateAndDeleteTestsWithPrepare(
	t *testing.T,
	prepare func(c embed.Cluster) int32,
	sql string,
	method partition.PartitionMethod,
	validPartition func(idx int, p partition.Partition),
) {
	runPartitionTableCreateAndDeleteTestsWithAware(
		t,
		prepare,
		sql,
		method,
		validPartition,
		func(string, string, embed.ServiceOperator, partition.PartitionMetadata) {},
		func(embed.ServiceOperator, partition.PartitionMetadata) {},
	)
}

func runPartitionClusterTest(
	t *testing.T,
	fn func(embed.Cluster),
) {
	mu.Lock()
	defer mu.Unlock()

	c, err := sharedPartitionCluster.get(func() (embed.Cluster, error) {
		return embed.NewCluster(embed.WithCNCount(3), embed.WithTesting())
	})
	require.NoError(t, err)

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
	fn(c)
}
