// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func runClientTest(t *testing.T,
	readOnly bool, fn func(*testing.T, LogServiceClientConfig, Client)) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewService(cfg)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service.Close())
	}()

	init := make(map[uint64]string)
	init[2] = service.ID()
	assert.NoError(t, service.store.StartReplica(1, 2, init, false))

	scfg := LogServiceClientConfig{
		ReadOnly:         readOnly,
		ShardID:          1,
		ReplicaID:        2,
		ServiceAddresses: []string{testServiceAddress},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := CreateClient(ctx, "shard1", scfg)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	fn(t, scfg, c)
}

func TestClientCanBeCreated(t *testing.T) {
	fn := func(t *testing.T, cfg LogServiceClientConfig, c Client) {
	}
	runClientTest(t, false, fn)
	runClientTest(t, true, fn)
}

func TestClientAppend(t *testing.T) {
	fn := func(t *testing.T, cfg LogServiceClientConfig, c Client) {
		cmd := make([]byte, 16+headerSize+8)
		cmd = getAppendCmd(cmd, cfg.ReplicaID)
		rand.Read(cmd[headerSize+8:])
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		lsn, err := c.Append(ctx, pb.LogRecord{Data: cmd})
		require.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)

		lsn, err = c.Append(ctx, pb.LogRecord{Data: cmd})
		require.NoError(t, err)
		assert.Equal(t, uint64(5), lsn)

		cmd = getAppendCmd(cmd, cfg.ReplicaID+1)
		_, err = c.Append(ctx, pb.LogRecord{Data: cmd})
		assert.Equal(t, ErrNotLeaseHolder, err)
	}
	runClientTest(t, false, fn)
}

func TestClientAppendAlloc(t *testing.T) {
	fn := func(t *testing.T, cfg LogServiceClientConfig, c Client) {
		cmd := make([]byte, 16+headerSize+8)
		cmd = getAppendCmd(cmd, cfg.ReplicaID)
		rand.Read(cmd[headerSize+8:])
		ac := testing.AllocsPerRun(1000, func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := c.Append(ctx, pb.LogRecord{Data: cmd})
			require.NoError(t, err)
		})
		plog.Infof("ac: %f", ac)
	}
	runClientTest(t, false, fn)
}

func TestClientRead(t *testing.T) {
	fn := func(t *testing.T, cfg LogServiceClientConfig, c Client) {
		cmd := make([]byte, 16+headerSize+8)
		cmd = getAppendCmd(cmd, cfg.ReplicaID)
		rand.Read(cmd[headerSize+8:])
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		lsn, err := c.Append(ctx, pb.LogRecord{Data: cmd})
		require.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)

		cmd2 := make([]byte, 16+headerSize+8)
		cmd2 = getAppendCmd(cmd2, cfg.ReplicaID)
		rand.Read(cmd2[headerSize+8:])
		lsn, err = c.Append(ctx, pb.LogRecord{Data: cmd2})
		require.NoError(t, err)
		assert.Equal(t, uint64(5), lsn)

		// FIXME: returned records should contain correct Index value
		recs, lsn, err := c.Read(ctx, 4, math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)
		require.Equal(t, 2, len(recs))
		assert.Equal(t, cmd, recs[0].Data)
		assert.Equal(t, cmd2, recs[1].Data)

		_, _, err = c.Read(ctx, 6, math.MaxUint64)
		assert.Equal(t, ErrOutOfRange, err)
	}
	runClientTest(t, false, fn)
}

func TestClientTruncate(t *testing.T) {
	fn := func(t *testing.T, cfg LogServiceClientConfig, c Client) {
		cmd := make([]byte, 16+headerSize+8)
		cmd = getAppendCmd(cmd, cfg.ReplicaID)
		rand.Read(cmd[headerSize+8:])
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		lsn, err := c.Append(ctx, pb.LogRecord{Data: cmd})
		require.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)

		require.NoError(t, c.Truncate(ctx, 4))
		lsn, err = c.GetTruncatedIndex(ctx)
		assert.NoError(t, err)
		assert.Equal(t, Lsn(4), lsn)

		assert.Equal(t, ErrInvalidTruncateIndex, c.Truncate(ctx, 3))
	}
	runClientTest(t, false, fn)
}

func TestReadOnlyClientRejectWriteRequests(t *testing.T) {
	fn := func(t *testing.T, cfg LogServiceClientConfig, c Client) {
		cmd := make([]byte, 16+headerSize+8)
		cmd = getAppendCmd(cmd, cfg.ReplicaID)
		rand.Read(cmd[headerSize+8:])
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.Append(ctx, pb.LogRecord{Data: cmd})
		require.Equal(t, ErrIncompatibleClient, err)
		require.Equal(t, ErrIncompatibleClient, c.Truncate(ctx, 4))
	}
	runClientTest(t, true, fn)
}
