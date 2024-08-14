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

package datasync

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

const (
	recordMaxSize = 10 * 1024 * 1024
)

// LogClient is a interface defines the operation of log entries.
type LogClient interface {
	// shardID returns the shardID of this shard.
	getShardID() uint64
	// getLeaderID returns the leader ID of the log shard.
	getLeaderID(ctx context.Context) (uint64, error)
	// write writes data to log service.
	write(ctx context.Context, data []byte) (uint64, error)
	// readEntries reads the log entries from the log storage.
	readEntries(ctx context.Context, lsn uint64) ([]logservice.LogRecord, uint64, error)
	// truncate sets the truncate lsn of this shard.
	truncate(ctx context.Context, lsn uint64) error
	// getTruncateLsn returns the updated lsn of this shard.
	getTruncatedLsn(ctx context.Context) (uint64, error)
	// getLatestLsn returns the latest Lsn of this shard.
	getLatestLsn(ctx context.Context) (uint64, error)
	// setRequiredLsn updates the required Lsn of this shard.
	// The log entries after the required lsn are required by
	// others, means that the log entries before the required
	// lsn have been consumed.
	setRequiredLsn(ctx context.Context, lsn uint64) error
	// getRequiredLsn returns the required lsn of this shard.
	getRequiredLsn(ctx context.Context) (uint64, error)
	// writeWithRetry is same with write(), but retries to call
	// it when there is an error.
	writeWithRetry(ctx context.Context, data []byte, retryTimes int) (uint64, error)
	// getTruncatedLsnWithRetry is same with getTruncatedLsn(),
	// but retries to call it when there is an error.
	getTruncatedLsnWithRetry(ctx context.Context) (uint64, error)
	// setTruncatedLsnWithRetry is same with setTruncatedLsn(),
	// but retries to call it when there is an error.
	setRequiredLsnWithRetry(ctx context.Context, lsn uint64) error
	// getRequiredLsnWithRetry is same with getRequiredLsn(),
	// but retries to call it when there is an error.
	getRequiredLsnWithRetry(ctx context.Context) (uint64, error)
	// getLatestLsnWithRetry is same with getLatestLsn(),
	// but retries to call it when there is an error.
	getLatestLsnWithRetry(ctx context.Context) (uint64, error)
	// close closes the log client.
	close()
}

type logClient struct {
	common
	haKeeperClient logservice.ClusterHAKeeperClient
	client         logservice.StandbyClient
	shardID        uint64
}

func newLogClient(common common, shardID uint64) *logClient {
	return &logClient{
		common:  common,
		shardID: shardID,
	}
}

func (c *logClient) close() {
	if c.haKeeperClient != nil {
		if err := c.haKeeperClient.Close(); err != nil {
			c.log.Error("failed to close HAKeeper client", zap.Error(err))
		}
	}
	if c.client != nil {
		if err := c.client.Close(); err != nil {
			c.log.Error("failed to close LogService client", zap.Error(err))
		}
	}
}

func (c *logClient) prepare(ctx context.Context) error {
	if c.client == nil {
		if c.haKeeperClient == nil {
			c.haKeeperClient = logservice.NewLogHAKeeperClientWithRetry(
				ctx,
				c.sid,
				c.haKeeperConfig,
			)
		}
		c.client = logservice.NewStandbyClientWithRetry(
			ctx,
			c.sid,
			logservice.ClientConfig{
				Tag:              "datasync",
				ReadOnly:         false,
				LogShardID:       c.shardID,
				DiscoveryAddress: c.common.haKeeperConfig.DiscoveryAddress,
				ServiceAddresses: c.common.haKeeperConfig.ServiceAddresses,
			},
		)
	}
	if c.client == nil {
		return moerr.NewInternalError(ctx, "cannot get log client")
	}
	return nil
}

// getShardID implements the LogClient interface.
func (c *logClient) getShardID() uint64 {
	return c.shardID
}

func (c *logClient) getLeaderID(ctx context.Context) (uint64, error) {
	if err := c.prepare(ctx); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()
	leaderID, err := c.client.GetLeaderID(ctx)
	if err != nil {
		return 0, err
	}
	return leaderID, nil
}

func (c *logClient) write(ctx context.Context, data []byte) (uint64, error) {
	if err := c.prepare(ctx); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()
	return c.client.Append(ctx, pb.LogRecord{Data: data})
}

func (c *logClient) readEntries(ctx context.Context, lsn uint64) ([]logservice.LogRecord, uint64, error) {
	if err := c.prepare(ctx); err != nil {
		return nil, 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()
	return c.client.Read(ctx, lsn, recordMaxSize)
}

func (c *logClient) truncate(ctx context.Context, lsn uint64) error {
	if err := c.prepare(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()
	return c.client.Truncate(ctx, lsn)
}

func (c *logClient) getTruncatedLsn(ctx context.Context) (uint64, error) {
	if err := c.prepare(ctx); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()
	return c.client.GetTruncatedLsn(ctx)
}

func (c *logClient) getLatestLsn(ctx context.Context) (uint64, error) {
	if err := c.prepare(ctx); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()
	return c.client.GetLatestLsn(ctx)
}

func (c *logClient) setRequiredLsn(ctx context.Context, lsn uint64) error {
	if err := c.prepare(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()
	return c.client.SetRequiredLsn(ctx, lsn)
}

func (c *logClient) getRequiredLsn(ctx context.Context) (uint64, error) {
	if err := c.prepare(ctx); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()
	return c.client.GetRequiredLsn(ctx)
}

func (c *logClient) writeWithRetry(ctx context.Context, data []byte, retryTimes int) (uint64, error) {
	lsn, err := fileservice.DoWithRetry("write-data", func() (uint64, error) {
		return c.write(ctx, data)
	}, retryTimes, func(_ error) bool {
		return true
	})
	if err != nil {
		return 0, err
	}
	return lsn, nil
}

func (c *logClient) getTruncatedLsnWithRetry(ctx context.Context) (uint64, error) {
	lsn, err := fileservice.DoWithRetry("get-truncated-lsn", func() (uint64, error) {
		return c.getTruncatedLsn(ctx)
	}, 100, func(_ error) bool {
		return true
	})
	if err != nil {
		return 0, err
	}
	return lsn, nil
}

func (c *logClient) setRequiredLsnWithRetry(ctx context.Context, lsn uint64) error {
	lsn, err := fileservice.DoWithRetry("write-data", func() (uint64, error) {
		return 0, c.setRequiredLsn(ctx, lsn)
	}, 10, func(_ error) bool {
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *logClient) getRequiredLsnWithRetry(ctx context.Context) (uint64, error) {
	lsn, err := fileservice.DoWithRetry("get-required-lsn", func() (uint64, error) {
		return c.getRequiredLsn(ctx)
	}, 100, func(_ error) bool {
		return true
	})
	if err != nil {
		return 0, err
	}
	return lsn, nil
}

func (c *logClient) getLatestLsnWithRetry(ctx context.Context) (uint64, error) {
	lsn, err := fileservice.DoWithRetry("get-latest-lsn", func() (uint64, error) {
		return c.getLatestLsn(ctx)
	}, 100, func(_ error) bool {
		return true
	})
	if err != nil {
		return 0, err
	}
	return lsn, nil
}
