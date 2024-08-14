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
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/backup"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

const (
	locationSeparator             = ";"
	locationVersionSeparator      = ":"
	defaultCleanStateInterval     = time.Second * 3
	defaultWaitPermissionInterval = time.Second * 3
	defaultLoopWorkInterval       = time.Second * 5
)

func parseUpstreamLsn(cmd []byte) uint64 {
	return binaryEnc.Uint64(cmd[headerSize:])
}

type consumerOption func(*consumer)

func withLogClient(l LogClient) consumerOption {
	return func(c *consumer) {
		c.logClient = l
	}
}

func withUpstreamLogClient(l LogClient) consumerOption {
	return func(c *consumer) {
		c.upstreamLogClient = l
	}
}

func withTxnClient(l TxnClient) consumerOption {
	return func(c *consumer) {
		c.txnClient = l
	}
}

func withWaitPermissionInterval(v time.Duration) consumerOption {
	return func(c *consumer) {
		c.waitPermissionInterval = v
	}
}

type consumer struct {
	common
	waitPermissionInterval time.Duration
	cleanStateInterval     time.Duration
	loopWorkInterval       time.Duration

	// srcFS is the file-service instance of the active MO cluster.
	srcFS fileservice.FileService
	// dstFS is the file-service instance of the standby MO cluster.
	dstFS fileservice.FileService
	// logClient is the client of logservice service.
	logClient LogClient
	// upstreamLogClient is the client of upstream shard's logservice client.
	upstreamLogClient LogClient
	// txnClient is the client of TN txn service, which is used to
	// get checkpoints from TN.
	txnClient TxnClient
	// writeLsn is the latest written LSN.
	writeLsn *atomic.Uint64
	// syncedLsn is the synced LSN.
	syncedLsn *atomic.Uint64
	// lastRequiredLsn is the required LSN that was set last time.
	lastRequiredLsn uint64
	// jobScheduler is the job scheduler to schedule and execute copy job.
	jobScheduler tasks.JobScheduler
}

func newConsumer(
	common common,
	fs fileservice.FileService,
	writeLsn *atomic.Uint64,
	syncedLsn *atomic.Uint64,
	opts ...consumerOption,
) Worker {
	c := &consumer{
		common:    common,
		writeLsn:  writeLsn,
		syncedLsn: syncedLsn,
	}
	srcFS, err := fileservice.Get[fileservice.FileService](
		fs, defines.SharedFileServiceName,
	)
	if err != nil {
		c.log.Error("failed to create source fileservice", zap.Error(err))
		return nil
	}
	c.srcFS = srcFS

	dstFS, err := fileservice.Get[fileservice.FileService](
		fs, defines.StandbyFileServiceName,
	)
	if err != nil {
		c.log.Error("failed to create destination fileservice", zap.Error(err))
		return nil
	}
	c.dstFS = dstFS

	for _, opt := range opts {
		opt(c)
	}

	// fill the necessary fields.
	c.fill(common)

	// start the job scheduler.
	c.jobScheduler = tasks.NewParallelJobScheduler(runtime.GOMAXPROCS(0) * 4)

	return c
}

func (c *consumer) fill(common common) {
	if c.logClient == nil {
		c.logClient = newLogClient(common, logShardID)
	}
	if c.upstreamLogClient == nil {
		c.upstreamLogClient = newLogClient(common, upstreamLogShardID)
	}
	if c.txnClient == nil {
		c.txnClient = newTxnClient(common, nil)
	}
	if c.waitPermissionInterval == 0 {
		c.waitPermissionInterval = defaultWaitPermissionInterval
	}
	if c.cleanStateInterval == 0 {
		c.cleanStateInterval = defaultCleanStateInterval
	}
	if c.loopWorkInterval == 0 {
		c.loopWorkInterval = defaultLoopWorkInterval
	}
}

// Start implements the Worker interface.
func (c *consumer) Start(ctx context.Context) {
	for {
		// wait util I am the leader.
		if err := c.waitPermission(ctx, c.waitPermissionInterval); err != nil {
			if !errors.Is(err, context.Canceled) {
				c.log.Error("wait permission error", zap.Error(err))
			}
			return
		}

		// Initialize the consumer.
		if err := c.init(ctx); err != nil {
			c.log.Error("consumer failed to init", zap.Error(err))

			// init failed, fall back to the original state and try again.
			c.cleanState(ctx, c.cleanStateInterval)
			continue
		}

		// Consume the log entries in datasync shard continuously.
		if err := c.loop(ctx, c.loopWorkInterval); err == nil {
			// role has changed, cannot do the data sync stuff.
			continue
		}

		// Hha, it is context error.
		if ctx.Err() != nil {
			return
		}

		// If the loop return, means something BAD happened, and we have
		// to restart the process all over, and the old data in WAL is useless
		// and could be removed.
		c.cleanState(ctx, c.cleanStateInterval)
	}
}

// Close implement the Worker interface.
func (c *consumer) Close() {
	c.logClient.close()
	c.txnClient.close()
	c.jobScheduler.Stop()
}

// waitPermission waits for the permission to do the data sync.
// only leader can do the data sync.
func (c *consumer) waitPermission(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if c.checkRole(ctx) {
				return nil
			}
		}
	}
}

func (c *consumer) init(ctx context.Context) error {
	// TODO(volgariver6): pause gc.

	if err := c.initSyncedLsn(ctx); err != nil {
		return err
	}
	if err := c.completeData(ctx); err != nil {
		return err
	}
	c.log.Info("consumer initialized")

	// TODO(volgariver6): resume gc.
	return nil
}

func (c *consumer) initSyncedLsn(ctx context.Context) error {
	lsn, err := c.logClient.getTruncatedLsnWithRetry(ctx)
	if err != nil {
		c.log.Error(fmt.Sprintf("failed to update truncated LSN: %v", err))
		return err
	}
	c.log.Info("initialized synced lsn", zap.Uint64("lsn", lsn))
	c.syncedLsn.Store(lsn)
	return nil
}

// completeData completes the data in storage and in WAL which may be missed.
func (c *consumer) completeData(ctx context.Context) error {
	// latestLsn the latest lsn of upstream shard.
	latestLsnUpstream, err := c.upstreamLogClient.getLatestLsnWithRetry(ctx)
	if err != nil {
		c.log.Error("failed to get latest lsn", zap.Error(err))
		return err
	}
	c.log.Info("latest upstream lsn", zap.Uint64("lsn", latestLsnUpstream))

	// requiredLsn is the lsn of upstream shard that have been synced.
	// we should read the left entries from this lsn to the latest lsn.
	requiredLsn, err := c.logClient.getRequiredLsnWithRetry(ctx)
	if err != nil {
		c.log.Error(fmt.Sprintf("failed to get upstream LSN: %v", err))
		return err
	}
	c.log.Info("required lsn for upstream shard", zap.Uint64("lsn", requiredLsn))

	var readFrom uint64

	// If the upstream LSN is 0, means it is the first time to sync data.
	// In this case, we should first check the latest checkpoint, and then:
	//   1. If there is any checkpoint, begin to sync the full data by the
	//      latest checkpoint. After it is finished, set the required LSN
	//      to the truncated_LSN+1 which is kept in the last checkpoint entry.
	//      then begin to read entries from the truncated LSN to the last one.
	//   2. If there is no checkpoint, begin to read entries from position 1
	//      and consume them until we got the last one.

	if requiredLsn == 0 {
		// The entries should start from the next lsn if it is zero.
		requiredLsn = 1

		// no data have been synced, all entries are required, so set the required LSN
		// to 1 to prevent truncation.
		if err := c.logClient.setRequiredLsnWithRetry(ctx, requiredLsn); err != nil {
			c.log.Error("failed to set required lsn for the first time", zap.Error(err))
			return err
		}

		ckp, err := c.txnClient.getLatestCheckpoint(ctx)
		if err != nil {
			c.log.Error("failed to get checkpoint", zap.Error(err))
			return err
		}

		// There is no checkpoint yet.
		if ckp.Location == "" {
			readFrom = requiredLsn
		} else {
			// full sync, this may take long time.
			if err := c.fullSync(ctx, ckp.Location); err != nil {
				c.log.Error("failed to fully sync object files", zap.Error(err))
				return err
			}

			requiredLsn = ckp.TruncateLsn + 1
			if err := c.logClient.setRequiredLsnWithRetry(ctx, requiredLsn); err != nil {
				c.log.Error("failed to set required lsn for the first time", zap.Error(err))
				return err
			}
			readFrom = requiredLsn
		}
	} else {
		readFrom = requiredLsn
	}

	for {
		// read the log entries from upstream to complete the missing entries.
		entries, next, err := c.upstreamLogClient.readEntries(ctx, readFrom)
		if err != nil {
			c.log.Error("failed to read entries", zap.Error(err))
			return err
		}
		if err := c.consumerEntries(ctx, entries, true); err != nil {
			c.log.Error("failed to consumer entries", zap.Error(err))
			return err
		}
		if next > latestLsnUpstream || next == readFrom {
			c.log.Info("finished the entries completion",
				zap.Uint64("next", next),
				zap.Uint64("latest upstream Lsn", latestLsnUpstream),
				zap.Uint64("readFrom", readFrom),
			)
			return nil
		}
		readFrom = next
	}
}

func (c *consumer) cleanState(ctx context.Context, interval time.Duration) {
	var count int
	for {
		count++
		if count > 1 {
			time.Sleep(interval)
		}
		c.log.Info("start to clean state", zap.Int("count", count))

		// First, reset the synced LSN. Fetch the latest lsn and set it to
		// syncedLsn, then the truncation will truncate the log entries before
		// it because they are useless.
		latestLsn, err := c.logClient.getLatestLsnWithRetry(ctx)
		if err != nil {
			c.log.Error("failed to get latest LSN", zap.Error(err))
			continue
		}
		c.syncedLsn.Store(latestLsn)

		// Second, set required LSN to 0.
		if err := c.logClient.setRequiredLsnWithRetry(ctx, 0); err != nil {
			c.log.Error("failed to set required LSN", zap.Error(err))
			continue
		}

		c.log.Info("end of cleaning state", zap.Int("count", count))
		return
	}
}

func (c *consumer) loop(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if !c.checkRole(ctx) {
				return nil
			}
			if c.writeLsn.Load() <= c.syncedLsn.Load() {
				continue
			}
			entries, _, err := c.logClient.readEntries(ctx, c.syncedLsn.Load()+1)
			if err != nil {
				c.log.Error("failed to read entries", zap.Error(err))
				continue
			}
			if err := c.consumerEntries(ctx, entries, false); err != nil {
				c.log.Error("failed to consume entries", zap.Error(err))

				// If the error is ErrFileNotFound, probably means that the file in
				// source file service has been GCed.
				if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
					c.log.Error("file not found, maybe has been GC, restart consumer")
					return err
				}
			}
		}
	}
}

// checkRole checks if current replica is leader, as only leader can do the
// data sync stuff. returns true means it is leader, false otherwise.
func (c *consumer) checkRole(ctx context.Context) bool {
	leaderID, err := c.logClient.getLeaderID(ctx)
	if err != nil {
		c.log.Error("failed to get leader ID", zap.Error(err))
		return false
	}
	if leaderID == 0 {
		c.log.Error("cannot get correct leader ID, it should not be 0")
		return false
	}
	v, ok := c.common.shardReplicaID.Load(c.logClient.getShardID())
	if !ok {
		c.log.Debug("shard not on the store",
			zap.Uint64("shardID", c.logClient.getShardID()),
		)
		return false
	}
	return v.(uint64) == leaderID
}

func (c *consumer) consumerEntries(
	ctx context.Context, entries []logservice.LogRecord, upstream bool,
) error {
	for _, entry := range entries {
		if err := c.consume(ctx, entry, upstream); err != nil {
			return err
		}
	}
	if upstream {
		// update the last required Lsn according to the last entry.
		if len(entries) > 0 {
			if err := c.logClient.setRequiredLsn(ctx, entries[len(entries)-1].Lsn); err != nil {
				return err
			}
			c.lastRequiredLsn = entries[len(entries)-1].Lsn + 1
		}
		return nil
	}
	// after consume all entries, update the upstream Lsn.
	count := len(entries)
	for count > 0 {
		entry := entries[count-1]
		locations := getLocations(entry)
		if len(locations) == 0 {
			count--
			continue
		}
		requiredLsn := parseUpstreamLsn(entry.Data) + 1
		if requiredLsn <= c.lastRequiredLsn {
			return nil
		}
		if err := c.logClient.setRequiredLsn(ctx, requiredLsn); err != nil {
			return err
		}
		c.lastRequiredLsn = requiredLsn
		return nil
	}
	return nil
}

// consume consumes the log entries from current log shard or upstream log shard.
func (c *consumer) consume(ctx context.Context, rec logservice.LogRecord, upstream bool) error {
	if !upstream && rec.Lsn <= c.syncedLsn.Load() { // the record has been synced.
		return nil
	}
	if len(rec.Data) == 0 {
		return nil
	}
	locations := getLocations(rec)
	if len(locations) == 0 {
		c.log.Debug("not a file location cmd", zap.Uint64("LSN", rec.Lsn))
		return nil
	}

	if err := c.copyFiles(ctx, locations, ""); err != nil {
		return err
	}

	// update the LSN
	if !upstream && rec.Lsn > c.syncedLsn.Load() {
		c.syncedLsn.Store(rec.Lsn)
	}
	return nil
}

// parseCheckpointLocations parses the checkpoint str and returns the object files
// name at last.
func (c *consumer) parseCheckpointLocations(ctx context.Context, locationStr string) ([]string, error) {
	var locations []string
	for _, loc := range strings.Split(locationStr, locationSeparator) {
		if len(loc) == 0 { // empty location string, ignore it.
			continue
		}
		parts := strings.Split(loc, locationVersionSeparator)
		if len(parts) != 2 { // invalid location, ignore it.
			continue
		}
		version, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return nil, err
		}
		ckpLocation, err := blockio.EncodeLocationFromString(parts[0])
		if err != nil {
			return nil, err
		}
		var ts types.TS
		objLocations, data, err := logtail.LoadCheckpointEntriesFromKey(
			ctx,
			c.sid,
			c.srcFS,
			ckpLocation,
			uint32(version),
			nil,
			&ts,
		)
		if err != nil {
			return nil, err
		}
		data.Close()
		for _, name := range objLocations {
			locations = append(locations, name.Location.String())
		}
	}
	return locations, nil
}

// fullSync syncs all the object files according to the checkpoint str.
func (c *consumer) fullSync(ctx context.Context, ckpLocationStr string) error {
	locations, err := c.parseCheckpointLocations(ctx, ckpLocationStr)
	if err != nil {
		return err
	}
	c.log.Info("before full sync", zap.Int("all count", len(locations)))
	defer func() {
		c.log.Info("full sync done")
	}()
	return c.copyFiles(ctx, locations, "")
}

func (c *consumer) copyFiles(ctx context.Context, locations []string, dstDir string) error {
	syncJobs := make([]*tasks.Job, len(locations))
	for i, location := range locations {
		job := new(tasks.Job)
		job.Init(ctx, location, tasks.JTAny,
			func(ctx context.Context) *tasks.JobResult {
				var locStr string
				if len(strings.Split(location, "_")) == 8 {
					loc, err := blockio.EncodeLocationFromString(location)
					if err != nil {
						c.log.Error("failed to encode location",
							zap.String("meta loc", location),
							zap.Error(err),
						)
						return &tasks.JobResult{Err: err}
					}
					locStr = loc.Name().String()
				} else {
					locStr = location
				}
				if err := c.copyFile(ctx, locStr, dstDir); err != nil {
					c.log.Error("failed to sync object",
						zap.String("name", locStr),
						zap.Error(err),
					)
					return &tasks.JobResult{Err: err}
				}
				return &tasks.JobResult{}
			},
		)

		// record the job to wait for it done.
		syncJobs[i] = job

		// put the job into the scheduler.
		if err := c.jobScheduler.Schedule(job); err != nil {
			job.DoneWithErr(err)
			c.log.Error("failed to schedule sync job", zap.Error(err))
			return err
		}
	}

	// Wait for all jobs to be done.
	for i, job := range syncJobs {
		res := job.WaitDone()
		if res != nil && res.Err != nil {
			c.log.Error("failed to sync object",
				zap.String("location", locations[i]),
				zap.Error(res.Err),
			)
			return res.Err
		}
	}

	return nil
}

// copyFile copies the file with the retry operation.
func (c *consumer) copyFile(ctx context.Context, name string, dstDir string) error {
	// TODO(volgariver6): checksum
	_, err := backup.CopyFileWithRetry(ctx, c.srcFS, c.dstFS, name, dstDir)
	if err != nil {
		// Ignore the error that the file already exists in destination.
		if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
			return nil
		}
		return err
	}
	return nil
}
