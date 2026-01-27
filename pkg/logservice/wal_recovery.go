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

package logservice

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

const walRecoveredTagFile = "./WAL_RECOVERED"

// WALEntry represents a WAL entry read from the recovery file
type WALEntry struct {
	DSN        uint64
	SafeDSN    uint64
	RaftIndex  uint64
	RaftTerm   uint64
	EntryCount uint32
	RawData    []byte
}

// WALRecoveryData contains all WAL entries to be recovered
type WALRecoveryData struct {
	Entries []WALEntry
}

// RecoverWALData recovers WAL data from the specified file during LogService bootstrap.
// This function is called when LogService starts in recovery mode with a WAL data file.
// It reads the WAL entries extracted from the damaged LogService's Raft logs and
// replays them into the new LogService's Log shard.
func (s *Service) RecoverWALData(ctx context.Context, cfg Config) error {
	walDataPath := cfg.BootstrapConfig.Restore.WALDataPath
	if walDataPath == "" {
		return nil
	}

	logger := s.runtime.SubLogger(runtime.SystemInit)
	logger.Info("WAL recovery: checking WAL data file",
		zap.String("path", walDataPath))

	// Check if WAL recovery has already been done
	fs, err := fileservice.Get[fileservice.FileService](s.fileService, defines.LocalFileServiceName)
	if err != nil {
		logger.Error("WAL recovery: failed to get file service", zap.Error(err))
		return err
	}

	// Check if already recovered (unless force is set)
	if !cfg.BootstrapConfig.Restore.Force {
		_, err := fs.StatFile(ctx, walRecoveredTagFile)
		if err == nil {
			logger.Info("WAL recovery: already recovered, skipping")
			return nil
		}
		if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			logger.Error("WAL recovery: failed to check recovery tag file", zap.Error(err))
			return err
		}
	}

	// Read WAL data from file
	walData, err := s.readWALDataFile(ctx, walDataPath)
	if err != nil {
		logger.Error("WAL recovery: failed to read WAL data file",
			zap.String("path", walDataPath),
			zap.Error(err))
		return err
	}

	if len(walData.Entries) == 0 {
		logger.Info("WAL recovery: no entries to recover")
		return nil
	}

	logger.Info("WAL recovery: loaded WAL entries",
		zap.Int("count", len(walData.Entries)),
		zap.Uint64("first_dsn", walData.Entries[0].DSN),
		zap.Uint64("last_dsn", walData.Entries[len(walData.Entries)-1].DSN))

	// Replay WAL entries to Log shard
	if err := s.replayWALEntries(ctx, walData); err != nil {
		logger.Error("WAL recovery: failed to replay WAL entries", zap.Error(err))
		return err
	}

	// Mark recovery as complete
	if err := fs.Write(ctx, fileservice.IOVector{
		FilePath: walRecoveredTagFile,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   1,
				Data:   []byte{1},
			},
		},
	}); err != nil {
		logger.Error("WAL recovery: failed to write recovery tag file", zap.Error(err))
		return err
	}

	logger.Info("WAL recovery: completed successfully",
		zap.Int("entries_recovered", len(walData.Entries)))

	return nil
}

// readWALDataFile reads WAL entries from the binary data file
// File format:
// [count:4][entry1_header:40][entry1_data]...[entryN_header:40][entryN_data]
// Entry header format:
// [DSN:8][SafeDSN:8][RaftIndex:8][RaftTerm:8][EntryCount:4][DataLen:4]
func (s *Service) readWALDataFile(ctx context.Context, filePath string) (*WALRecoveryData, error) {
	logger := s.runtime.SubLogger(runtime.SystemInit)

	// Open the file
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL data file: %w", err)
	}
	defer f.Close()

	// Read entry count
	countBuf := make([]byte, 4)
	if _, err := io.ReadFull(f, countBuf); err != nil {
		return nil, fmt.Errorf("failed to read entry count: %w", err)
	}
	count := binary.LittleEndian.Uint32(countBuf)

	logger.Info("WAL recovery: reading entries from file",
		zap.Uint32("count", count),
		zap.String("file", filePath))

	entries := make([]WALEntry, 0, count)

	// Read each entry
	headerBuf := make([]byte, 40)
	for i := uint32(0); i < count; i++ {
		// Read header
		if _, err := io.ReadFull(f, headerBuf); err != nil {
			return nil, fmt.Errorf("failed to read entry %d header: %w", i, err)
		}

		entry := WALEntry{
			DSN:        binary.LittleEndian.Uint64(headerBuf[0:8]),
			SafeDSN:    binary.LittleEndian.Uint64(headerBuf[8:16]),
			RaftIndex:  binary.LittleEndian.Uint64(headerBuf[16:24]),
			RaftTerm:   binary.LittleEndian.Uint64(headerBuf[24:32]),
			EntryCount: binary.LittleEndian.Uint32(headerBuf[32:36]),
			RawData:    nil,
		}
		dataLen := binary.LittleEndian.Uint32(headerBuf[36:40])

		// Read raw data
		entry.RawData = make([]byte, dataLen)
		if _, err := io.ReadFull(f, entry.RawData); err != nil {
			return nil, fmt.Errorf("failed to read entry %d data: %w", i, err)
		}

		entries = append(entries, entry)
	}

	// Sort entries by DSN to ensure correct replay order
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].DSN < entries[j].DSN
	})

	return &WALRecoveryData{Entries: entries}, nil
}

// replayWALEntries replays WAL entries to the Log shard
func (s *Service) replayWALEntries(ctx context.Context, walData *WALRecoveryData) error {
	logger := s.runtime.SubLogger(runtime.SystemInit)

	// Get the Log shard ID (first log shard)
	shardID := firstLogShardID

	logger.Info("WAL recovery: starting replay to Log shard",
		zap.Uint64("shard_id", shardID),
		zap.Int("entry_count", len(walData.Entries)))

	// Wait for Log shard to be ready before replaying
	// The Log shard is started by HAKeeper after bootstrap, so we need to wait
	logger.Info("WAL recovery: waiting for Log shard to be ready",
		zap.Uint64("shard_id", shardID))

	maxWaitTime := 5 * time.Minute
	waitStart := time.Now()
	for {
		if time.Since(waitStart) > maxWaitTime {
			return fmt.Errorf("timeout waiting for Log shard %d to be ready", shardID)
		}

		info, ok := s.getShardInfo(shardID)
		if ok && info.LeaderID != 0 {
			logger.Info("WAL recovery: Log shard is ready",
				zap.Uint64("shard_id", shardID),
				zap.Uint64("leader_id", info.LeaderID))
			// Wait a bit more to ensure leader election is stable
			time.Sleep(5 * time.Second)
			// Verify again that leader is still there
			info2, ok2 := s.getShardInfo(shardID)
			if ok2 && info2.LeaderID != 0 {
				logger.Info("WAL recovery: Log shard leader confirmed",
					zap.Uint64("shard_id", shardID),
					zap.Uint64("leader_id", info2.LeaderID))
				break
			}
			logger.Warn("WAL recovery: Log shard leader lost after wait, retrying...")
			continue
		}

		logger.Info("WAL recovery: Log shard not ready yet, waiting...",
			zap.Uint64("shard_id", shardID),
			zap.Bool("found", ok))
		time.Sleep(time.Second)
	}

	// Replay each entry
	for i, entry := range walData.Entries {
		// The RawData contains the original LogEntry that was stored in Raft
		// We need to append it to the Log shard
		rec := pb.LogRecord{
			Data: entry.RawData,
		}

		// Create a context with deadline for each append operation
		// dragonboat requires context to have a deadline set
		appendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)

		// Use the store's append method to write to the Log shard
		lsn, err := s.store.append(appendCtx, shardID, rec.Data)
		cancel() // Release resources immediately after use

		if err != nil {
			logger.Error("WAL recovery: failed to append entry",
				zap.Int("index", i),
				zap.Uint64("dsn", entry.DSN),
				zap.Error(err))
			return fmt.Errorf("failed to append WAL entry %d (DSN=%d): %w", i, entry.DSN, err)
		}

		if i%1000 == 0 || i == len(walData.Entries)-1 {
			logger.Info("WAL recovery: progress",
				zap.Int("current", i+1),
				zap.Int("total", len(walData.Entries)),
				zap.Uint64("dsn", entry.DSN),
				zap.Uint64("lsn", lsn))
		}
	}

	logger.Info("WAL recovery: replay completed",
		zap.Int("entries_replayed", len(walData.Entries)))

	return nil
}
