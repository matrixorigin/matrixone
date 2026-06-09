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
	"container/heap"
	"context"
	"encoding/binary"
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

const (
	walRecoveredTagFile = "./WAL_RECOVERED"

	logEntrySafeDSNOffset = 50

	walDataFileCountSize       = 4
	walDataFileEntryHeaderSize = 40

	maxWALRecoveryEntries         = 10 * 1000 * 1000
	maxWALRecoveryEntryDataSize   = defaultMaxMessageSize
	maxWALRecoveryPreallocEntries = 64 * 1024

	leaseHolderIDSize = 8
)

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
		zap.Uint64("first_raft_index", walData.Entries[0].RaftIndex),
		zap.Uint64("last_raft_index", walData.Entries[len(walData.Entries)-1].RaftIndex),
		zap.Uint64("first_entry_dsn", walData.Entries[0].DSN),
		zap.Uint64("last_entry_dsn", walData.Entries[len(walData.Entries)-1].DSN),
		zap.Uint64("first_entry_safe_dsn", walData.Entries[0].SafeDSN),
		zap.Uint64("last_entry_safe_dsn", walData.Entries[len(walData.Entries)-1].SafeDSN))

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
		if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
			logger.Info("WAL recovery: recovery tag file already exists")
			return nil
		}
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
		return nil, moerr.NewInternalErrorf(ctx, "failed to open WAL data file: %v", err)
	}
	defer f.Close()

	fileInfo, err := f.Stat()
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to stat WAL data file: %v", err)
	}
	if fileInfo.Size() < walDataFileCountSize {
		return nil, moerr.NewInternalErrorf(ctx,
			"WAL data file too small: size %d, need at least %d bytes",
			fileInfo.Size(), walDataFileCountSize)
	}

	// Read entry count
	countBuf := make([]byte, walDataFileCountSize)
	if _, err := io.ReadFull(f, countBuf); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to read entry count: %v", err)
	}
	count := binary.LittleEndian.Uint32(countBuf)
	if count > maxWALRecoveryEntries {
		return nil, moerr.NewInternalErrorf(ctx,
			"WAL data file entry count %d exceeds limit %d",
			count, maxWALRecoveryEntries)
	}
	maxEntriesByFileSize := uint64(fileInfo.Size()-walDataFileCountSize) / walDataFileEntryHeaderSize
	if uint64(count) > maxEntriesByFileSize {
		return nil, moerr.NewInternalErrorf(ctx,
			"WAL data file entry count %d exceeds max possible entries %d for file size %d",
			count, maxEntriesByFileSize, fileInfo.Size())
	}

	logger.Info("WAL recovery: reading entries from file",
		zap.Uint32("count", count),
		zap.String("file", filePath))

	entries := make([]WALEntry, 0, walRecoveryPreallocEntries(count))

	// Read each entry
	headerBuf := make([]byte, walDataFileEntryHeaderSize)
	bytesRead := int64(walDataFileCountSize)
	for i := uint32(0); i < count; i++ {
		// Read header
		if _, err := io.ReadFull(f, headerBuf); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to read entry %d header: %v", i, err)
		}
		bytesRead += walDataFileEntryHeaderSize

		entry := WALEntry{
			DSN:        binary.LittleEndian.Uint64(headerBuf[0:8]),
			SafeDSN:    binary.LittleEndian.Uint64(headerBuf[8:16]),
			RaftIndex:  binary.LittleEndian.Uint64(headerBuf[16:24]),
			RaftTerm:   binary.LittleEndian.Uint64(headerBuf[24:32]),
			EntryCount: binary.LittleEndian.Uint32(headerBuf[32:36]),
			RawData:    nil,
		}
		dataLen := binary.LittleEndian.Uint32(headerBuf[36:40])
		if dataLen > maxWALRecoveryEntryDataSize {
			return nil, moerr.NewInternalErrorf(ctx,
				"WAL data file entry %d data length %d exceeds limit %d",
				i, dataLen, maxWALRecoveryEntryDataSize)
		}
		remaining := fileInfo.Size() - bytesRead
		if remaining < 0 || uint64(dataLen) > uint64(remaining) {
			return nil, moerr.NewInternalErrorf(ctx,
				"WAL data file entry %d data length %d exceeds remaining file bytes %d",
				i, dataLen, remaining)
		}

		// Read raw data
		entry.RawData = make([]byte, int(dataLen))
		if _, err := io.ReadFull(f, entry.RawData); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to read entry %d data: %v", i, err)
		}
		bytesRead += int64(dataLen)

		entries = append(entries, entry)
	}
	if bytesRead < fileInfo.Size() {
		logger.Warn("WAL recovery: WAL data file has trailing bytes",
			zap.Int64("trailing_bytes", fileInfo.Size()-bytesRead),
			zap.String("file", filePath))
	}

	// Replay must follow the original LogService PSN order. The backup file
	// stores the source raft index, which is the old LogService LSN/PSN.
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].RaftIndex < entries[j].RaftIndex
	})
	normalizeWALEntrySafeDSN(entries)

	return &WALRecoveryData{Entries: entries}, nil
}

func walRecoveryPreallocEntries(count uint32) int {
	if count > maxWALRecoveryPreallocEntries {
		return maxWALRecoveryPreallocEntries
	}
	return int(count)
}

func normalizeWALEntrySafeDSN(entries []WALEntry) {
	baseSafeDSN := firstNormalDSN(entries)
	if baseSafeDSN > 0 {
		baseSafeDSN--
	}

	contiguousDSN := baseSafeDSN
	pending := make(dsnIntervalHeap, 0)
	for i := range entries {
		entry := &entries[i]
		if entry.DSN != 0 && entry.EntryCount != 0 {
			start := entry.DSN
			end := entry.DSN + uint64(entry.EntryCount) - 1
			if start <= contiguousDSN+1 {
				if end > contiguousDSN {
					contiguousDSN = end
				}
				contiguousDSN = drainContiguousIntervals(&pending, contiguousDSN)
			} else {
				heap.Push(&pending, dsnInterval{start: start, end: end})
			}
		}

		safeDSN := entry.SafeDSN
		if safeDSN == 0 || safeDSN > contiguousDSN {
			safeDSN = contiguousDSN
		}
		if safeDSN < baseSafeDSN {
			safeDSN = baseSafeDSN
		}
		entry.SafeDSN = safeDSN
		if len(entry.RawData) >= logEntrySafeDSNOffset+8 {
			binary.LittleEndian.PutUint64(entry.RawData[logEntrySafeDSNOffset:], safeDSN)
		}
	}
}

func firstNormalDSN(entries []WALEntry) uint64 {
	for _, entry := range entries {
		if entry.DSN != 0 && entry.EntryCount != 0 {
			return entry.DSN
		}
	}
	return 0
}

func drainContiguousIntervals(pending *dsnIntervalHeap, contiguousDSN uint64) uint64 {
	for pending.Len() > 0 {
		next := (*pending)[0]
		if next.start > contiguousDSN+1 {
			break
		}
		heap.Pop(pending)
		if next.end > contiguousDSN {
			contiguousDSN = next.end
		}
	}
	return contiguousDSN
}

type dsnInterval struct {
	start uint64
	end   uint64
}

type dsnIntervalHeap []dsnInterval

func (h dsnIntervalHeap) Len() int { return len(h) }

func (h dsnIntervalHeap) Less(i, j int) bool {
	if h[i].start != h[j].start {
		return h[i].start < h[j].start
	}
	return h[i].end < h[j].end
}

func (h dsnIntervalHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *dsnIntervalHeap) Push(x any) {
	*h = append(*h, x.(dsnInterval))
}

func (h *dsnIntervalHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// buildUserEntryCmd builds a LogService command for UserEntryUpdate.
// LogService command format:
// - UpdateType: 4 bytes (big-endian uint32) = 2 (UserEntryUpdate)
// - LeaseHolderID: 8 bytes (big-endian uint64)
// - Payload: LogEntry data
func buildUserEntryCmd(leaseHolderID uint64, payload []byte) []byte {
	cmd := make([]byte, headerSize+leaseHolderIDSize+len(payload))
	binaryEnc.PutUint32(cmd[0:4], uint32(pb.UserEntryUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], leaseHolderID)
	copy(cmd[headerSize+leaseHolderIDSize:], payload)
	return cmd
}

// replayWALEntries replays WAL entries to the Log shard
func (s *Service) replayWALEntries(ctx context.Context, walData *WALRecoveryData) error {
	logger := s.runtime.SubLogger(runtime.SystemInit)

	// Get the Log shard ID (first log shard)
	shardID := firstLogShardID

	logger.Info("WAL recovery: starting replay to Log shard",
		zap.Uint64("shard_id", shardID),
		zap.Int("entry_count", len(walData.Entries)))

	// Wait for Log shard to be ready before replaying.
	logger.Info("WAL recovery: waiting for Log shard to be ready",
		zap.Uint64("shard_id", shardID))

	maxWaitTime := 5 * time.Minute
	waitStart := time.Now()
	for {
		if time.Since(waitStart) > maxWaitTime {
			return moerr.NewInternalErrorf(ctx, "timeout waiting for Log shard %d to be ready", shardID)
		}

		info, ok := s.getShardInfo(shardID)
		if ok && info.LeaderID != 0 {
			logger.Info("WAL recovery: Log shard is ready",
				zap.Uint64("shard_id", shardID),
				zap.Uint64("leader_id", info.LeaderID))
			time.Sleep(5 * time.Second)

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
	logger.Info("WAL recovery: starting entry replay loop",
		zap.Int("total_entries", len(walData.Entries)))

	appendCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)
	v, err := s.store.read(appendCtx, shardID, leaseHolderIDQuery{})
	cancel()

	var leaseHolderID uint64
	if err != nil {
		logger.Warn("WAL recovery: failed to get lease holder ID, using 0",
			zap.Error(err))
		leaseHolderID = 0
	} else {
		leaseHolderID = v.(uint64)
		logger.Info("WAL recovery: got current lease holder ID",
			zap.Uint64("lease_holder_id", leaseHolderID))
	}

	for i, entry := range walData.Entries {
		if i == 0 {
			logger.Info("WAL recovery: processing first entry",
				zap.Uint64("dsn", entry.DSN),
				zap.Uint64("safe_dsn", entry.SafeDSN),
				zap.Uint64("raft_index", entry.RaftIndex),
				zap.Int("data_len", len(entry.RawData)))
		}

		cmd := buildUserEntryCmd(leaseHolderID, entry.RawData)
		appendCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)

		if i == 0 {
			logger.Info("WAL recovery: calling append for first entry",
				zap.Int("cmd_len", len(cmd)))
		}

		lsn, err := s.store.append(appendCtx, shardID, cmd)
		cancel()

		if i == 0 {
			logger.Info("WAL recovery: first append completed",
				zap.Uint64("lsn", lsn),
				zap.Error(err))
		}

		if err != nil {
			logger.Error("WAL recovery: failed to append entry",
				zap.Int("index", i),
				zap.Uint64("dsn", entry.DSN),
				zap.Error(err))
			return moerr.NewInternalErrorf(ctx, "failed to append WAL entry %d (DSN=%d): %v", i, entry.DSN, err)
		}

		if i%1000 == 0 || i == len(walData.Entries)-1 {
			logger.Info("WAL recovery: progress",
				zap.Int("current", i+1),
				zap.Int("total", len(walData.Entries)),
				zap.Uint64("dsn", entry.DSN),
				zap.Uint64("safe_dsn", entry.SafeDSN),
				zap.Uint64("raft_index", entry.RaftIndex),
				zap.Uint64("lsn", lsn))
		}
	}

	logger.Info("WAL recovery: replay completed",
		zap.Int("entries_replayed", len(walData.Entries)))

	return nil
}
