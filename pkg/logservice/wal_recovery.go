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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
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
	walRecoveredTagFilePrefix = "./WAL_RECOVERED"
	walRecoveryStateVersion   = 1
	walRecoveryStateSuffix    = ".restore-state.json"

	logEntrySafeDSNOffset = 50

	walDataFileCountSize       = 4
	walDataFileEntryHeaderSize = 40

	maxWALRecoveryEntries         = 10 * 1000 * 1000
	maxWALRecoveryEntryDataSize   = defaultMaxMessageSize
	maxWALRecoveryPreallocEntries = 64 * 1024
	maxWALRecoveryFileSize        = 10 * defaultMaxMessageSize

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
	Digest  string
}

type walRecoveryState struct {
	Version    int    `json:"version"`
	WALDigest  string `json:"wal_digest"`
	EntryCount uint64 `json:"entry_count"`
	BaseLSN    uint64 `json:"base_lsn"`
	Complete   bool   `json:"complete"`
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
	walRecoveredTagFile := getWALRecoveredTagFile(walDataPath)
	walRecoveryStateFile := getWALRecoveryStateFile(walDataPath)

	// Preserve compatibility with recovery images that only wrote the legacy
	// completion tag. Force must not replay an already completed WAL into a
	// non-empty shard; rebuilding empty LogService storage is the safe retry.
	if _, stateErr := os.Stat(walRecoveryStateFile); os.IsNotExist(stateErr) {
		_, err := fs.StatFile(ctx, walRecoveredTagFile)
		if err == nil {
			logger.Info("WAL recovery: legacy completion tag found, skipping replay",
				zap.Bool("force_ignored", cfg.BootstrapConfig.Restore.Force),
				zap.String("tag_file", walRecoveredTagFile))
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
		if err := completeEmptyWALRecovery(ctx, walRecoveryStateFile, walData.Digest); err != nil {
			return err
		}
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
	if err := s.replayWALEntries(ctx, cfg, walData); err != nil {
		logger.Error("WAL recovery: failed to replay WAL entries", zap.Error(err))
		return err
	}

	// Keep writing the legacy tag so a rollback to an older recovery image
	// cannot replay a WAL that this version already completed.
	if err := fs.Write(ctx, fileservice.IOVector{
		FilePath: walRecoveredTagFile,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(len(walData.Digest)),
				Data:   []byte(walData.Digest),
			},
		},
	}); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
			logger.Info("WAL recovery: recovery tag file already exists")
		} else {
			logger.Warn("WAL recovery: failed to write legacy recovery tag",
				zap.Error(err))
		}
	}

	logger.Info("WAL recovery: completed successfully",
		zap.Int("entries_recovered", len(walData.Entries)))

	return nil
}

func getWALRecoveredTagFile(walDataPath string) string {
	key := filepath.Clean(walDataPath)
	if abs, err := filepath.Abs(walDataPath); err == nil {
		key = abs
	}
	sum := sha256.Sum256([]byte(key))
	return walRecoveredTagFilePrefix + "_" + hex.EncodeToString(sum[:8])
}

func getWALRecoveryStateFile(walDataPath string) string {
	return filepath.Clean(walDataPath) + walRecoveryStateSuffix
}

func readWALRecoveryState(ctx context.Context, path string) (*walRecoveryState, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var state walRecoveryState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, moerr.NewInternalErrorf(ctx,
			"invalid WAL recovery state file %s: %v", path, err)
	}
	if state.Version != walRecoveryStateVersion {
		return nil, moerr.NewInternalErrorf(ctx,
			"unsupported WAL recovery state version %d in %s", state.Version, path)
	}
	return &state, nil
}

func writeWALRecoveryState(ctx context.Context, path string, state walRecoveryState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".wal-restore-state-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	closed = true
	if err := os.Rename(tmpPath, path); err != nil {
		return moerr.NewInternalErrorf(ctx,
			"failed to publish WAL recovery state %s: %v", path, err)
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return err
	}
	return nil
}

func validateWALRecoveryState(
	ctx context.Context,
	state *walRecoveryState,
	digest string,
	entryCount uint64,
) error {
	if state.WALDigest != digest {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery file changed after replay started: state digest %s, current digest %s",
			state.WALDigest, digest)
	}
	if state.EntryCount != entryCount {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry count changed after replay started: state %d, current %d",
			state.EntryCount, entryCount)
	}
	return nil
}

func completeEmptyWALRecovery(ctx context.Context, path, digest string) error {
	state, err := readWALRecoveryState(ctx, path)
	if err != nil {
		return err
	}
	if state == nil {
		state = &walRecoveryState{
			Version:   walRecoveryStateVersion,
			WALDigest: digest,
			Complete:  true,
		}
		return writeWALRecoveryState(ctx, path, *state)
	}
	if err := validateWALRecoveryState(ctx, state, digest, 0); err != nil {
		return err
	}
	if state.Complete {
		return nil
	}
	state.Complete = true
	return writeWALRecoveryState(ctx, path, *state)
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
	if fileInfo.Size() > maxWALRecoveryFileSize {
		return nil, moerr.NewInternalErrorf(ctx,
			"WAL data file size %d exceeds in-memory recovery limit %d",
			fileInfo.Size(), maxWALRecoveryFileSize)
	}
	hasher := sha256.New()
	reader := io.TeeReader(f, hasher)

	// Read entry count
	countBuf := make([]byte, walDataFileCountSize)
	if _, err := io.ReadFull(reader, countBuf); err != nil {
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
		if _, err := io.ReadFull(reader, headerBuf); err != nil {
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
		if _, err := io.ReadFull(reader, entry.RawData); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to read entry %d data: %v", i, err)
		}
		bytesRead += int64(dataLen)

		entries = append(entries, entry)
	}
	if bytesRead < fileInfo.Size() {
		return nil, moerr.NewInternalErrorf(ctx,
			"WAL data file has %d trailing bytes", fileInfo.Size()-bytesRead)
	}

	// Replay must follow the original LogService PSN order. The backup file
	// stores the source raft index, which is the old LogService LSN/PSN.
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].RaftIndex < entries[j].RaftIndex
	})
	normalizeWALEntrySafeDSN(entries)

	return &WALRecoveryData{
		Entries: entries,
		Digest:  hex.EncodeToString(hasher.Sum(nil)),
	}, nil
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

func walRecoveryResumeIndex(
	ctx context.Context,
	state *walRecoveryState,
	walData *WALRecoveryData,
	currentLSN uint64,
) (int, error) {
	entryCount := uint64(len(walData.Entries))
	if err := validateWALRecoveryState(ctx, state, walData.Digest, entryCount); err != nil {
		return 0, err
	}
	if currentLSN < state.BaseLSN {
		return 0, moerr.NewInternalErrorf(ctx,
			"Log shard latest LSN %d is below WAL recovery base LSN %d",
			currentLSN, state.BaseLSN)
	}
	replayed := currentLSN - state.BaseLSN
	if state.Complete {
		if replayed < entryCount {
			return 0, moerr.NewInternalErrorf(ctx,
				"completed WAL recovery is missing entries: destination has %d, expected at least %d",
				replayed, entryCount)
		}
		return len(walData.Entries), nil
	}
	if replayed > entryCount {
		return 0, moerr.NewInternalErrorf(ctx,
			"destination Log shard advanced by %d entries during WAL recovery, expected at most %d",
			replayed, entryCount)
	}
	return int(replayed), nil
}

// replayWALEntries replays WAL entries to the Log shard.
//
// BootstrapHAKeeper elects exactly one configured LogService as the recovery
// coordinator. It may not be the Log shard leader, so WAL replay appends
// through the normal LogService client path. The client uses discovery/service
// addresses to reach the current shard leader instead of proposing through
// this pod's local NodeHost.
func (s *Service) replayWALEntries(ctx context.Context, cfg Config, walData *WALRecoveryData) error {
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
	leaderServiceAddress := ""
	for {
		select {
		case <-ctx.Done():
			return moerr.AttachCause(ctx, ctx.Err())
		default:
		}
		if time.Since(waitStart) > maxWaitTime {
			return moerr.NewInternalErrorf(ctx, "timeout waiting for Log shard %d to be ready", shardID)
		}

		info, ok := s.getShardInfo(shardID)
		if ok && info.LeaderID != 0 {
			logger.Info("WAL recovery: Log shard is ready",
				zap.Uint64("shard_id", shardID),
				zap.Uint64("leader_id", info.LeaderID))
			if err := waitWALRecoveryInterval(ctx, 5*time.Second); err != nil {
				return err
			}

			info2, ok2 := s.getShardInfo(shardID)
			if ok2 && info2.LeaderID != 0 {
				if replicaInfo, ok := info2.Replicas[info2.LeaderID]; ok {
					leaderServiceAddress = replicaInfo.ServiceAddress
				}
				logger.Info("WAL recovery: Log shard leader confirmed",
					zap.Uint64("shard_id", shardID),
					zap.Uint64("leader_id", info2.LeaderID),
					zap.String("leader_service_address", leaderServiceAddress))
				break
			}
			logger.Warn("WAL recovery: Log shard leader lost after wait, retrying...")
			continue
		}

		logger.Info("WAL recovery: Log shard not ready yet, waiting...",
			zap.Uint64("shard_id", shardID),
			zap.Bool("found", ok))
		if err := waitWALRecoveryInterval(ctx, time.Second); err != nil {
			return err
		}
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

	replayClient, err := s.newWALRecoveryClient(ctx, cfg, leaderServiceAddress)
	if err != nil {
		logger.Error("WAL recovery: failed to create replay client", zap.Error(err))
		return err
	}
	defer func() {
		if err := replayClient.close(); err != nil {
			logger.Warn("WAL recovery: failed to close replay client", zap.Error(err))
		}
	}()
	logger.Info("WAL recovery: replay client connected",
		zap.String("address", replayClient.addr),
		zap.String("discovery_address", cfg.HAKeeperClientConfig.DiscoveryAddress),
		zap.Strings("service_addresses", cfg.HAKeeperClientConfig.ServiceAddresses))

	lsnCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)
	currentLSN, err := replayClient.doGetLatestLsn(lsnCtx)
	cancel()
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get Log shard latest LSN: %v", err)
	}

	statePath := getWALRecoveryStateFile(cfg.BootstrapConfig.Restore.WALDataPath)
	recoveryState, err := readWALRecoveryState(ctx, statePath)
	if err != nil {
		return err
	}
	if recoveryState == nil {
		recoveryState = &walRecoveryState{
			Version:    walRecoveryStateVersion,
			WALDigest:  walData.Digest,
			EntryCount: uint64(len(walData.Entries)),
			BaseLSN:    uint64(currentLSN),
		}
		// The recovery state must be durable before the first append. On a
		// restart, the destination LSN then provides the authoritative number
		// of entries already replayed, including a crash immediately after an
		// append but before any local progress update.
		if err := writeWALRecoveryState(ctx, statePath, *recoveryState); err != nil {
			return err
		}
	}
	startIndex, err := walRecoveryResumeIndex(ctx, recoveryState, walData, uint64(currentLSN))
	if err != nil {
		return err
	}
	if recoveryState.Complete {
		logger.Info("WAL recovery: durable completion state found, skipping replay",
			zap.Int("entries", len(walData.Entries)),
			zap.Bool("force_ignored", cfg.BootstrapConfig.Restore.Force))
		return nil
	}
	logger.Info("WAL recovery: resume position",
		zap.Int("completed_entries", startIndex),
		zap.Uint64("base_lsn", recoveryState.BaseLSN),
		zap.Uint64("current_lsn", uint64(currentLSN)))

	for i := startIndex; i < len(walData.Entries); i++ {
		entry := walData.Entries[i]
		if i == startIndex {
			logger.Info("WAL recovery: processing first entry",
				zap.Int("index", i),
				zap.Uint64("dsn", entry.DSN),
				zap.Uint64("safe_dsn", entry.SafeDSN),
				zap.Uint64("raft_index", entry.RaftIndex),
				zap.Int("data_len", len(entry.RawData)))
		}

		cmd := buildUserEntryCmd(leaseHolderID, entry.RawData)
		appendCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)

		if i == startIndex {
			logger.Info("WAL recovery: calling append for first entry",
				zap.Int("cmd_len", len(cmd)))
		}

		lsn, err := replayClient.doAppend(appendCtx, pb.LogRecord{Data: cmd})
		cancel()

		if i == startIndex {
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
		expectedLSN := recoveryState.BaseLSN + uint64(i) + 1
		if uint64(lsn) != expectedLSN {
			return moerr.NewInternalErrorf(ctx,
				"unexpected LSN while replaying WAL entry %d: got %d, expected %d; another writer may be active",
				i, lsn, expectedLSN)
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

	recoveryState.Complete = true
	if err := writeWALRecoveryState(ctx, statePath, *recoveryState); err != nil {
		return err
	}

	logger.Info("WAL recovery: replay completed",
		zap.Int("entries_replayed", len(walData.Entries)-startIndex),
		zap.Int("entries_total", len(walData.Entries)))

	return nil
}

func waitWALRecoveryInterval(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return moerr.AttachCause(ctx, ctx.Err())
	case <-timer.C:
		return nil
	}
}

func (s *Service) newWALRecoveryClient(
	ctx context.Context,
	cfg Config,
	leaderServiceAddress string,
) (*client, error) {
	serviceAddresses := append([]string{}, cfg.HAKeeperClientConfig.ServiceAddresses...)
	if cfg.HAKeeperClientConfig.DiscoveryAddress == "" && leaderServiceAddress != "" {
		serviceAddresses = []string{leaderServiceAddress}
	}
	clientCfg := ClientConfig{
		Tag:              "wal-recovery",
		ReadOnly:         true,
		LogShardID:       firstLogShardID,
		DiscoveryAddress: cfg.HAKeeperClientConfig.DiscoveryAddress,
		ServiceAddresses: serviceAddresses,
		MaxMessageSize:   int(cfg.RPC.MaxMessageSize),
		EnableCompress:   cfg.RPC.EnableCompress,
	}
	connectCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)
	defer cancel()
	return newClient(connectCtx, cfg.UUID, clientCfg)
}
