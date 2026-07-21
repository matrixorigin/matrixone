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
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/google/btree"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

const (
	walRecoveredTagFilePrefix = "./WAL_RECOVERED"
	walRecoveryStateVersion   = 3
	walRecoveryStateSuffix    = ".restore-state.json"

	logEntryTypeOffset       = 0
	logEntryVersionOffset    = 2
	logEntryCommandOffset    = 4
	logEntryCountOffset      = 38
	logEntryStartDSNOffset   = 42
	logEntrySafeDSNOffset    = 50
	logEntryFooterOffset     = 58
	logEntryHeaderSize       = 62
	logEntryFooterRecordSize = 8
	logEntryTypeWALRecord    = 1000
	logEntryVersionV2        = 2
	logEntryCommandNormal    = 1
	logEntryCommandSkipDSN   = 2

	tnEntryDSNSize           = 8
	tnEntryDescriptorSize    = 12
	tnEntryMinSize           = tnEntryDSNSize + tnEntryDescriptorSize
	tnEntryTypeOffset        = tnEntryDSNSize
	tnEntryVersionOffset     = tnEntryTypeOffset + 2
	tnEntryPayloadSizeOffset = tnEntryVersionOffset + 2
	tnEntryInfoSizeOffset    = tnEntryPayloadSizeOffset + 4
	tnEntryInfoOffset        = tnEntryInfoSizeOffset + 4
	tnEntryMinType           = 2001
	tnEntryMaxType           = 2007
	tnEntryVersionV1         = 1
	tnEntryInfoHeaderSize    = 28

	walDataFileCountSize       = 4
	walDataFileEntryHeaderSize = 40

	maxWALRecoveryEntries         = 1000 * 1000
	maxWALRecoveryEntryDataSize   = defaultMaxMessageSize
	maxWALRecoveryPreallocEntries = 64 * 1024
	// Recovery files contain many RPC-sized entries and commonly reach
	// multiple GiB. Payloads are streamed, so cap total operator input without
	// tying the file limit to a single RPC message.
	maxWALRecoveryFileSize        = 10 << 30
	walRecoveryRPCEnvelopeReserve = 256
	walRecoveryCheckpointEntries  = 100

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
	dataOffset int64
	dataLen    uint32
	dataDigest [sha256.Size]byte
	skip       *walRecoverySkipSemantics
}

type walRecoverySkipReference struct {
	dsn uint64
	psn uint64
}

type walRecoverySkipSemantics struct {
	references []walRecoverySkipReference
}

type walRecoveryDSNRange struct {
	start     uint64
	end       uint64
	raftIndex uint64
}

// WALRecoveryData contains all WAL entries to be recovered
type WALRecoveryData struct {
	Entries []WALEntry
	Digest  string
	Path    string
}

type walRecoveryState struct {
	Version               int    `json:"version"`
	WALDigest             string `json:"wal_digest"`
	EntryCount            uint64 `json:"entry_count"`
	BaseLSN               uint64 `json:"base_lsn"`
	CompletedEntries      uint64 `json:"completed_entries"`
	LastLSN               uint64 `json:"last_lsn"`
	Complete              bool   `json:"complete"`
	OriginalLeaseHolderID uint64 `json:"original_lease_holder_id"`
}

// RecoverWALData recovers WAL data from the specified file during LogService bootstrap.
// This function is called when LogService starts in recovery mode with a WAL data file.
// It reads the WAL entries extracted from the damaged LogService's Raft logs and
// replays them into the new LogService's Log shard.
func (s *Service) RecoverWALData(ctx context.Context, cfg Config) error {
	return s.recoverWALData(ctx, cfg, false)
}

func (s *Service) recoverWALData(
	ctx context.Context,
	cfg Config,
	requireCompletedDestination bool,
) error {
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
	if _, err := fs.StatFile(ctx, walRecoveredTagFile); err == nil {
		// Legacy tags are artifact-local and are not bound to a destination
		// shard. The replicated Log RSM marker below is the sole completion
		// authority; this tag is retained only for rollback compatibility.
		logger.Warn("WAL recovery: legacy completion tag found; validating destination marker",
			zap.String("tag_file", walRecoveredTagFile))
	} else if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		logger.Error("WAL recovery: failed to check recovery tag file", zap.Error(err))
		return err
	}

	maxEntryDataSize, err := walRecoveryMaxEntryDataSize(ctx, cfg)
	if err != nil {
		return err
	}

	// Read WAL metadata from the file. Entry payloads are streamed during
	// replay so a large recovery file does not have to fit in process memory.
	walData, err := s.readWALDataFileWithLimit(ctx, walDataPath, maxEntryDataSize)
	if err != nil {
		logger.Error("WAL recovery: failed to read WAL data file",
			zap.String("path", walDataPath),
			zap.Error(err))
		return err
	}

	if len(walData.Entries) == 0 {
		logger.Info("WAL recovery: no entries to replay; validating empty destination")
	} else {
		logger.Info("WAL recovery: loaded WAL entries",
			zap.Int("count", len(walData.Entries)),
			zap.Uint64("first_raft_index", walData.Entries[0].RaftIndex),
			zap.Uint64("last_raft_index", walData.Entries[len(walData.Entries)-1].RaftIndex),
			zap.Uint64("first_entry_dsn", walData.Entries[0].DSN),
			zap.Uint64("last_entry_dsn", walData.Entries[len(walData.Entries)-1].DSN),
			zap.Uint64("first_entry_safe_dsn", walData.Entries[0].SafeDSN),
			zap.Uint64("last_entry_safe_dsn", walData.Entries[len(walData.Entries)-1].SafeDSN))
	}

	// Replay WAL entries to Log shard
	if err := s.replayWALEntries(ctx, cfg, walData, requireCompletedDestination); err != nil {
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
	if state.CompletedEntries > state.EntryCount {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery state has %d completed entries, more than source entry count %d",
			state.CompletedEntries, state.EntryCount)
	}
	if state.LastLSN < state.BaseLSN {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery last LSN %d is below base LSN %d",
			state.LastLSN, state.BaseLSN)
	}
	if state.Complete && state.CompletedEntries != state.EntryCount {
		return moerr.NewInternalErrorf(ctx,
			"completed WAL recovery has %d completed entries, expected %d",
			state.CompletedEntries, state.EntryCount)
	}
	return nil
}

func walRecoveryMaxEntryDataSize(ctx context.Context, cfg Config) (uint32, error) {
	maxMessageSize := uint64(cfg.RPC.MaxMessageSize)
	template := RPCRequest{Request: pb.Request{
		RequestID: math.MaxUint64,
		Method:    pb.APPEND,
		LogRequest: pb.LogRequest{
			ShardID: math.MaxUint64,
		},
	}}
	// The RPC codec applies MaxMessageSize to RPCRequest. ProtoSize includes
	// the protobuf envelope and payload; use maximum-width request fields so
	// the remaining bytes are a conservative bound for the raw WAL payload.
	// Keep a small additional reserve for the READ response record envelope.
	overhead := uint64(template.ProtoSize() + headerSize + leaseHolderIDSize + walRecoveryRPCEnvelopeReserve)
	if maxMessageSize <= overhead {
		return 0, moerr.NewInternalErrorf(ctx,
			"LogService RPC max message size %d is too small for WAL recovery overhead %d",
			maxMessageSize, overhead)
	}
	limit := maxMessageSize - overhead
	if limit > maxWALRecoveryEntryDataSize {
		limit = maxWALRecoveryEntryDataSize
	}
	return uint32(limit), nil
}

// readWALDataFile reads WAL entries from the binary data file
// File format:
// [count:4][entry1_header:40][entry1_data]...[entryN_header:40][entryN_data]
// Entry header format:
// [DSN:8][SafeDSN:8][RaftIndex:8][RaftTerm:8][EntryCount:4][DataLen:4]
func (s *Service) readWALDataFile(ctx context.Context, filePath string) (*WALRecoveryData, error) {
	return s.readWALDataFileWithLimit(ctx, filePath, maxWALRecoveryEntryDataSize)
}

func (s *Service) readWALDataFileWithLimit(
	ctx context.Context,
	filePath string,
	maxEntryDataSize uint32,
) (*WALRecoveryData, error) {
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
			"WAL data file size %d exceeds recovery limit %d",
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
		}
		dataLen := binary.LittleEndian.Uint32(headerBuf[36:40])
		if dataLen > maxEntryDataSize {
			return nil, moerr.NewInternalErrorf(ctx,
				"WAL data file entry %d data length %d exceeds configured RPC payload limit %d",
				i, dataLen, maxEntryDataSize)
		}
		remaining := fileInfo.Size() - bytesRead
		if remaining < 0 || uint64(dataLen) > uint64(remaining) {
			return nil, moerr.NewInternalErrorf(ctx,
				"WAL data file entry %d data length %d exceeds remaining file bytes %d",
				i, dataLen, remaining)
		}

		entry.dataOffset = bytesRead
		entry.dataLen = dataLen
		data := make([]byte, int(dataLen))
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to read entry %d data: %v", i, err)
		}
		entry.dataDigest = sha256.Sum256(data)
		if err := validateWALLogEntry(ctx, int(i), entry, data); err != nil {
			return nil, err
		}
		if binary.LittleEndian.Uint16(data[logEntryCommandOffset:]) == logEntryCommandSkipDSN {
			references, err := parseWALRecoverySkipReferences(ctx, int(i), data)
			if err != nil {
				return nil, err
			}
			entry.skip = &walRecoverySkipSemantics{references: references}
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
	if err := validateWALRecoveryEntries(ctx, entries); err != nil {
		return nil, err
	}
	normalizeWALEntrySafeDSN(entries)

	return &WALRecoveryData{
		Entries: entries,
		Digest:  hex.EncodeToString(hasher.Sum(nil)),
		Path:    filePath,
	}, nil
}

func loadWALRecoveryEntryData(
	ctx context.Context,
	walData *WALRecoveryData,
	file *os.File,
	index int,
) ([]byte, error) {
	if index < 0 || index >= len(walData.Entries) {
		return nil, moerr.NewInternalErrorf(ctx,
			"WAL recovery entry index %d is out of range", index)
	}
	entry := walData.Entries[index]
	var data []byte
	if entry.RawData != nil {
		data = bytes.Clone(entry.RawData)
	} else {
		if file == nil {
			return nil, moerr.NewInternalError(ctx,
				"WAL recovery data file is not open")
		}
		data = make([]byte, int(entry.dataLen))
		if _, err := file.ReadAt(data, entry.dataOffset); err != nil {
			return nil, moerr.NewInternalErrorf(ctx,
				"failed to read WAL recovery entry %d data: %v", index, err)
		}
	}
	expectedDigest := entry.dataDigest
	if entry.RawData != nil && expectedDigest == ([sha256.Size]byte{}) {
		// In-memory WAL data is used by focused recovery helpers and tests. A
		// file-backed recovery always records the digest during the initial scan.
		expectedDigest = sha256.Sum256(entry.RawData)
	}
	if digest := sha256.Sum256(data); digest != expectedDigest {
		return nil, moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d changed after validation", index)
	}
	if len(data) >= logEntrySafeDSNOffset+8 {
		binary.LittleEndian.PutUint64(data[logEntrySafeDSNOffset:], entry.SafeDSN)
	}
	return data, nil
}

func verifyWALRecoverySource(ctx context.Context, path, expectedDigest string) error {
	f, err := os.Open(path)
	if err != nil {
		return moerr.NewInternalErrorf(ctx,
			"failed to reopen WAL recovery source for final verification: %v", err)
	}
	defer f.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return moerr.NewInternalErrorf(ctx,
			"failed to verify WAL recovery source: %v", err)
	}
	actual := hex.EncodeToString(hasher.Sum(nil))
	if actual != expectedDigest {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery source changed during replay: expected digest %s, got %s",
			expectedDigest, actual)
	}
	return nil
}

func validateWALRecoveryEntries(ctx context.Context, entries []WALEntry) error {
	activeByDSN := make(map[uint64]walRecoveryDSNRange)
	activeRanges := btree.NewG(32, func(a, b walRecoveryDSNRange) bool {
		return a.start < b.start
	})
	for i := range entries {
		entry := entries[i]
		if entry.RaftIndex == 0 {
			return moerr.NewInternalErrorf(ctx,
				"WAL recovery entry %d has invalid zero raft index", i)
		}
		if i > 0 && entry.RaftIndex <= entries[i-1].RaftIndex {
			return moerr.NewInternalErrorf(ctx,
				"WAL recovery raft indexes are not unique: entry %d has index %d after %d",
				i, entry.RaftIndex, entries[i-1].RaftIndex)
		}
		if entry.EntryCount > 0 && entry.DSN > math.MaxUint64-(uint64(entry.EntryCount)-1) {
			return moerr.NewInternalErrorf(ctx,
				"WAL recovery entry %d DSN range overflows uint64: start %d, count %d",
				i, entry.DSN, entry.EntryCount)
		}

		skip, err := walRecoverySkipReferences(ctx, i, entry)
		if err != nil {
			return err
		}
		if skip != nil {
			seen := make(map[uint64]struct{}, len(skip))
			for _, ref := range skip {
				if _, ok := seen[ref.dsn]; ok {
					return moerr.NewInternalErrorf(ctx,
						"WAL recovery skip entry %d repeats DSN %d", i, ref.dsn)
				}
				seen[ref.dsn] = struct{}{}
				active, ok := activeByDSN[ref.dsn]
				if !ok {
					return moerr.NewInternalErrorf(ctx,
						"WAL recovery skip entry %d references unknown DSN %d", i, ref.dsn)
				}
				if active.raftIndex != ref.psn {
					return moerr.NewInternalErrorf(ctx,
						"WAL recovery skip entry %d DSN %d PSN mismatch: got %d, want %d",
						i, ref.dsn, ref.psn, active.raftIndex)
				}
				delete(activeByDSN, ref.dsn)
				activeRanges.Delete(active)
			}
			continue
		}

		if entry.DSN == 0 || entry.EntryCount == 0 {
			continue
		}
		current := walRecoveryDSNRange{
			start:     entry.DSN,
			end:       entry.DSN + uint64(entry.EntryCount) - 1,
			raftIndex: entry.RaftIndex,
		}
		var overlap walRecoveryDSNRange
		activeRanges.DescendLessOrEqual(current, func(item walRecoveryDSNRange) bool {
			if item.end >= current.start {
				overlap = item
			}
			return false
		})
		if overlap.raftIndex == 0 {
			activeRanges.AscendGreaterOrEqual(current, func(item walRecoveryDSNRange) bool {
				if item.start <= current.end {
					overlap = item
				}
				return false
			})
		}
		if overlap.raftIndex != 0 {
			return moerr.NewInternalErrorf(ctx,
				"WAL recovery entry %d DSN range %d-%d overlaps active DSN range %d-%d from raft index %d",
				i, current.start, current.end, overlap.start, overlap.end, overlap.raftIndex)
		}
		activeByDSN[current.start] = current
		activeRanges.ReplaceOrInsert(current)
	}
	return nil
}

func walRecoverySkipReferences(
	ctx context.Context,
	index int,
	entry WALEntry,
) ([]walRecoverySkipReference, error) {
	if entry.skip != nil {
		return entry.skip.references, nil
	}
	if entry.RawData == nil || len(entry.RawData) < logEntryHeaderSize {
		return nil, nil
	}
	if binary.LittleEndian.Uint16(entry.RawData[logEntryCommandOffset:]) != logEntryCommandSkipDSN {
		return nil, nil
	}
	return parseWALRecoverySkipReferences(ctx, index, entry.RawData)
}

func parseWALRecoverySkipReferences(
	ctx context.Context,
	index int,
	data []byte,
) ([]walRecoverySkipReference, error) {
	if len(data) < logEntryHeaderSize {
		return nil, moerr.NewInternalErrorf(ctx,
			"WAL recovery skip entry %d payload is too small", index)
	}
	footerOffset := uint64(binary.LittleEndian.Uint32(data[logEntryFooterOffset:]))
	if footerOffset < logEntryHeaderSize || footerOffset > uint64(len(data)) {
		return nil, moerr.NewInternalErrorf(ctx,
			"WAL recovery skip entry %d has invalid footer offset", index)
	}
	body := data[logEntryHeaderSize:footerOffset]
	if len(body) == 0 || len(body)%16 != 0 {
		return nil, moerr.NewInternalErrorf(ctx,
			"WAL recovery skip entry %d has invalid command payload", index)
	}
	count := len(body) / 16
	result := make([]walRecoverySkipReference, count)
	psnOffset := count * 8
	for i := 0; i < count; i++ {
		result[i] = walRecoverySkipReference{
			dsn: binary.LittleEndian.Uint64(body[i*8:]),
			psn: binary.LittleEndian.Uint64(body[psnOffset+i*8:]),
		}
	}
	return result, nil
}

func validateWALLogEntry(
	ctx context.Context,
	index int,
	outer WALEntry,
	data []byte,
) error {
	if len(data) < logEntryHeaderSize {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d payload is too small: %d bytes", index, len(data))
	}
	entryType := binary.LittleEndian.Uint16(data[logEntryTypeOffset:])
	version := binary.LittleEndian.Uint16(data[logEntryVersionOffset:])
	command := binary.LittleEndian.Uint16(data[logEntryCommandOffset:])
	if entryType != logEntryTypeWALRecord {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d has invalid LogEntry type %d", index, entryType)
	}
	if version != logEntryVersionV2 {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d has unsupported LogEntry version %d", index, version)
	}
	if command != logEntryCommandNormal && command != logEntryCommandSkipDSN {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d has invalid LogEntry command %d", index, command)
	}

	entryCount := binary.LittleEndian.Uint32(data[logEntryCountOffset:])
	startDSN := binary.LittleEndian.Uint64(data[logEntryStartDSNOffset:])
	if entryCount != outer.EntryCount || startDSN != outer.DSN {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d LogEntry metadata mismatch: outer DSN/count %d/%d, payload %d/%d",
			index, outer.DSN, outer.EntryCount, startDSN, entryCount)
	}
	if entryCount == 0 || (command == logEntryCommandNormal && startDSN == 0) {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d has invalid LogEntry DSN/count %d/%d",
			index, startDSN, entryCount)
	}
	if startDSN > math.MaxUint64-(uint64(entryCount)-1) {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d DSN range overflows uint64: start %d, count %d",
			index, startDSN, entryCount)
	}

	footerOffset := binary.LittleEndian.Uint32(data[logEntryFooterOffset:])
	if footerOffset < logEntryHeaderSize || uint64(footerOffset) > uint64(len(data)) {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d has invalid footer offset %d for payload size %d",
			index, footerOffset, len(data))
	}
	expectedFooterSize := uint64(entryCount) * logEntryFooterRecordSize
	if uint64(len(data))-uint64(footerOffset) != expectedFooterSize {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d footer size does not match entry count %d",
			index, entryCount)
	}

	expectedOffset := uint64(logEntryHeaderSize)
	for i := uint32(0); i < entryCount; i++ {
		pos := uint64(footerOffset) + uint64(i)*logEntryFooterRecordSize
		offset := uint64(binary.LittleEndian.Uint32(data[pos:]))
		length := uint64(binary.LittleEndian.Uint32(data[pos+4:]))
		if length == 0 || offset != expectedOffset || offset+length > uint64(footerOffset) {
			return moerr.NewInternalErrorf(ctx,
				"WAL recovery entry %d has invalid footer record %d: offset %d, length %d",
				index, i, offset, length)
		}
		entryData := data[offset : offset+length]
		if command == logEntryCommandNormal {
			if err := validateWALTNEntry(ctx, index, i, startDSN+uint64(i), entryData); err != nil {
				return err
			}
		} else if entryCount != 1 || len(entryData) == 0 || len(entryData)%16 != 0 {
			return moerr.NewInternalErrorf(ctx,
				"WAL recovery entry %d has invalid skip command payload", index)
		}
		expectedOffset = offset + length
	}
	if expectedOffset != uint64(footerOffset) {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d footer does not cover payload body", index)
	}
	return nil
}

func validateWALTNEntry(
	ctx context.Context,
	walIndex int,
	entryIndex uint32,
	expectedDSN uint64,
	data []byte,
) error {
	if len(data) < tnEntryMinSize {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d TN entry %d is too small: %d bytes",
			walIndex, entryIndex, len(data))
	}
	embeddedDSN := binary.LittleEndian.Uint64(data[:tnEntryDSNSize])
	if embeddedDSN != expectedDSN {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d TN entry %d embedded DSN %d does not match expected DSN %d",
			walIndex, entryIndex, embeddedDSN, expectedDSN)
	}
	typ := binary.LittleEndian.Uint16(data[tnEntryTypeOffset:])
	version := binary.LittleEndian.Uint16(data[tnEntryVersionOffset:])
	payloadSize := uint64(binary.LittleEndian.Uint32(data[tnEntryPayloadSizeOffset:]))
	infoSize := uint64(binary.LittleEndian.Uint32(data[tnEntryInfoSizeOffset:]))
	if typ < tnEntryMinType || typ > tnEntryMaxType || version != tnEntryVersionV1 {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d TN entry %d has unsupported type/version %d/%d",
			walIndex, entryIndex, typ, version)
	}
	if infoSize < tnEntryInfoHeaderSize ||
		infoSize+payloadSize != uint64(len(data)-tnEntryInfoOffset) {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d TN entry %d has invalid info/payload sizes %d/%d for %d bytes",
			walIndex, entryIndex, infoSize, payloadSize, len(data))
	}
	infoEnd := uint64(tnEntryInfoOffset) + infoSize
	return validateWALTNEntryInfo(ctx, walIndex, entryIndex, data[tnEntryInfoOffset:infoEnd])
}

func validateWALTNEntryInfo(
	ctx context.Context,
	walIndex int,
	entryIndex uint32,
	info []byte,
) error {
	pos := uint64(20)
	consume := func(size uint64) ([]byte, bool) {
		if size > uint64(len(info))-pos {
			return nil, false
		}
		result := info[pos : pos+size]
		pos += size
		return result, true
	}
	invalid := func() error {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery entry %d TN entry %d has malformed info metadata",
			walIndex, entryIndex)
	}

	checkpointCountBytes, ok := consume(8)
	if !ok {
		return invalid()
	}
	checkpointCount := binary.LittleEndian.Uint64(checkpointCountBytes)
	if checkpointCount > (uint64(len(info))-pos)/20 {
		return invalid()
	}
	for i := uint64(0); i < checkpointCount; i++ {
		if _, ok := consume(4); !ok { // checkpoint group
			return invalid()
		}
		intervalCountBytes, ok := consume(8)
		if !ok {
			return invalid()
		}
		intervalCount := binary.LittleEndian.Uint64(intervalCountBytes)
		if intervalCount > (uint64(len(info))-pos)/16 {
			return invalid()
		}
		if _, ok := consume(intervalCount * 16); !ok {
			return invalid()
		}
		commandCountBytes, ok := consume(8)
		if !ok {
			return invalid()
		}
		commandCount := binary.LittleEndian.Uint64(commandCountBytes)
		if commandCount > (uint64(len(info))-pos)/16 {
			return invalid()
		}
		for j := uint64(0); j < commandCount; j++ {
			if _, ok := consume(8); !ok { // command LSN
				return invalid()
			}
			commandIDCountBytes, ok := consume(4)
			if !ok {
				return invalid()
			}
			remaining := uint64(len(info)) - pos
			commandIDCount := uint64(binary.LittleEndian.Uint32(commandIDCountBytes))
			if remaining < 4 || commandIDCount > (remaining-4)/4 {
				return invalid()
			}
			if _, ok := consume(commandIDCount*4 + 4); !ok {
				return invalid()
			}
		}
	}
	if pos != uint64(len(info)) {
		return invalid()
	}
	return nil
}

func walRecoveryPreallocEntries(count uint32) int {
	if count > maxWALRecoveryPreallocEntries {
		return maxWALRecoveryPreallocEntries
	}
	return int(count)
}

func shouldCheckpointWALRecoveryState(completed, total uint64) bool {
	return completed == total || completed%walRecoveryCheckpointEntries == 0
}

func normalizeWALEntrySafeDSN(entries []WALEntry) {
	contiguousDSN := firstNormalDSN(entries)
	if contiguousDSN > 0 {
		contiguousDSN--
	}

	pending := make(dsnIntervalHeap, 0)
	for i := range entries {
		entry := &entries[i]
		// SafeDSN describes entries known durable before this LogEntry. Including
		// the current batch causes TN replay to skip a multi-entry batch because
		// the replayer indexes the batch only by its starting DSN.
		// Never raise the source watermark: lower DSNs may occur at later PSNs
		// when asynchronous commits complete out of order.
		safeDSN := entry.SafeDSN
		if safeDSN > contiguousDSN {
			safeDSN = contiguousDSN
		}
		entry.SafeDSN = safeDSN
		if len(entry.RawData) >= logEntrySafeDSNOffset+8 {
			binary.LittleEndian.PutUint64(entry.RawData[logEntrySafeDSNOffset:], safeDSN)
		}

		if entry.DSN != 0 && entry.EntryCount != 0 {
			start := entry.DSN
			end := entry.DSN + uint64(entry.EntryCount) - 1
			if intervalStartsBeforeOrAtNext(start, contiguousDSN) {
				if end > contiguousDSN {
					contiguousDSN = end
				}
				contiguousDSN = drainContiguousIntervals(&pending, contiguousDSN)
			} else {
				heap.Push(&pending, dsnInterval{start: start, end: end})
			}
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
		if !intervalStartsBeforeOrAtNext(next.start, contiguousDSN) {
			break
		}
		heap.Pop(pending)
		if next.end > contiguousDSN {
			contiguousDSN = next.end
		}
	}
	return contiguousDSN
}

func intervalStartsBeforeOrAtNext(start, current uint64) bool {
	return start <= current || (current < math.MaxUint64 && start == current+1)
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

func walRecoveryRecordMatches(
	ctx context.Context,
	walData *WALRecoveryData,
	file *os.File,
	entryIndex int,
	record pb.LogRecord,
) (bool, error) {
	if record.Type != pb.UserRecord || len(record.Data) < headerSize+leaseHolderIDSize {
		return false, nil
	}
	if parseCmdTag(record.Data) != pb.UserEntryUpdate {
		return false, nil
	}
	expected, err := loadWALRecoveryEntryData(ctx, walData, file, entryIndex)
	if err != nil {
		return false, err
	}
	return bytes.Equal(record.Data[headerSize+leaseHolderIDSize:], expected), nil
}

func walRecoveryProgressAfterRecords(
	ctx context.Context,
	state *walRecoveryState,
	walData *WALRecoveryData,
	file *os.File,
	currentLSN uint64,
	records []pb.LogRecord,
) (uint64, error) {
	entryCount := uint64(len(walData.Entries))
	if err := validateWALRecoveryState(ctx, state, walData.Digest, entryCount); err != nil {
		return 0, err
	}
	if currentLSN < state.LastLSN {
		return 0, moerr.NewInternalErrorf(ctx,
			"Log shard latest LSN %d is below WAL recovery last LSN %d",
			currentLSN, state.LastLSN)
	}
	if state.Complete {
		return state.CompletedEntries, nil
	}

	completed := state.CompletedEntries
	lastRecordLSN := state.LastLSN
	for _, record := range records {
		if record.Lsn <= lastRecordLSN || record.Lsn > currentLSN {
			return 0, moerr.NewInternalErrorf(ctx,
				"WAL recovery read returned invalid LSN %d after %d with destination latest LSN %d",
				record.Lsn, lastRecordLSN, currentLSN)
		}
		lastRecordLSN = record.Lsn
		// QueryRaftLog also returns Raft metadata and lease updates. They
		// consume destination LSNs, but they are not records exported by
		// mo_br and therefore must not advance the source WAL position.
		if record.Type != pb.UserRecord {
			continue
		}
		if completed >= entryCount {
			return 0, moerr.NewInternalErrorf(ctx,
				"destination Log shard contains an unexpected user entry at LSN %d after all WAL entries",
				record.Lsn)
		}
		matches, err := walRecoveryRecordMatches(ctx, walData, file, int(completed), record)
		if err != nil {
			return 0, err
		}
		if !matches {
			return 0, moerr.NewInternalErrorf(ctx,
				"destination Log shard user entry at LSN %d does not match WAL recovery entry %d; another writer may be active",
				record.Lsn, completed)
		}
		completed++
	}
	return completed, nil
}

func (s *Service) reconcileWALRecoveryProgress(
	ctx context.Context,
	replayClient *client,
	state *walRecoveryState,
	walData *WALRecoveryData,
	file *os.File,
	currentLSN uint64,
	readMaxSize uint64,
) (int, bool, error) {
	if state.Complete || currentLSN == state.LastLSN {
		completed, err := walRecoveryProgressAfterRecords(ctx, state, walData, file, currentLSN, nil)
		return int(completed), false, err
	}
	if currentLSN < state.LastLSN {
		return 0, false, moerr.NewInternalErrorf(ctx,
			"Log shard latest LSN %d is below WAL recovery last LSN %d",
			currentLSN, state.LastLSN)
	}
	firstLSN := state.LastLSN + 1
	progress := *state
	for {
		readCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)
		chunk, nextLSN, err := replayClient.doRead(readCtx, firstLSN, readMaxSize)
		cancel()
		if err != nil {
			return 0, false, moerr.NewInternalErrorf(ctx,
				"failed to inspect destination Log shard from LSN %d: %v", firstLSN, err)
		}
		completed, err := walRecoveryProgressAfterRecords(ctx, &progress, walData, file, currentLSN, chunk)
		if err != nil {
			return 0, false, err
		}
		progress.CompletedEntries = completed
		if len(chunk) > 0 {
			progress.LastLSN = chunk[len(chunk)-1].Lsn
		}
		if nextLSN == firstLSN {
			break
		}
		if nextLSN <= firstLSN || nextLSN > currentLSN {
			return 0, false, moerr.NewInternalErrorf(ctx,
				"destination Log shard returned invalid next LSN %d while reading from %d to %d",
				nextLSN, firstLSN, currentLSN)
		}
		firstLSN = nextLSN
	}

	state.CompletedEntries = progress.CompletedEntries
	state.LastLSN = currentLSN
	return int(progress.CompletedEntries), true, nil
}

func (s *Service) getWALRecoveryLeaseHolder(ctx context.Context, shardID uint64) (uint64, error) {
	readCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)
	defer cancel()
	v, err := s.store.read(readCtx, shardID, leaseHolderIDQuery{})
	if err != nil {
		return 0, moerr.NewInternalErrorf(ctx,
			"failed to read Log shard lease holder before WAL recovery: %v", err)
	}
	leaseHolderID, ok := v.(uint64)
	if !ok {
		return 0, moerr.NewInternalErrorf(ctx,
			"unexpected Log shard lease holder type %T", v)
	}
	return leaseHolderID, nil
}

func (s *Service) getWALRecoveryMarker(
	ctx context.Context,
	shardID uint64,
) (walRecoveryMarkerState, error) {
	readCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)
	defer cancel()
	v, err := s.store.read(readCtx, shardID, walRecoveryStateQuery{})
	if err != nil {
		return walRecoveryMarkerState{}, moerr.NewInternalErrorf(ctx,
			"failed to read destination WAL recovery marker: %v", err)
	}
	marker, ok := v.(walRecoveryMarkerState)
	if !ok {
		return walRecoveryMarkerState{}, moerr.NewInternalErrorf(ctx,
			"unexpected WAL recovery marker type %T", v)
	}
	return marker, nil
}

func validateWALRecoveryMarker(
	ctx context.Context,
	marker walRecoveryMarkerState,
	digest string,
	entryCount uint64,
) error {
	if marker.Digest == "" {
		return nil
	}
	if marker.Digest != digest || marker.EntryCount != entryCount {
		return moerr.NewInternalErrorf(ctx,
			"destination Log shard belongs to WAL recovery %s/%d, source is %s/%d",
			marker.Digest, marker.EntryCount, digest, entryCount)
	}
	if marker.CompletedEntries > marker.EntryCount {
		return moerr.NewInternalErrorf(ctx,
			"destination WAL recovery marker completed %d entries, expected at most %d",
			marker.CompletedEntries, marker.EntryCount)
	}
	if marker.Complete && marker.CompletedEntries != marker.EntryCount {
		return moerr.NewInternalErrorf(ctx,
			"destination WAL recovery marker is complete with %d of %d entries",
			marker.CompletedEntries, marker.EntryCount)
	}
	return nil
}

func appendWALRecoveryControl(
	ctx context.Context,
	replayClient *client,
	cmd []byte,
) (uint64, error) {
	appendCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)
	defer cancel()
	lsn, err := replayClient.doAppend(appendCtx, pb.LogRecord{Data: cmd})
	return uint64(lsn), err
}

func ensureWALRecoveryDestinationClean(
	ctx context.Context,
	replayClient *client,
	lastLSN uint64,
	readMaxSize uint64,
) error {
	if lastLSN == 0 {
		return nil
	}
	firstLSN := uint64(1)
	for firstLSN <= lastLSN {
		readCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)
		records, nextLSN, err := replayClient.doRead(readCtx, firstLSN, readMaxSize)
		cancel()
		if err != nil {
			return moerr.NewInternalErrorf(ctx,
				"failed to validate empty Log shard from LSN %d: %v", firstLSN, err)
		}
		for _, record := range records {
			if uint64(record.Lsn) > lastLSN {
				break
			}
			if record.Type == pb.UserRecord {
				return moerr.NewInternalErrorf(ctx,
					"WAL recovery requires a clean destination Log shard, found user entry at LSN %d",
					record.Lsn)
			}
		}
		if uint64(nextLSN) == firstLSN || uint64(nextLSN) > lastLSN {
			return nil
		}
		if uint64(nextLSN) < firstLSN {
			return moerr.NewInternalErrorf(ctx,
				"invalid next LSN %d while validating Log shard from %d",
				nextLSN, firstLSN)
		}
		firstLSN = uint64(nextLSN)
	}
	return nil
}

// replayWALEntries replays WAL entries to the Log shard.
//
// BootstrapHAKeeper elects exactly one configured LogService as the recovery
// coordinator. It may not be the Log shard leader, so WAL replay appends
// through the normal LogService client path. The client uses discovery/service
// addresses to reach the current shard leader instead of proposing through
// this pod's local NodeHost.
func (s *Service) replayWALEntries(
	ctx context.Context,
	cfg Config,
	walData *WALRecoveryData,
	requireCompletedDestination bool,
) error {
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

		info, ok := s.getShardInfo(ctx, shardID, true, true)
		if ok && info.LeaderID != 0 {
			logger.Info("WAL recovery: Log shard is ready",
				zap.Uint64("shard_id", shardID),
				zap.Uint64("leader_id", info.LeaderID))
			if err := waitWALRecoveryInterval(ctx, 5*time.Second); err != nil {
				return err
			}

			info2, ok2 := s.getShardInfo(ctx, shardID, true, true)
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

	var walFile *os.File
	if walData.Path != "" {
		walFile, err = os.Open(walData.Path)
		if err != nil {
			return moerr.NewInternalErrorf(ctx,
				"failed to reopen WAL recovery data file: %v", err)
		}
		defer walFile.Close()
	}
	maxEntryDataSize, err := walRecoveryMaxEntryDataSize(ctx, cfg)
	if err != nil {
		return err
	}
	readMaxSize := uint64(maxEntryDataSize)

	statePath := getWALRecoveryStateFile(cfg.BootstrapConfig.Restore.WALDataPath)
	entryCount := uint64(len(walData.Entries))
	marker, err := s.getWALRecoveryMarker(ctx, shardID)
	if err != nil {
		return err
	}
	if err := validateWALRecoveryMarker(ctx, marker, walData.Digest, entryCount); err != nil {
		return err
	}
	if requireCompletedDestination && !marker.Complete {
		return moerr.NewInternalError(ctx,
			"HAKeeper recovery is complete but destination Log shard has no completion marker")
	}
	leaseHolderID, err := s.getWALRecoveryLeaseHolder(ctx, shardID)
	if err != nil {
		return err
	}
	recoveryState, sidecarErr := readWALRecoveryState(ctx, statePath)
	if sidecarErr != nil {
		logger.Warn("WAL recovery: ignoring invalid local progress sidecar",
			zap.String("path", statePath),
			zap.Error(sidecarErr))
		recoveryState = nil
	}
	if marker.Complete {
		if leaseHolderID == walRecoveryLeaseHolderID {
			return moerr.NewInternalError(ctx,
				"completed destination WAL recovery marker still has an active fence")
		}
		if err := verifyWALRecoverySource(ctx, walData.Path, walData.Digest); err != nil {
			return err
		}
		logger.Info("WAL recovery: destination-bound completion marker found, skipping replay",
			zap.Int("entries", len(walData.Entries)),
			zap.Bool("force_ignored", cfg.BootstrapConfig.Restore.Force))
		return nil
	}

	newDestination := marker.Digest == ""
	if newDestination {
		if leaseHolderID == walRecoveryLeaseHolderID {
			return moerr.NewInternalError(ctx,
				"Log shard has a legacy WAL recovery fence without a destination marker; rebuild the destination shard")
		}
		if recoveryState != nil {
			logger.Warn("WAL recovery: ignoring sidecar from a different destination shard")
		}
		recoveryState = &walRecoveryState{
			Version:               walRecoveryStateVersion,
			WALDigest:             walData.Digest,
			EntryCount:            entryCount,
			OriginalLeaseHolderID: leaseHolderID,
		}
	} else {
		if leaseHolderID != walRecoveryLeaseHolderID {
			return moerr.NewInternalErrorf(ctx,
				"incomplete destination WAL recovery marker requires fence, found lease holder %d",
				leaseHolderID)
		}
		if recoveryState != nil {
			if stateErr := validateWALRecoveryState(
				ctx, recoveryState, walData.Digest, entryCount); stateErr != nil ||
				recoveryState.BaseLSN != marker.BaseLSN ||
				recoveryState.OriginalLeaseHolderID != marker.OriginalLeaseHolderID ||
				recoveryState.CompletedEntries > marker.CompletedEntries ||
				recoveryState.LastLSN > marker.LastLSN {
				logger.Warn("WAL recovery: rebuilding local progress from destination marker",
					zap.Error(stateErr))
				recoveryState = nil
			}
		}
		if recoveryState == nil {
			recoveryState = &walRecoveryState{
				Version:               walRecoveryStateVersion,
				WALDigest:             walData.Digest,
				EntryCount:            entryCount,
				BaseLSN:               marker.BaseLSN,
				LastLSN:               marker.BaseLSN,
				OriginalLeaseHolderID: marker.OriginalLeaseHolderID,
			}
		}
		recoveryState.Complete = false
	}
	if leaseHolderID != recoveryState.OriginalLeaseHolderID &&
		leaseHolderID != walRecoveryLeaseHolderID {
		return moerr.NewInternalErrorf(ctx,
			"incomplete WAL recovery expected lease holder %d or recovery fence, found %d",
			recoveryState.OriginalLeaseHolderID, leaseHolderID)
	}

	fenceLSN, err := appendWALRecoveryControl(
		ctx, replayClient, getBeginWALRecoveryCmd(walData.Digest, entryCount))
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to establish WAL recovery fence: %v", err)
	}
	marker, err = s.getWALRecoveryMarker(ctx, shardID)
	if err != nil {
		return err
	}
	if err := validateWALRecoveryMarker(ctx, marker, walData.Digest, entryCount); err != nil {
		return err
	}
	if marker.Digest == "" || marker.Complete {
		return moerr.NewInternalError(ctx,
			"destination did not establish an incomplete WAL recovery marker")
	}
	truncateCtx, truncateCancel := context.WithTimeoutCause(
		ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)
	truncatedLSN, truncateErr := replayClient.doGetTruncatedLsn(truncateCtx)
	truncateCancel()
	if truncateErr != nil {
		return moerr.NewInternalErrorf(ctx,
			"failed to get destination Log shard truncated LSN: %v", truncateErr)
	}
	if truncatedLSN != 0 {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery requires an untruncated destination Log shard, truncated LSN is %d",
			truncatedLSN)
	}
	// Recheck the immutable pre-recovery prefix on every retry. A first
	// attempt can persist the marker and fence before discovering a dirty
	// destination; marker existence must not let a retry bypass this check.
	if err := ensureWALRecoveryDestinationClean(
		ctx, replayClient, marker.BaseLSN, readMaxSize); err != nil {
		return err
	}
	if newDestination {
		if marker.BaseLSN != fenceLSN {
			return moerr.NewInternalErrorf(ctx,
				"destination WAL recovery marker base LSN %d does not match fence LSN %d",
				marker.BaseLSN, fenceLSN)
		}
	}
	recoveryState.BaseLSN = marker.BaseLSN
	if recoveryState.LastLSN == 0 {
		recoveryState.LastLSN = marker.BaseLSN
	}
	recoveryState.OriginalLeaseHolderID = marker.OriginalLeaseHolderID
	if err := writeWALRecoveryState(ctx, statePath, *recoveryState); err != nil {
		return err
	}

	lsnCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)
	currentLSN, err := replayClient.doGetLatestLsn(lsnCtx)
	cancel()
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get fenced Log shard latest LSN: %v", err)
	}
	startIndex, changed, err := s.reconcileWALRecoveryProgress(
		ctx, replayClient, recoveryState, walData, walFile, uint64(currentLSN), readMaxSize)
	if err != nil {
		return err
	}
	if changed {
		if err := writeWALRecoveryState(ctx, statePath, *recoveryState); err != nil {
			return err
		}
	}
	logger.Info("WAL recovery: resume position",
		zap.Int("completed_entries", startIndex),
		zap.Uint64("base_lsn", recoveryState.BaseLSN),
		zap.Uint64("last_lsn", recoveryState.LastLSN),
		zap.Uint64("current_lsn", uint64(currentLSN)))

	for int(recoveryState.CompletedEntries) < len(walData.Entries) {
		latestCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)
		currentLSN, err = replayClient.doGetLatestLsn(latestCtx)
		cancel()
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to check Log shard latest LSN during recovery: %v", err)
		}
		_, changed, err = s.reconcileWALRecoveryProgress(
			ctx, replayClient, recoveryState, walData, walFile, uint64(currentLSN), readMaxSize)
		if err != nil {
			return err
		}
		if changed {
			if err := writeWALRecoveryState(ctx, statePath, *recoveryState); err != nil {
				return err
			}
			if int(recoveryState.CompletedEntries) == len(walData.Entries) {
				break
			}
		}

		i := int(recoveryState.CompletedEntries)
		entry := walData.Entries[i]
		entryData, err := loadWALRecoveryEntryData(ctx, walData, walFile, i)
		if err != nil {
			return err
		}
		if i == startIndex {
			logger.Info("WAL recovery: processing first entry",
				zap.Int("index", i),
				zap.Uint64("dsn", entry.DSN),
				zap.Uint64("safe_dsn", entry.SafeDSN),
				zap.Uint64("raft_index", entry.RaftIndex),
				zap.Int("data_len", len(entryData)))
		}

		cmd := buildUserEntryCmd(walRecoveryLeaseHolderID, entryData)
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
		if uint64(lsn) <= recoveryState.LastLSN {
			return moerr.NewInternalErrorf(ctx,
				"non-increasing LSN while replaying WAL entry %d: got %d after %d",
				i, lsn, recoveryState.LastLSN)
		}
		if uint64(lsn) == recoveryState.LastLSN+1 {
			recoveryState.CompletedEntries++
			recoveryState.LastLSN = uint64(lsn)
		} else {
			completedBefore := recoveryState.CompletedEntries
			_, changed, err = s.reconcileWALRecoveryProgress(
				ctx, replayClient, recoveryState, walData, walFile, uint64(lsn), readMaxSize)
			if err != nil {
				return err
			}
			if !changed || recoveryState.CompletedEntries != completedBefore+1 {
				return moerr.NewInternalErrorf(ctx,
					"WAL recovery append at LSN %d did not add exactly entry %d",
					lsn, i)
			}
		}
		// A retry reconciles entries after the last durable checkpoint against
		// the destination shard, so avoid an fsync for every WAL entry.
		if shouldCheckpointWALRecoveryState(
			recoveryState.CompletedEntries,
			uint64(len(walData.Entries)),
		) {
			if err := writeWALRecoveryState(ctx, statePath, *recoveryState); err != nil {
				return err
			}
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

	latestCtx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, moerr.CauseLogServiceBootstrap)
	currentLSN, err = replayClient.doGetLatestLsn(latestCtx)
	cancel()
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to verify Log shard latest LSN after recovery: %v", err)
	}
	_, _, err = s.reconcileWALRecoveryProgress(
		ctx, replayClient, recoveryState, walData, walFile, uint64(currentLSN), readMaxSize)
	if err != nil {
		return err
	}
	if recoveryState.CompletedEntries != uint64(len(walData.Entries)) {
		return moerr.NewInternalErrorf(ctx,
			"WAL recovery destination contains %d of %d expected entries",
			recoveryState.CompletedEntries, len(walData.Entries))
	}
	marker, err = s.getWALRecoveryMarker(ctx, shardID)
	if err != nil {
		return err
	}
	if err := validateWALRecoveryMarker(ctx, marker, walData.Digest, entryCount); err != nil {
		return err
	}
	if marker.CompletedEntries != entryCount {
		return moerr.NewInternalErrorf(ctx,
			"destination WAL recovery marker is at %d entries, sidecar is at %d",
			marker.CompletedEntries, recoveryState.CompletedEntries)
	}
	if err := verifyWALRecoverySource(ctx, walData.Path, walData.Digest); err != nil {
		return err
	}
	if _, err := appendWALRecoveryControl(
		ctx, replayClient, getEndWALRecoveryCmd(walData.Digest, entryCount)); err != nil {
		return moerr.NewInternalErrorf(ctx,
			"failed to release WAL recovery fence: %v", err)
	}
	marker, err = s.getWALRecoveryMarker(ctx, shardID)
	if err != nil {
		return err
	}
	if err := validateWALRecoveryMarker(ctx, marker, walData.Digest, entryCount); err != nil {
		return err
	}
	if !marker.Complete {
		return moerr.NewInternalError(ctx,
			"destination did not persist WAL recovery completion")
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
