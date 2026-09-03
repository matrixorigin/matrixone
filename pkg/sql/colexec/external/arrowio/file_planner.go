// Copyright 2026 Matrix Origin
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

package arrowio

import (
	"context"
	"encoding/binary"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/arrowipc"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const (
	messageHeaderDictionaryBatch = byte(2)
	messageHeaderRecordBatch     = byte(3)
)

// RecordBatchInfo is the footer-stable planning metadata for one record block.
type RecordBatchInfo struct {
	Index     int32
	Rows      int64
	WireBytes int64
}

// DictionaryBlockInfo describes one dictionary epoch transition in footer
// order. A non-delta block establishes an ID exactly once; subsequent blocks
// for that ID must be deltas.
type DictionaryBlockInfo struct {
	Index     int32
	ID        int64
	IsDelta   bool
	Rows      int64
	WireBytes int64
}

// FilePlan is an immutable, bounded description of an IPC File. Schema is the
// official Arrow-Go decoded schema; payload bodies are not read by planning.
type FilePlan struct {
	Schema        *arrow.Schema
	RecordBatches []RecordBatchInfo
	Dictionaries  []DictionaryBlockInfo
	recordBlocks  []fileBlock
	dictBlocks    []fileBlock
}

// Shard returns a self-contained decoder interval and its conservative
// dictionary closure. Replaying every dictionary transition preceding the last
// selected record is safe even when projection-specific dependencies are not
// provable.
func (p *FilePlan) Shard(start, end int) (FileShard, int64, int64, error) {
	if p == nil || start < 0 || start >= end || end > len(p.RecordBatches) {
		return FileShard{}, 0, 0, moerr.NewInvalidInputNoCtx("invalid Arrow IPC File shard interval")
	}
	shard := FileShard{RecordBatchStart: int32(start), RecordBatchEnd: int32(end)}
	var rows, wireBytes int64
	for _, record := range p.RecordBatches[start:end] {
		if record.Rows > 0 && rows > maxInt64-record.Rows {
			return FileShard{}, 0, 0, moerr.NewInvalidInputNoCtx("Arrow shard row count overflows")
		}
		if record.WireBytes > 0 && wireBytes > maxInt64-record.WireBytes {
			return FileShard{}, 0, 0, moerr.NewInvalidInputNoCtx("Arrow shard wire size overflows")
		}
		rows += record.Rows
		wireBytes += record.WireBytes
	}
	lastRecordOffset := p.recordBlocks[end-1].offset
	for index, dictionary := range p.dictBlocks {
		if dictionary.offset >= lastRecordOffset {
			continue
		}
		shard.RequiredDictionaryBlockIndices = append(
			shard.RequiredDictionaryBlockIndices, int32(index),
		)
		if dictionary.metadata+dictionary.body > maxInt64-wireBytes {
			return FileShard{}, 0, 0, moerr.NewInvalidInputNoCtx("Arrow shard wire size overflows")
		}
		wireBytes += dictionary.metadata + dictionary.body
	}
	return shard, rows, wireBytes, nil
}

const maxInt64 = int64(^uint64(0) >> 1)

// InspectFile reads only the bounded footer, schema metadata, and block
// metadata. It validates dictionary base/delta ordering before any shard can
// be published.
func InspectFile(
	ctx context.Context,
	fs fileservice.FileService,
	path string,
	size int64,
	admission fileservice.RangeReadAdmission,
	options Options,
) (_ *FilePlan, retErr error) {
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	if fs == nil || path == "" || size < 0 || admission == nil {
		return nil, moerr.NewInvalidInput(ctx, "invalid Arrow IPC File planning source")
	}
	if options.FileShard != nil {
		return nil, moerr.NewInvalidInput(ctx, "Arrow IPC File planning cannot consume a shard")
	}
	rangeReader := fileservice.NewLeasedRangeReader(fs)
	if options.ExpectedIdentity != nil {
		expected := *options.ExpectedIdentity
		if err := expected.Validate(); err != nil {
			return nil, err
		}
		if expected.Size != size {
			return nil, moerr.NewInvalidInputf(ctx,
				"Arrow object identity size %d does not match planned size %d", expected.Size, size)
		}
		conditional, ok := rangeReader.(fileservice.ConditionalLeasedRangeReader)
		if !ok {
			return nil, moerr.NewNotSupported(ctx, "conditional Arrow object reads")
		}
		rangeReader = fixedIdentityRangeReader{reader: conditional, expected: expected}
	}
	ownedAllocator := newAdmissionAllocator(ctx, admission)
	defer ownedAllocator.releaseAll()
	options.Allocator = ownedAllocator
	defer func() {
		if recovered := recover(); recovered != nil {
			if allocationErr, matched := recoveredAllocationError(recovered); matched {
				retErr = allocationErr
				return
			}
			retErr = moerr.NewInvalidInputf(ctx, "invalid Arrow IPC File planning metadata: %v", recovered)
		}
	}()

	records, dictionaries, err := readFooterBlocks(ctx, rangeReader, path, size, admission, options)
	if err != nil {
		return nil, err
	}
	if _, err = mergeFileBlocks(records, dictionaries); err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "invalid Arrow IPC File block ordering: %v", err)
	}
	schema, err := inspectFileSchema(ctx, rangeReader, path, size, admission, options, len(records))
	if err != nil {
		return nil, err
	}
	plan := &FilePlan{
		Schema: schema, RecordBatches: make([]RecordBatchInfo, len(records)),
		Dictionaries: make([]DictionaryBlockInfo, len(dictionaries)),
		recordBlocks: records, dictBlocks: dictionaries,
	}
	baseSeen := make(map[int64]struct{}, len(dictionaries))
	for index, block := range dictionaries {
		metadata, err := inspectFileBlockMetadata(ctx, rangeReader, path, block, admission)
		if err != nil {
			return nil, err
		}
		if metadata.headerType != messageHeaderDictionaryBatch {
			return nil, moerr.NewInvalidInputf(ctx,
				"Arrow footer dictionary block %d contains message type %d", index, metadata.headerType)
		}
		if err := acceptDictionaryTransition(baseSeen, metadata.dictionaryID, metadata.isDelta); err != nil {
			return nil, err
		}
		plan.Dictionaries[index] = DictionaryBlockInfo{
			Index: int32(index), ID: metadata.dictionaryID, IsDelta: metadata.isDelta,
			Rows: metadata.rows, WireBytes: block.metadata + block.body,
		}
	}
	for index, block := range records {
		metadata, err := inspectFileBlockMetadata(ctx, rangeReader, path, block, admission)
		if err != nil {
			return nil, err
		}
		if metadata.headerType != messageHeaderRecordBatch {
			return nil, moerr.NewInvalidInputf(ctx,
				"Arrow footer record block %d contains message type %d", index, metadata.headerType)
		}
		plan.RecordBatches[index] = RecordBatchInfo{
			Index: int32(index), Rows: metadata.rows, WireBytes: block.metadata + block.body,
		}
	}
	return plan, nil
}

func acceptDictionaryTransition(baseSeen map[int64]struct{}, id int64, isDelta bool) error {
	if isDelta {
		if _, ok := baseSeen[id]; !ok {
			return moerr.NewInvalidInputNoCtxf("Arrow dictionary %d delta precedes its base", id)
		}
		return nil
	}
	if _, exists := baseSeen[id]; exists {
		return moerr.NewInvalidInputNoCtxf("Arrow dictionary %d has a replacement base", id)
	}
	baseSeen[id] = struct{}{}
	return nil
}

func inspectFileSchema(
	ctx context.Context,
	rangeReader fileservice.LeasedRangeReader,
	path string,
	size int64,
	admission fileservice.RangeReadAdmission,
	options Options,
	expectedRecords int,
) (*arrow.Schema, error) {
	reader, err := ipc.NewFileReader(
		&rangeReadAtSeeker{ctx: ctx, reader: rangeReader, path: path, size: size, admission: admission},
		ipc.WithAllocator(options.Allocator),
		ipc.WithMetadataSizeLimit(options.MaxMetadataBytes),
		ipc.WithBodySizeLimit(options.MaxBodyBytes),
		ipc.WithEnsureNativeEndian(true),
	)
	if err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "invalid Arrow IPC File footer: %v", err)
	}
	schema := reader.Schema()
	records := reader.NumRecords()
	_ = reader.Close()
	if schema == nil || records != expectedRecords {
		return nil, moerr.NewInvalidInput(ctx, "Arrow IPC File footer record count is inconsistent")
	}
	return schema, nil
}

type inspectedBlockMetadata struct {
	headerType   byte
	rows         int64
	dictionaryID int64
	isDelta      bool
	bodyBytes    int64
}

func inspectFileBlockMetadata(
	ctx context.Context,
	reader fileservice.LeasedRangeReader,
	path string,
	block fileBlock,
	admission fileservice.RangeReadAdmission,
) (inspectedBlockMetadata, error) {
	lease, err := reader.ReadRangeLease(ctx, path, block.offset, block.metadata, admission)
	if err != nil {
		return inspectedBlockMetadata{}, err
	}
	defer lease.Release()
	return inspectFileBlockMetadataBytes(ctx, lease.Bytes(), block)
}

func inspectFileBlockMetadataBytes(
	ctx context.Context,
	data []byte,
	block fileBlock,
) (inspectedBlockMetadata, error) {
	if int64(len(data)) != block.metadata || len(data) < 4 {
		return inspectedBlockMetadata{}, moerr.NewInvalidInput(ctx, "Arrow IPC block metadata is truncated")
	}
	prefix := 4
	if len(data) >= 8 && binary.LittleEndian.Uint32(data) == ipcContinuationToken {
		prefix = 8
	} else if binary.LittleEndian.Uint32(data) == 0 {
		prefix = 0
	}
	if len(data)-prefix < 4 {
		return inspectedBlockMetadata{}, moerr.NewInvalidInput(ctx, "Arrow IPC block metadata prefix is invalid")
	}
	return inspectIPCMessageMetadata(
		ctx, data[prefix:], block.body, block.body, nil, false, DefaultMaxDecodedRecordBytes,
	)
}

// inspectIPCMessageMetadata adapts the shared transport-neutral validator to
// the File planner's private metadata shape.
func inspectIPCMessageMetadata(
	ctx context.Context,
	payload []byte,
	maxBodyBytes int64,
	bodyEnvelopeBytes int64,
	body []byte,
	validateBody bool,
	maxDecodedRecordBytes int64,
) (inspectedBlockMetadata, error) {
	info, err := arrowipc.InspectMessage(ctx, payload, arrowipc.ValidationOptions{
		MaxMetadataBytes:      DefaultMaxMetadataBytes,
		MaxBodyBytes:          maxBodyBytes,
		BodyEnvelopeBytes:     bodyEnvelopeBytes,
		Body:                  body,
		ValidateBody:          validateBody,
		MaxDecodedRecordBytes: maxDecodedRecordBytes,
	})
	if err != nil {
		return inspectedBlockMetadata{}, err
	}
	return inspectedBlockMetadata{
		headerType: info.HeaderType, rows: info.Rows,
		dictionaryID: info.DictionaryID, isDelta: info.IsDelta,
		bodyBytes: info.BodyBytes,
	}, nil
}
