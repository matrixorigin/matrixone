// Copyright 2025 Matrix Origin
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

package group

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type SpillManager struct {
	proc         *process.Process
	fileService  fileservice.ReaderWriterFileService
	spillFiles   []string
	groupByTypes []types.Type
	aggInfos     []aggexec.AggFuncExecExpression
}

func NewSpillManager(proc *process.Process, groupByTypes []types.Type, aggInfos []aggexec.AggFuncExecExpression) (*SpillManager, error) {
	manager := &SpillManager{
		proc:         proc,
		spillFiles:   make([]string, 0),
		groupByTypes: groupByTypes,
		aggInfos:     aggInfos,
	}

	fs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}
	if rwfs, ok := fs.(fileservice.ReaderWriterFileService); !ok {
		return nil, moerr.NewInternalErrorNoCtxf("%T is not ReaderWriterFileService", fs)
	} else {
		manager.fileService = rwfs
	}

	return manager, nil
}

func (sm *SpillManager) SpillToDisk(groups []*batch.Batch, aggs []aggexec.AggFuncExec) (err error) {
	return sm.SpillToDiskWithHashTable(groups, aggs, nil, false, false, 0)
}

func (sm *SpillManager) SpillToDiskWithHashTable(groups []*batch.Batch, aggs []aggexec.AggFuncExec, hashTableData []byte, isStrHash bool, keyNullable bool, keyWidth int) (err error) {
	if err := sm.validateSpillInputs(groups, aggs); err != nil {
		return moerr.NewInternalErrorNoCtxf("spill input validation failed: %v", err)
	}

	if len(groups) == 0 && len(aggs) == 0 && len(hashTableData) == 0 {
		return nil
	}

	logutil.Infof("SpillManager: Starting spill operation with %d groups, %d aggregations, and hash table data", len(groups), len(aggs))

	if sm.fileService == nil {
		return moerr.NewInternalErrorNoCtx("file service is not available for spilling")
	}

	filePath := sm.generateSpillFilePath()

	writer, err := sm.fileService.NewWriter(context.Background(), filePath)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to create spill file writer for %s: %v", filePath, err)
	}
	defer func() {
		err = errors.Join(err, writer.Close())
		if err != nil {
			_ = sm.fileService.Delete(context.Background(), filePath)
		}
	}()

	startTime := time.Now()
	defer func() {
		if err == nil {
			duration := time.Since(startTime)
			logutil.Infof("SpillManager: Successfully completed spill operation to %s. Duration: %v, Total spill files: %d",
				filePath, duration, len(sm.spillFiles))
		}
	}()

	logutil.Infof("SpillManager: Writing spill data to file %s", filePath)

	if err := sm.writeHeaderWithHashTable(writer, groups, aggs, hashTableData, isStrHash, keyNullable, keyWidth); err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to write spill file header to %s: %v", filePath, err)
	}

	if err := sm.writeGroupBatches(writer, groups); err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to write group batches to spill file %s: %v", filePath, err)
	}

	if err := sm.writeAggStates(writer, aggs); err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to write aggregation states to spill file %s: %v", filePath, err)
	}

	if err := sm.writeHashTableData(writer, hashTableData); err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to write hash table data to spill file %s: %v", filePath, err)
	}

	sm.spillFiles = append(sm.spillFiles, filePath)

	return nil
}

func (sm *SpillManager) validateSpillInputs(groups []*batch.Batch, aggs []aggexec.AggFuncExec) error {
	for i, group := range groups {
		if group == nil {
			continue
		}

		if group.RowCount() < 0 {
			return moerr.NewInternalErrorNoCtxf("group batch %d has negative row count: %d", i, group.RowCount())
		}

		const maxReasonableRowCount = 1000000
		if group.RowCount() > maxReasonableRowCount {
			return moerr.NewInternalErrorNoCtxf("group batch %d has excessive row count: %d", i, group.RowCount())
		}

		if group.RowCount() > 0 && len(group.Vecs) == 0 {
			return moerr.NewInternalErrorNoCtxf("group batch %d has rows but no vectors", i)
		}
	}

	for i, agg := range aggs {
		if agg == nil {
			continue
		}

		if size := agg.Size(); size < 0 {
			return moerr.NewInternalErrorNoCtxf("aggregation %d has negative size: %d", i, size)
		}
	}

	return nil
}

func (sm *SpillManager) writeHeader(writer io.Writer, groups []*batch.Batch, aggs []aggexec.AggFuncExec) error {
	return sm.writeHeaderWithHashTable(writer, groups, aggs, nil, false, false, 0)
}

func (sm *SpillManager) writeHeaderWithHashTable(writer io.Writer, groups []*batch.Batch, aggs []aggexec.AggFuncExec, hashTableData []byte, isStrHash bool, keyNullable bool, keyWidth int) error {
	nonEmptyGroupCount := int32(0)
	for _, groupBatch := range groups {
		if groupBatch != nil && groupBatch.RowCount() > 0 {
			nonEmptyGroupCount++
		}
	}

	hasHashTable := int32(0)
	if len(hashTableData) > 0 {
		hasHashTable = 1
	}

	hashTableFlags := int32(0)
	if isStrHash {
		hashTableFlags |= 1
	}
	if keyNullable {
		hashTableFlags |= 2
	}

	header := struct {
		GroupCount     int32
		AggCount       int32
		HasHashTable   int32
		HashTableFlags int32
		KeyWidth       int32
		Reserved       int32
	}{
		GroupCount:     nonEmptyGroupCount,
		AggCount:       int32(len(aggs)),
		HasHashTable:   hasHashTable,
		HashTableFlags: hashTableFlags,
		KeyWidth:       int32(keyWidth),
		Reserved:       0,
	}

	return binary.Write(writer, binary.BigEndian, header)
}

func (sm *SpillManager) writeGroupBatches(writer io.Writer, groups []*batch.Batch) error {
	for _, groupBatch := range groups {
		if groupBatch == nil || groupBatch.RowCount() == 0 {
			continue
		}

		if err := sm.writeSizedData(writer, groupBatch.MarshalBinary); err != nil {
			return err
		}
	}
	return nil
}

func (sm *SpillManager) writeAggStates(writer io.Writer, aggs []aggexec.AggFuncExec) error {
	for _, agg := range aggs {
		if agg == nil {
			continue
		}

		if err := sm.writeSizedData(writer, func() ([]byte, error) {
			return aggexec.MarshalAggFuncExec(agg)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (sm *SpillManager) writeSizedData(writer io.Writer, marshalFunc func() ([]byte, error)) error {
	data, err := marshalFunc()
	if err != nil {
		return err
	}

	size := int32(len(data))
	if err := binary.Write(writer, binary.BigEndian, size); err != nil {
		return err
	}

	_, err = writer.Write(data)
	return err
}

var spillCounter atomic.Int64

func (sm *SpillManager) generateSpillFilePath() string {
	return fmt.Sprintf("group_spill_%d", spillCounter.Add(1))
}

func (sm *SpillManager) ReadSpilledData(filePath string) ([]*batch.Batch, []aggexec.AggFuncExec, error) {
	groups, aggs, _, _, _, _, err := sm.ReadSpilledDataWithHashTable(filePath)
	return groups, aggs, err
}

func (sm *SpillManager) ReadSpilledDataWithHashTable(filePath string) ([]*batch.Batch, []aggexec.AggFuncExec, []byte, bool, bool, int, error) {
	if filePath == "" {
		return nil, nil, nil, false, false, 0, moerr.NewInternalErrorNoCtx("spill file path cannot be empty")
	}

	if sm.fileService == nil {
		return nil, nil, nil, false, false, 0, moerr.NewInternalErrorNoCtx("file service is not available for reading spilled data")
	}

	reader, err := sm.fileService.NewReader(context.Background(), filePath)
	if err != nil {
		return nil, nil, nil, false, false, 0, moerr.NewInternalErrorNoCtxf("failed to open spill file %s: %v", filePath, err)
	}
	defer reader.Close()

	logutil.Infof("SpillManager: Reading spilled data from file %s", filePath)
	startTime := time.Now()

	header, err := sm.readHeaderWithHashTable(reader)
	if err != nil {
		return nil, nil, nil, false, false, 0, moerr.NewInternalErrorNoCtxf("failed to read spill file header from %s: %v", filePath, err)
	}

	if err := sm.validateSpillHeaderWithHashTable(header, filePath); err != nil {
		return nil, nil, nil, false, false, 0, err
	}

	var groups []*batch.Batch
	var aggs []aggexec.AggFuncExec
	var hashTableData []byte
	var readErr error

	cleanup := func() {
		cleanupResources(sm.proc.Mp(), groups, aggs)
		groups = nil
		aggs = nil
	}

	if groups, readErr = sm.readGroupBatches(reader, header.GroupCount); readErr != nil {
		cleanup()
		return nil, nil, nil, false, false, 0, moerr.NewInternalErrorNoCtxf("failed to read group batches from %s: %v", filePath, readErr)
	}

	if aggs, readErr = sm.readAggStates(reader, header.AggCount); readErr != nil {
		cleanup()
		return nil, nil, nil, false, false, 0, moerr.NewInternalErrorNoCtxf("failed to read aggregation states from %s: %v", filePath, readErr)
	}

	if header.HasHashTable > 0 {
		if hashTableData, readErr = sm.readHashTableData(reader); readErr != nil {
			cleanup()
			return nil, nil, nil, false, false, 0, moerr.NewInternalErrorNoCtxf("failed to read hash table data from %s: %v", filePath, readErr)
		}
	}

	if int32(len(groups)) != header.GroupCount {
		cleanup()
		return nil, nil, nil, false, false, 0, moerr.NewInternalErrorNoCtxf("spill file %s group count mismatch: expected %d, got %d",
			filePath, header.GroupCount, len(groups))
	}

	if int32(len(aggs)) != header.AggCount {
		cleanup()
		return nil, nil, nil, false, false, 0, moerr.NewInternalErrorNoCtxf("spill file %s aggregation count mismatch: expected %d, got %d",
			filePath, header.AggCount, len(aggs))
	}

	isStrHash := (header.HashTableFlags & 1) != 0
	keyNullable := (header.HashTableFlags & 2) != 0
	keyWidth := int(header.KeyWidth)

	duration := time.Since(startTime)
	logutil.Infof("SpillManager: Successfully read spilled data from %s. Duration: %v, Groups: %d, Aggregations: %d, HashTable: %t",
		filePath, duration, len(groups), len(aggs), len(hashTableData) > 0)

	return groups, aggs, hashTableData, isStrHash, keyNullable, keyWidth, nil
}

func (sm *SpillManager) validateSpillHeader(header struct{ GroupCount, AggCount int32 }, filePath string) error {
	headerExt := struct {
		GroupCount     int32
		AggCount       int32
		HasHashTable   int32
		HashTableFlags int32
		KeyWidth       int32
		Reserved       int32
	}{
		GroupCount:     header.GroupCount,
		AggCount:       header.AggCount,
		HasHashTable:   0,
		HashTableFlags: 0,
		KeyWidth:       0,
		Reserved:       0,
	}

	return sm.validateSpillHeaderWithHashTable(headerExt, filePath)
}

func (sm *SpillManager) validateSpillHeaderWithHashTable(header struct {
	GroupCount     int32
	AggCount       int32
	HasHashTable   int32
	HashTableFlags int32
	KeyWidth       int32
	Reserved       int32
}, filePath string) error {
	if header.GroupCount < 0 {
		return moerr.NewInternalErrorNoCtxf("spill file %s has negative group count: %d", filePath, header.GroupCount)
	}

	if header.AggCount < 0 {
		return moerr.NewInternalErrorNoCtxf("spill file %s has negative aggregation count: %d", filePath, header.AggCount)
	}

	if header.HasHashTable < 0 || header.HasHashTable > 1 {
		return moerr.NewInternalErrorNoCtxf("spill file %s has invalid hash table flag: %d", filePath, header.HasHashTable)
	}

	const maxReasonableCount = 100000
	if header.GroupCount > maxReasonableCount {
		return moerr.NewInternalErrorNoCtxf("spill file %s has excessive group count: %d", filePath, header.GroupCount)
	}

	if header.AggCount > maxReasonableCount {
		return moerr.NewInternalErrorNoCtxf("spill file %s has excessive aggregation count: %d", filePath, header.AggCount)
	}

	totalCount := int64(header.GroupCount) + int64(header.AggCount)
	if totalCount > maxReasonableCount {
		return moerr.NewInternalErrorNoCtxf("spill file %s has excessive total count: %d", filePath, totalCount)
	}

	return nil
}

func (sm *SpillManager) Cleanup(ctx context.Context) error {
	_ = sm.fileService.Delete(ctx, sm.spillFiles...)
	sm.spillFiles = sm.spillFiles[:0]
	return nil
}

func (sm *SpillManager) HasSpilledData() bool {
	return len(sm.spillFiles) > 0
}

func (sm *SpillManager) GetSpillFileCount() int {
	return len(sm.spillFiles)
}

func (sm *SpillManager) Size() int64 {
	var size int64
	// Account for spill file paths
	for _, filePath := range sm.spillFiles {
		size += int64(len(filePath))
	}
	// Account for groupByTypes
	for _, typ := range sm.groupByTypes {
		size += int64(typ.ProtoSize())
	}
	// Account for aggInfos (approximate)
	size += int64(len(sm.aggInfos) * 64) // Approximate size per agg info
	// Account for fileCounter (negligible but included for completeness)
	size += 8
	return size
}

func (sm *SpillManager) readHeader(reader io.Reader) (struct{ GroupCount, AggCount int32 }, error) {
	headerExt, err := sm.readHeaderWithHashTable(reader)
	if err != nil {
		return struct{ GroupCount, AggCount int32 }{}, err
	}

	return struct{ GroupCount, AggCount int32 }{
		GroupCount: headerExt.GroupCount,
		AggCount:   headerExt.AggCount,
	}, nil
}

func (sm *SpillManager) readHeaderWithHashTable(reader io.Reader) (struct {
	GroupCount     int32
	AggCount       int32
	HasHashTable   int32
	HashTableFlags int32
	KeyWidth       int32
	Reserved       int32
}, error) {
	var header struct {
		GroupCount     int32
		AggCount       int32
		HasHashTable   int32
		HashTableFlags int32
		KeyWidth       int32
		Reserved       int32
	}

	// Try to read the extended header first
	if err := binary.Read(reader, binary.BigEndian, &header); err != nil {
		// If it fails, try to read the old format
		var oldHeader struct {
			GroupCount int32
			AggCount   int32
		}

		// Reset reader position would be ideal, but since we can't,
		// we'll assume the error means old format and return appropriate default
		if err := binary.Read(reader, binary.BigEndian, &oldHeader); err != nil {
			return header, err
		}

		header.GroupCount = oldHeader.GroupCount
		header.AggCount = oldHeader.AggCount
		header.HasHashTable = 0
		header.HashTableFlags = 0
		header.KeyWidth = 0
		header.Reserved = 0
	}

	return header, nil
}

func (sm *SpillManager) readGroupBatches(reader io.Reader, count int32) ([]*batch.Batch, error) {
	groups := make([]*batch.Batch, 0, count)

	for i := int32(0); i < count; i++ {
		batch, err := sm.readSingleBatch(reader)
		if err != nil {
			cleanupResources(sm.proc.Mp(), groups, nil)
			return nil, err
		}
		groups = append(groups, batch)
	}

	return groups, nil
}

func (sm *SpillManager) readAggStates(reader io.Reader, count int32) ([]aggexec.AggFuncExec, error) {
	aggs := make([]aggexec.AggFuncExec, 0, count)

	for i := int32(0); i < count; i++ {
		agg, err := sm.readSingleAgg(reader)
		if err != nil {
			cleanupResources(sm.proc.Mp(), nil, aggs)
			return nil, err
		}
		aggs = append(aggs, agg)
	}

	return aggs, nil
}

func (sm *SpillManager) readSingleBatch(reader io.Reader) (*batch.Batch, error) {
	data, err := sm.readSizedData(reader)
	if err != nil {
		return nil, err
	}

	bat := batch.NewWithSize(0)
	if err := bat.UnmarshalBinary(data); err != nil {
		bat.Clean(sm.proc.Mp())
		return nil, err
	}

	return bat, nil
}

func (sm *SpillManager) readSingleAgg(reader io.Reader) (aggexec.AggFuncExec, error) {
	data, err := sm.readSizedData(reader)
	if err != nil {
		return nil, err
	}

	return aggexec.UnmarshalAggFuncExec(sm.proc, data)
}

func (sm *SpillManager) readSizedData(reader io.Reader) ([]byte, error) {
	var size int32
	if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("failed to read data size: %v", err)
	}

	if size < 0 {
		return nil, moerr.NewInternalErrorNoCtxf("invalid negative data size: %d", size)
	}

	const maxReasonableSize = 100 * 1024 * 1024
	if size > maxReasonableSize {
		return nil, moerr.NewInternalErrorNoCtxf("data size too large: %d bytes (max %d)", size, maxReasonableSize)
	}

	if size == 0 {
		return []byte{}, nil
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("failed to read %d bytes of data: %v", size, err)
	}

	return data, nil
}

func cleanupResources(mp *mpool.MPool, groups []*batch.Batch, aggs []aggexec.AggFuncExec) {
	for _, batch := range groups {
		if batch != nil {
			batch.Clean(mp)
		}
	}
	for _, agg := range aggs {
		if agg != nil {
			agg.Free()
		}
	}
}

func (sm *SpillManager) writeHashTableData(writer io.Writer, hashTableData []byte) error {
	if len(hashTableData) == 0 {
		return nil
	}

	return sm.writeSizedData(writer, func() ([]byte, error) {
		return hashTableData, nil
	})
}

func (sm *SpillManager) readHashTableData(reader io.Reader) ([]byte, error) {
	return sm.readSizedData(reader)
}
