// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package group

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	distinctSpillMagic            = uint64(0x4453545350494c4c) // "DSTSPILL"
	distinctSpillVersion          = uint16(1)
	distinctSpillKindKey          = uint16(1)
	distinctSpillKindContribution = uint16(2)
	distinctSpillNumBuckets       = 8
	distinctSpillMaskBits         = 3
)

type distinctSpillController struct {
	ctr                    *container
	root                   [spillNumBuckets]*spillBucket
	result                 [spillMaxPass][]*spillBucket
	resultSplit            [spillMaxPass - 1][]bool
	record                 reusableSpillBuffer
	copy                   reusableSpillBuffer
	sortKey                reusableSpillBuffer
	mergeLeft              reusableSpillBuffer
	mergeRight             reusableSpillBuffer
	rootName               string
	resultName             string
	keys                   uint64
	bytes                  uint64
	closed                 bool
	hashForUT              func(groupHash uint64, aggregate int, payload []byte) uint64
	forceExternalSortForUT bool
	sortArenaBytesForUT    int
	appliedLevel1          [spillNumBuckets]bool
	appliedLevel2          [spillNumBuckets * spillNumBuckets]bool
	appliedLevel3          [spillNumBuckets * spillNumBuckets * spillNumBuckets]bool
	repartitions           uint64
	externalSorts          uint64
	partialPending         [spillMaxPass * spillNumBuckets]*spillBucket
	partialPendingCount    int
	partialActive          *spillBucket
	partialActiveOffset    int64
	stats                  *process.OperatorStats
	uniqueKeys             uint64
	contributionRecords    uint64
	contributionReads      uint64
	partialContinuations   uint64
	completionRecorded     bool
}

type preparedDistinctDrain struct {
	aggregate int
	state     aggexec.ExactCountDistinctSpillState
	drain     aggexec.DistinctArgumentDrain
}

func newDistinctSpillController(ctr *container) (*distinctSpillController, error) {
	if ctr == nil || ctr.mp == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	controller := &distinctSpillController{
		ctr:        ctr,
		rootName:   fmt.Sprintf("distinct_spill_%s", id.String()),
		resultName: fmt.Sprintf("distinct_result_%s", id.String()),
	}
	for bucket := range controller.root {
		controller.root[bucket] = &spillBucket{
			name: fmt.Sprintf("%s_%d", controller.rootName, bucket),
		}
	}
	partitions := spillNumBuckets
	for level := range controller.result {
		controller.result[level] = make([]*spillBucket, partitions)
		if level < len(controller.resultSplit) {
			controller.resultSplit[level] = make([]bool, partitions)
		}
		partitions *= spillNumBuckets
	}
	for bucket := range controller.result[0] {
		controller.result[0][bucket] = &spillBucket{
			lv:      1,
			name:    fmt.Sprintf("%s_1_%d", controller.resultName, bucket),
			path:    [spillMaxPass]uint8{uint8(bucket)},
			pathLen: 1,
		}
	}
	return controller, nil
}

func (c *distinctSpillController) close() {
	if c == nil || c.closed {
		return
	}
	c.closed = true
	for bucket := range c.root {
		if c.root[bucket] != nil {
			_ = c.root[bucket].free()
			c.root[bucket] = nil
		}
	}
	for level := range c.result {
		for bucket := range c.result[level] {
			if c.result[level][bucket] != nil {
				_ = c.result[level][bucket].free()
				c.result[level][bucket] = nil
			}
		}
		c.result[level] = nil
	}
	for c.partialPendingCount > 0 {
		c.partialPendingCount--
		if c.partialPending[c.partialPendingCount] != nil {
			_ = c.partialPending[c.partialPendingCount].free()
			c.partialPending[c.partialPendingCount] = nil
		}
	}
	if c.partialActive != nil {
		_ = c.partialActive.free()
		c.partialActive = nil
	}
	c.partialActiveOffset = 0
	if c.record != nil {
		c.record.Free()
		c.record = nil
	}
	if c.copy != nil {
		c.copy.Free()
		c.copy = nil
	}
	if c.sortKey != nil {
		c.sortKey.Free()
		c.sortKey = nil
	}
	if c.mergeLeft != nil {
		c.mergeLeft.Free()
		c.mergeLeft = nil
	}
	if c.mergeRight != nil {
		c.mergeRight.Free()
		c.mergeRight = nil
	}
	c.ctr = nil
	c.hashForUT = nil
	c.forceExternalSortForUT = false
	c.sortArenaBytesForUT = 0
	clear(c.appliedLevel1[:])
	clear(c.appliedLevel2[:])
	clear(c.appliedLevel3[:])
	for level := range c.resultSplit {
		c.resultSplit[level] = nil
	}
}

func (c *distinctSpillController) recordCompletion() {
	if c == nil || c.completionRecorded {
		return
	}
	c.completionRecorded = true
	if c.stats == nil {
		return
	}
	c.stats.AddExtraStat(
		"GroupDistinctSpillUniqueKeys", int64(min(c.uniqueKeys, math.MaxInt64)))
	duplicates := uint64(0)
	if c.keys >= c.uniqueKeys {
		duplicates = c.keys - c.uniqueKeys
	}
	c.stats.AddExtraStat(
		"GroupDistinctSpillDuplicatesRemoved",
		int64(min(duplicates, math.MaxInt64)),
	)
	c.stats.AddExtraStat(
		"GroupDistinctSpillContributionRecords",
		int64(min(c.contributionRecords, math.MaxInt64)),
	)
	c.stats.AddExtraStat(
		"GroupDistinctSpillContributionReads",
		int64(min(c.contributionReads, math.MaxInt64)),
	)
	c.stats.AddExtraStat(
		"GroupDistinctSpillPartialContinuations",
		int64(min(c.partialContinuations, math.MaxInt64)),
	)
}

func (c *distinctSpillController) pushPartialChildren(
	children *[spillNumBuckets]*spillBucket,
) error {
	if c == nil || children == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	for bucket := len(children) - 1; bucket >= 0; bucket-- {
		child := children[bucket]
		if child == nil || child.cnt == 0 {
			continue
		}
		if c.partialPendingCount >= len(c.partialPending) {
			return moerr.NewInternalErrorNoCtx(
				"distinct partial partition stack overflow")
		}
		c.partialPending[c.partialPendingCount] = child
		c.partialPendingCount++
		children[bucket] = nil
	}
	return nil
}

func (c *distinctSpillController) takePartialPartition() *spillBucket {
	if c == nil {
		return nil
	}
	if c.partialPendingCount > 0 {
		c.partialPendingCount--
		partition := c.partialPending[c.partialPendingCount]
		c.partialPending[c.partialPendingCount] = nil
		return partition
	}
	for bucket := range c.root {
		if c.root[bucket] != nil && c.root[bucket].cnt > 0 {
			partition := c.root[bucket]
			c.root[bucket] = nil
			return partition
		}
	}
	return nil
}

func (c *distinctSpillController) hash(
	groupHash uint64,
	aggregate int,
	payload []byte,
) uint64 {
	if c.hashForUT != nil {
		return c.hashForUT(groupHash, aggregate, payload)
	}
	hash := keycodec.HashCombine(groupHash, xxhash.Sum64(payload))
	return keycodec.HashCombine(hash, uint64(aggregate)+1)
}

func (c *distinctSpillController) ensureRecordBuffer() error {
	if c.record != nil {
		return nil
	}
	buffer, err := newGroupSpillBuffer(c.ctr, GroupAllocationSiteDistinctRecord)
	if err != nil {
		return err
	}
	c.record = buffer
	return nil
}

func (c *distinctSpillController) writeRecord(
	target io.Writer,
	routeHash uint64,
	groupHash uint64,
	aggregate int,
	groups *batch.Batch,
	row int32,
	payload []byte,
) (int64, error) {
	if c == nil || c.closed || target == nil || groups == nil ||
		aggregate < 0 || row < 0 || int(row) >= groups.RowCount() {
		return 0, moerr.NewInvalidInputNoCtx("invalid distinct spill record")
	}
	if err := c.ensureRecordBuffer(); err != nil {
		return 0, err
	}
	c.record.Reset()
	if err := types.WriteUint64(c.record, routeHash); err != nil {
		return 0, err
	}
	if err := types.WriteUint64(c.record, groupHash); err != nil {
		return 0, err
	}
	if aggregate > math.MaxInt32 {
		return 0, moerr.NewInvalidInputNoCtx(
			"distinct spill aggregate ordinal exceeds wire format")
	}
	if err := types.WriteInt32(c.record, int32(aggregate)); err != nil {
		return 0, err
	}
	if err := appendSpillGroupByRows(c.record, groups, []int32{row}); err != nil {
		return 0, err
	}
	if err := types.WriteSizeBytes(payload, c.record); err != nil {
		return 0, err
	}
	if c.record.Len() > math.MaxInt32 {
		return 0, moerr.NewInvalidInputNoCtx(
			"distinct spill record exceeds wire format")
	}

	record := spillRecordWriter{target: target}
	if err := types.WriteUint64(&record, distinctSpillMagic); err != nil {
		return record.written, err
	}
	if err := types.WriteUint16(&record, distinctSpillVersion); err != nil {
		return record.written, err
	}
	if err := types.WriteUint16(&record, distinctSpillKindKey); err != nil {
		return record.written, err
	}
	length := int32(c.record.Len())
	if err := types.WriteInt32(&record, length); err != nil {
		return record.written, err
	}
	if n, err := record.Write(c.record.Bytes()); err != nil {
		return record.written, err
	} else if n != c.record.Len() {
		return record.written, io.ErrShortWrite
	}
	if err := types.WriteInt32(&record, length); err != nil {
		return record.written, err
	}
	if err := types.WriteUint64(&record, distinctSpillMagic); err != nil {
		return record.written, err
	}
	return record.written, nil
}

func (c *distinctSpillController) readRecord(
	reader io.Reader,
	groups *batch.Batch,
) (
	routeHash uint64,
	groupHash uint64,
	aggregate int,
	payload []byte,
	eof bool,
	err error,
) {
	if c == nil || c.closed || reader == nil || groups == nil {
		err = moerr.NewInvalidInputNoCtx("invalid distinct spill reader")
		return
	}
	magic, readErr := types.ReadUint64(reader)
	if readErr != nil {
		if readErr == io.EOF {
			eof = true
			return
		}
		err = readErr
		return
	}
	if magic != distinctSpillMagic {
		err = moerr.NewInvalidInputNoCtx("invalid distinct spill header")
		return
	}
	version, readErr := types.ReadUint16(reader)
	if readErr != nil {
		err = readErr
		return
	}
	if version != distinctSpillVersion {
		err = moerr.NewInvalidInputNoCtxf(
			"unsupported distinct spill version %d", version)
		return
	}
	kind, readErr := types.ReadUint16(reader)
	if readErr != nil {
		err = readErr
		return
	}
	if kind != distinctSpillKindKey {
		err = moerr.NewInvalidInputNoCtxf("invalid distinct spill kind %d", kind)
		return
	}
	length, readErr := types.ReadInt32AsInt(reader)
	if readErr != nil {
		err = readErr
		return
	}
	if length <= 0 {
		err = moerr.NewInvalidInputNoCtx("invalid distinct spill record length")
		return
	}
	if err = c.ensureRecordBuffer(); err != nil {
		return
	}
	if err = c.record.Resize(length); err != nil {
		return
	}
	if _, err = io.ReadFull(reader, c.record.Bytes()); err != nil {
		return
	}
	trailingLength, readErr := types.ReadInt32AsInt(reader)
	if readErr != nil {
		err = readErr
		return
	}
	if trailingLength != length {
		err = moerr.NewInvalidInputNoCtx(
			"distinct spill record length trailer mismatch")
		return
	}
	trailingMagic, readErr := types.ReadUint64(reader)
	if readErr != nil {
		err = readErr
		return
	}
	if trailingMagic != distinctSpillMagic {
		err = moerr.NewInvalidInputNoCtx("invalid distinct spill trailer")
		return
	}

	groups.CleanOnlyData()
	payloadReader := bytes.NewReader(c.record.Bytes())
	if routeHash, err = types.ReadUint64(payloadReader); err != nil {
		return
	}
	if groupHash, err = types.ReadUint64(payloadReader); err != nil {
		return
	}
	aggregate32, readErr := types.ReadInt32(payloadReader)
	if readErr != nil {
		err = readErr
		return
	}
	if aggregate32 < 0 {
		err = moerr.NewInvalidInputNoCtx(
			"invalid distinct spill aggregate ordinal")
		return
	}
	aggregate = int(aggregate32)
	if err = unmarshalSpillGroupByRows(payloadReader, groups, 1, c.ctr.mp); err != nil {
		return
	}
	payloadLength, readErr := types.ReadInt32AsInt(payloadReader)
	if readErr != nil {
		err = readErr
		return
	}
	if payloadLength < 0 || payloadLength > payloadReader.Len() {
		err = moerr.NewInvalidInputNoCtx(
			"invalid distinct spill argument length")
		return
	}
	offset := len(c.record.Bytes()) - payloadReader.Len()
	payload = c.record.Bytes()[offset : offset+payloadLength]
	if _, err = payloadReader.Seek(int64(payloadLength), io.SeekCurrent); err != nil {
		return
	}
	if payloadReader.Len() != 0 {
		err = moerr.NewInvalidInputNoCtx(
			"distinct spill record has trailing payload")
		return
	}
	return
}

func distinctGroupBucket(groupHash uint64, level int) int {
	if level <= 0 {
		return 0
	}
	multiplier := uint64(0x9e3779b97f4a7c15) + uint64(level)*2
	return int(((groupHash * multiplier) >> (64 - spillMaskBits)) &
		(spillNumBuckets - 1))
}

func (c *distinctSpillController) writeContribution(
	proc *process.Process,
	spillfs fileservice.MutableFileService,
	groupHash uint64,
	aggregate int,
	groups *batch.Batch,
	row int32,
	count uint64,
) error {
	if c == nil || c.closed || proc == nil || spillfs == nil || groups == nil ||
		aggregate < 0 || aggregate > math.MaxInt32 || row < 0 ||
		int(row) >= groups.RowCount() || count == 0 || count > math.MaxInt64 {
		return moerr.NewInvalidInputNoCtx("invalid distinct contribution")
	}
	if err := c.ensureRecordBuffer(); err != nil {
		return err
	}
	c.record.Reset()
	if err := types.WriteUint64(c.record, groupHash); err != nil {
		return err
	}
	if err := types.WriteInt32(c.record, int32(aggregate)); err != nil {
		return err
	}
	if err := appendSpillGroupByRows(c.record, groups, []int32{row}); err != nil {
		return err
	}
	if err := types.WriteUint64(c.record, count); err != nil {
		return err
	}
	if c.record.Len() <= 0 || c.record.Len() > math.MaxInt32 {
		return moerr.NewInvalidInputNoCtx(
			"distinct contribution exceeds wire format")
	}
	target := c.result[0][distinctGroupBucket(groupHash, 1)]
	if target == nil {
		return moerr.NewInternalErrorNoCtx(
			"distinct contribution partition is closed")
	}
	if err := c.writeContributionEnvelope(proc, spillfs, target); err != nil {
		return err
	}
	c.contributionRecords++
	return nil
}

func (c *distinctSpillController) writeContributionEnvelope(
	proc *process.Process,
	spillfs fileservice.MutableFileService,
	target *spillBucket,
) error {
	if c == nil || c.closed || proc == nil || spillfs == nil || target == nil ||
		c.record == nil || c.record.Len() <= 0 || c.record.Len() > math.MaxInt32 {
		return moerr.NewInvalidInputNoCtx(
			"invalid distinct contribution envelope")
	}
	if target.file == nil {
		if err := c.ctr.openSpillBucket(proc, spillfs, target); err != nil {
			return err
		}
	}
	record := spillRecordWriter{target: target.writer}
	if err := types.WriteUint64(&record, distinctSpillMagic); err != nil {
		return err
	}
	if err := types.WriteUint16(&record, distinctSpillVersion); err != nil {
		return err
	}
	if err := types.WriteUint16(&record, distinctSpillKindContribution); err != nil {
		return err
	}
	length := int32(c.record.Len())
	if err := types.WriteInt32(&record, length); err != nil {
		return err
	}
	if _, err := record.Write(c.record.Bytes()); err != nil {
		return err
	}
	if err := types.WriteInt32(&record, length); err != nil {
		return err
	}
	if err := types.WriteUint64(&record, distinctSpillMagic); err != nil {
		return err
	}
	target.cnt++
	return nil
}

func (c *distinctSpillController) flushContributionWriters() error {
	if c == nil || c.closed {
		return mpool.ErrAllocationAccountInvalid
	}
	for level := range c.result {
		for bucket := range c.result[level] {
			partition := c.result[level][bucket]
			if partition == nil || partition.writer == nil {
				continue
			}
			if err := partition.flushWriter(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *distinctSpillController) readContribution(
	reader io.Reader,
	groups *batch.Batch,
) (
	groupHash uint64,
	aggregate int,
	count uint64,
	eof bool,
	err error,
) {
	if c == nil || c.closed || reader == nil || groups == nil {
		err = moerr.NewInvalidInputNoCtx("invalid distinct contribution reader")
		return
	}
	magic, readErr := types.ReadUint64(reader)
	if readErr != nil {
		if readErr == io.EOF {
			eof = true
			return
		}
		err = readErr
		return
	}
	if magic != distinctSpillMagic {
		err = moerr.NewInvalidInputNoCtx(
			"invalid distinct contribution header")
		return
	}
	version, readErr := types.ReadUint16(reader)
	if readErr != nil {
		err = readErr
		return
	}
	kind, readErr := types.ReadUint16(reader)
	if readErr != nil {
		err = readErr
		return
	}
	if version != distinctSpillVersion || kind != distinctSpillKindContribution {
		err = moerr.NewInvalidInputNoCtx(
			"invalid distinct contribution version or kind")
		return
	}
	length, readErr := types.ReadInt32AsInt(reader)
	if readErr != nil {
		err = readErr
		return
	}
	if length <= 0 {
		err = moerr.NewInvalidInputNoCtx(
			"invalid distinct contribution length")
		return
	}
	if err = c.ensureRecordBuffer(); err != nil {
		return
	}
	if err = c.record.Resize(length); err != nil {
		return
	}
	if _, err = io.ReadFull(reader, c.record.Bytes()); err != nil {
		return
	}
	trailingLength, readErr := types.ReadInt32AsInt(reader)
	if readErr != nil {
		err = readErr
		return
	}
	trailingMagic, readErr := types.ReadUint64(reader)
	if readErr != nil {
		err = readErr
		return
	}
	if trailingLength != length || trailingMagic != distinctSpillMagic {
		err = moerr.NewInvalidInputNoCtx(
			"invalid distinct contribution trailer")
		return
	}
	groups.CleanOnlyData()
	payloadReader := bytes.NewReader(c.record.Bytes())
	if groupHash, err = types.ReadUint64(payloadReader); err != nil {
		return
	}
	aggregate32, readErr := types.ReadInt32(payloadReader)
	if readErr != nil {
		err = readErr
		return
	}
	if aggregate32 < 0 {
		err = moerr.NewInvalidInputNoCtx(
			"invalid distinct contribution aggregate")
		return
	}
	aggregate = int(aggregate32)
	if err = unmarshalSpillGroupByRows(payloadReader, groups, 1, c.ctr.mp); err != nil {
		return
	}
	if count, err = types.ReadUint64(payloadReader); err != nil {
		return
	}
	if count == 0 || count > math.MaxInt64 || payloadReader.Len() != 0 {
		err = moerr.NewInvalidInputNoCtx(
			"invalid distinct contribution payload")
		return
	}
	c.contributionReads++
	return
}

func (c *distinctSpillController) newPrivateWave() ([spillNumBuckets]*spillBucket, error) {
	var wave [spillNumBuckets]*spillBucket
	id, err := uuid.NewV7()
	if err != nil {
		return wave, err
	}
	for bucket := range wave {
		wave[bucket] = &spillBucket{
			name: fmt.Sprintf("%s_wave_%s_%d", c.rootName, id.String(), bucket),
		}
	}
	return wave, nil
}

func freeDistinctWave(wave *[spillNumBuckets]*spillBucket) {
	if wave == nil {
		return
	}
	for bucket := range wave {
		if wave[bucket] != nil {
			_ = wave[bucket].free()
			wave[bucket] = nil
		}
	}
}

func (c *distinctSpillController) ensureCopyBuffer() error {
	if c.copy == nil {
		buffer, err := newGroupSpillBuffer(c.ctr, GroupAllocationSiteDistinctCopy)
		if err != nil {
			return err
		}
		c.copy = buffer
	}
	if c.copy.Cap() < spillWrBufSize {
		if err := c.copy.Resize(spillWrBufSize); err != nil {
			return err
		}
	}
	return nil
}

func (c *distinctSpillController) mergeCommittedWave(
	proc *process.Process,
	spillfs fileservice.MutableFileService,
	wave *[spillNumBuckets]*spillBucket,
) error {
	if c == nil || c.closed || proc == nil || spillfs == nil || wave == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := c.ensureCopyBuffer(); err != nil {
		return err
	}
	for bucket := range wave {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return err
		}
		source := wave[bucket]
		if source == nil || source.cnt == 0 {
			continue
		}
		if err := source.flushWriter(); err != nil {
			return err
		}
		if _, err := source.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		target := c.root[bucket]
		if err := c.ctr.openSpillBucket(proc, spillfs, target); err != nil {
			return err
		}
		c.copy.Reset()
		if err := c.copy.Resize(spillWrBufSize); err != nil {
			return err
		}
		written, err := io.CopyBuffer(target.writer, source.file, c.copy.Bytes())
		if err != nil {
			return err
		}
		if written < 0 {
			return moerr.NewInternalErrorNoCtx("invalid distinct spill copy count")
		}
		target.cnt += source.cnt
		c.bytes += uint64(written)
		c.keys += uint64(source.cnt)
		if err := source.free(); err != nil {
			return err
		}
		wave[bucket] = nil
	}
	return nil
}

func (c *distinctSpillController) repartition(
	proc *process.Process,
	partition *spillBucket,
	groups *batch.Batch,
) (
	children [spillNumBuckets]*spillBucket,
	progress bool,
	retErr error,
) {
	if c == nil || c.closed || proc == nil || partition == nil ||
		partition.file == nil || groups == nil || partition.lv >= spillMaxPass {
		return children, false, mpool.ErrAllocationAccountInvalid
	}
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return children, false, err
	}
	children, err = c.newPrivateWave()
	if err != nil {
		return children, false, err
	}
	success := false
	defer func() {
		if !success {
			freeDistinctWave(&children)
		}
	}()
	for bucket := range children {
		children[bucket].lv = partition.lv + 1
	}
	if err := partition.flushWriter(); err != nil {
		return children, false, err
	}
	if _, err := partition.file.Seek(0, io.SeekStart); err != nil {
		return children, false, err
	}
	reader, err := newGroupSpillReader(c.ctr, partition.file, proc.Ctx)
	if err != nil {
		return children, false, err
	}
	defer reader.Free()
	for {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return children, false, err
		}
		routeHash, groupHash, aggregate, payload, eof, err := c.readRecord(
			reader, groups)
		if err != nil {
			return children, false, err
		}
		if eof {
			break
		}
		shift := uint((partition.lv + 1) * distinctSpillMaskBits)
		bucket := int((routeHash >> shift) & (distinctSpillNumBuckets - 1))
		target := children[bucket]
		if target.file == nil {
			if err := c.ctr.openSpillBucket(proc, spillfs, target); err != nil {
				return children, false, err
			}
		}
		if _, err := c.writeRecord(
			target.writer,
			routeHash,
			groupHash,
			aggregate,
			groups,
			0,
			payload,
		); err != nil {
			return children, false, err
		}
		target.cnt++
	}
	nonEmpty := 0
	var largest int64
	for bucket := range children {
		if children[bucket].cnt == 0 {
			continue
		}
		nonEmpty++
		largest = max(largest, children[bucket].cnt)
		if err := children[bucket].flushWriter(); err != nil {
			return children, false, err
		}
	}
	progress = nonEmpty > 1 && largest < partition.cnt
	if !progress {
		freeDistinctWave(&children)
		return children, false, nil
	}
	c.repartitions++
	if c.stats != nil {
		c.stats.AddExtraStat("GroupDistinctSpillRepartitions", 1)
		c.stats.SetMaxExtraStat(
			"GroupDistinctSpillMaxLevel", int64(partition.lv+1))
	}
	success = true
	return children, true, nil
}

func (c *distinctSpillController) newBuffer(
	site mpool.AllocationSite,
) (reusableSpillBuffer, error) {
	return newGroupSpillBuffer(c.ctr, site)
}

func (c *distinctSpillController) ensureSortBuffers() error {
	var err error
	if c.sortKey == nil {
		c.sortKey, err = c.newBuffer(GroupAllocationSiteDistinctRecord)
		if err != nil {
			return err
		}
	}
	if c.mergeLeft == nil {
		c.mergeLeft, err = c.newBuffer(GroupAllocationSiteDistinctCopy)
		if err != nil {
			return err
		}
	}
	if c.mergeRight == nil {
		c.mergeRight, err = c.newBuffer(GroupAllocationSiteDistinctCopy)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *distinctSpillController) sortArenaCapacity() int {
	const (
		minimum = 64 * 1024
		maximum = 8 * 1024 * 1024
	)
	if c != nil && c.sortArenaBytesForUT > 0 {
		return c.sortArenaBytesForUT
	}
	if c == nil || c.ctr == nil || c.ctr.spillMem < 10000 {
		return 1024 * 1024
	}
	capacity := c.ctr.spillMem / 4
	if capacity < minimum {
		capacity = minimum
	}
	if capacity > maximum {
		capacity = maximum
	}
	return int(capacity)
}

func (c *distinctSpillController) allocateSortArena() (
	[]byte,
	*arenaskl.Skiplist,
	error,
) {
	if c == nil || c.ctr == nil || c.ctr.mp == nil ||
		c.ctr.allocationAccount == nil {
		return nil, nil, mpool.ErrAllocationAccountInvalid
	}
	capacity := c.sortArenaCapacity()
	minimum := 64 * 1024
	if c.sortArenaBytesForUT > 0 {
		minimum = c.sortArenaBytesForUT
	}
	for capacity >= minimum {
		buffer, err := c.ctr.mp.AllocAccountedWithCapacityClass(
			capacity,
			c.ctr.allocationAccount,
			mpool.AllocationOwnerGroup,
			GroupAllocationSiteDistinctRecord,
			c.ctr.recoveryCapacityClass,
		)
		if err == nil {
			arena := arenaskl.NewArena(buffer)
			return buffer, arenaskl.NewSkiplist(arena, bytes.Compare), nil
		}
		if !mpool.IsRetryableAllocationCapacity(err) {
			return nil, nil, err
		}
		capacity /= 2
	}
	return nil, nil, mpool.ErrAllocationAccountCapacity
}

func (c *distinctSpillController) newSortRun(level int) (*spillBucket, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	return &spillBucket{
		lv:   level,
		name: fmt.Sprintf("%s_sort_%s", c.rootName, id.String()),
	}, nil
}

func writeDistinctSortKey(writer io.Writer, key []byte) error {
	if len(key) == 0 || uint64(len(key)) > math.MaxInt32 {
		return moerr.NewInvalidInputNoCtx("invalid distinct sort key length")
	}
	return types.WriteSizeBytes(key, writer)
}

func readDistinctSortKey(
	reader io.Reader,
	buffer reusableSpillBuffer,
) (key []byte, eof bool, err error) {
	if reader == nil || buffer == nil {
		return nil, false, mpool.ErrAllocationAccountInvalid
	}
	length, err := types.ReadInt32AsInt(reader)
	if err != nil {
		if err == io.EOF {
			return nil, true, nil
		}
		return nil, false, err
	}
	if length <= 0 {
		return nil, false, moerr.NewInvalidInputNoCtx(
			"invalid distinct sort key length")
	}
	if err := buffer.Resize(length); err != nil {
		return nil, false, err
	}
	if _, err := io.ReadFull(reader, buffer.Bytes()); err != nil {
		return nil, false, err
	}
	return buffer.Bytes(), false, nil
}

func (c *distinctSpillController) flushSortSet(
	proc *process.Process,
	spillfs fileservice.MutableFileService,
	set *arenaskl.Skiplist,
) (*spillBucket, error) {
	if c == nil || proc == nil || spillfs == nil || set == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	run, err := c.newSortRun(0)
	if err != nil {
		return nil, err
	}
	if err = c.ctr.openSpillBucket(proc, spillfs, run); err != nil {
		_ = run.free()
		return nil, err
	}
	it := set.NewIter(nil, nil)
	defer it.Close()
	for ok, key, _ := it.First(); ok; ok, key, _ = it.Next() {
		if err, canceled := vm.CancelCheck(proc); canceled {
			_ = run.free()
			return nil, err
		}
		if err := writeDistinctSortKey(run.writer, key); err != nil {
			_ = run.free()
			return nil, err
		}
		run.cnt++
	}
	if run.cnt == 0 {
		_ = run.free()
		return nil, moerr.NewInternalErrorNoCtx("empty distinct sort run")
	}
	if err := run.flushWriter(); err != nil {
		_ = run.free()
		return nil, err
	}
	return run, nil
}

func (c *distinctSpillController) mergeSortRuns(
	proc *process.Process,
	spillfs fileservice.MutableFileService,
	left *spillBucket,
	right *spillBucket,
) (*spillBucket, error) {
	if c == nil || proc == nil || spillfs == nil || left == nil || right == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if err := c.ensureSortBuffers(); err != nil {
		return nil, err
	}
	if err := left.flushWriter(); err != nil {
		return nil, err
	}
	if err := right.flushWriter(); err != nil {
		return nil, err
	}
	if _, err := left.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := right.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	leftReader, err := newGroupSpillReader(c.ctr, left.file, proc.Ctx)
	if err != nil {
		return nil, err
	}
	defer leftReader.Free()
	rightReader, err := newGroupSpillReader(c.ctr, right.file, proc.Ctx)
	if err != nil {
		return nil, err
	}
	defer rightReader.Free()
	out, err := c.newSortRun(max(left.lv, right.lv) + 1)
	if err != nil {
		return nil, err
	}
	if err = c.ctr.openSpillBucket(proc, spillfs, out); err != nil {
		_ = out.free()
		return nil, err
	}
	leftKey, leftEOF, err := readDistinctSortKey(leftReader, c.mergeLeft)
	if err != nil {
		_ = out.free()
		return nil, err
	}
	rightKey, rightEOF, err := readDistinctSortKey(rightReader, c.mergeRight)
	if err != nil {
		_ = out.free()
		return nil, err
	}
	write := func(key []byte) error {
		if err := writeDistinctSortKey(out.writer, key); err != nil {
			return err
		}
		out.cnt++
		return nil
	}
	for !leftEOF || !rightEOF {
		if err, canceled := vm.CancelCheck(proc); canceled {
			_ = out.free()
			return nil, err
		}
		comparison := 0
		if leftEOF {
			comparison = 1
		} else if rightEOF {
			comparison = -1
		} else {
			comparison = bytes.Compare(leftKey, rightKey)
		}
		if comparison < 0 {
			if err := write(leftKey); err != nil {
				_ = out.free()
				return nil, err
			}
			leftKey, leftEOF, err = readDistinctSortKey(leftReader, c.mergeLeft)
		} else if comparison > 0 {
			if err := write(rightKey); err != nil {
				_ = out.free()
				return nil, err
			}
			rightKey, rightEOF, err = readDistinctSortKey(rightReader, c.mergeRight)
		} else {
			if err := write(leftKey); err != nil {
				_ = out.free()
				return nil, err
			}
			leftKey, leftEOF, err = readDistinctSortKey(leftReader, c.mergeLeft)
			if err == nil {
				rightKey, rightEOF, err = readDistinctSortKey(
					rightReader, c.mergeRight)
			}
		}
		if err != nil {
			_ = out.free()
			return nil, err
		}
	}
	if err := out.flushWriter(); err != nil {
		_ = out.free()
		return nil, err
	}
	if err := left.free(); err != nil {
		_ = out.free()
		return nil, err
	}
	if err := right.free(); err != nil {
		_ = out.free()
		return nil, err
	}
	return out, nil
}

func (c *distinctSpillController) addSortRun(
	proc *process.Process,
	spillfs fileservice.MutableFileService,
	slots *[64]*spillBucket,
	run *spillBucket,
) error {
	for level := 0; level < len(slots); level++ {
		if slots[level] == nil {
			run.lv = level
			slots[level] = run
			return nil
		}
		merged, err := c.mergeSortRuns(proc, spillfs, slots[level], run)
		if err != nil {
			return err
		}
		slots[level] = nil
		run = merged
	}
	_ = run.free()
	return moerr.NewInternalErrorNoCtx("distinct external sort level overflow")
}

func (c *distinctSpillController) externalSortH0Partition(
	proc *process.Process,
	partition *spillBucket,
) (counts []uint64, retErr error) {
	if c == nil || c.closed || proc == nil || partition == nil ||
		partition.file == nil || c.ctr == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	c.externalSorts++
	if c.stats != nil {
		c.stats.AddExtraStat("GroupDistinctSpillExternalSorts", 1)
	}
	if err := c.ensureSortBuffers(); err != nil {
		return nil, err
	}
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}
	if err := partition.flushWriter(); err != nil {
		return nil, err
	}
	if _, err := partition.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	groups, err := c.ctr.createNewGroupByBatchWithAllocation(
		nil, 1, c.ctr.spillGroupByAllocation)
	if err != nil {
		return nil, err
	}
	defer groups.Clean(c.ctr.mp)
	reader, err := newGroupSpillReader(c.ctr, partition.file, proc.Ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Free()
	aggregateCount := len(c.ctr.aggExprs)
	if aggregateCount == 0 {
		return nil, moerr.NewInternalErrorNoCtx(
			"distinct sort aggregate shape is empty")
	}

	var slots [64]*spillBucket
	defer func() {
		for i := range slots {
			if slots[i] != nil {
				_ = slots[i].free()
				slots[i] = nil
			}
		}
	}()
	buffer, set, err := c.allocateSortArena()
	if err != nil {
		return nil, err
	}
	defer func() {
		if cap(buffer) > 0 {
			c.ctr.mp.Free(buffer)
		}
	}()
	setKeys := 0
	flush := func(replace bool) error {
		if setKeys == 0 {
			return nil
		}
		run, err := c.flushSortSet(proc, spillfs, set)
		if err != nil {
			return err
		}
		if err := c.addSortRun(proc, spillfs, &slots, run); err != nil {
			_ = run.free()
			return err
		}
		c.ctr.mp.Free(buffer)
		buffer = nil
		if replace {
			buffer, set, err = c.allocateSortArena()
			if err != nil {
				return err
			}
		}
		setKeys = 0
		return nil
	}
	for {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return nil, err
		}
		_, _, aggregate, payload, eof, err := c.readRecord(reader, groups)
		if err != nil {
			return nil, err
		}
		if eof {
			break
		}
		if aggregate < 0 || aggregate >= aggregateCount {
			return nil, moerr.NewInvalidInputNoCtx(
				"distinct sort aggregate ordinal out of range")
		}
		if len(payload) > math.MaxInt-4 {
			return nil, moerr.NewInvalidInputNoCtx(
				"distinct sort key exceeds address space")
		}
		if err := c.sortKey.Resize(4 + len(payload)); err != nil {
			return nil, err
		}
		key := c.sortKey.Bytes()
		binary.BigEndian.PutUint32(key[:4], uint32(aggregate))
		copy(key[4:], payload)
		for {
			err = set.Add(key, nil)
			if err == nil {
				setKeys++
				break
			}
			if err == arenaskl.ErrRecordExists {
				break
			}
			if err != arenaskl.ErrArenaFull {
				return nil, err
			}
			if setKeys == 0 {
				return nil, moerr.NewInvalidInputNoCtxf(
					"distinct spill record requires more than %d bytes",
					len(buffer))
			}
			if err := flush(true); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(false); err != nil {
		return nil, err
	}

	var final *spillBucket
	for level := range slots {
		if slots[level] == nil {
			continue
		}
		if final == nil {
			final = slots[level]
			slots[level] = nil
			continue
		}
		merged, err := c.mergeSortRuns(proc, spillfs, final, slots[level])
		if err != nil {
			return nil, err
		}
		final = merged
		slots[level] = nil
	}
	if final == nil {
		return make([]uint64, aggregateCount), nil
	}
	defer func() {
		if final != nil {
			if err := final.free(); retErr == nil && err != nil {
				retErr = err
			}
		}
	}()
	if err := final.flushWriter(); err != nil {
		return nil, err
	}
	if _, err := final.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	finalReader, err := newGroupSpillReader(c.ctr, final.file, proc.Ctx)
	if err != nil {
		return nil, err
	}
	defer finalReader.Free()
	counts = make([]uint64, aggregateCount)
	for {
		key, eof, err := readDistinctSortKey(finalReader, c.mergeLeft)
		if err != nil {
			return nil, err
		}
		if eof {
			break
		}
		if len(key) < 4 {
			return nil, moerr.NewInvalidInputNoCtx(
				"truncated distinct sort key")
		}
		aggregate := int(binary.BigEndian.Uint32(key[:4]))
		if aggregate < 0 || aggregate >= len(counts) || counts[aggregate] == math.MaxUint64 {
			return nil, moerr.NewInvalidInputNoCtx(
				"invalid distinct sort aggregate count")
		}
		counts[aggregate]++
	}
	return counts, nil
}

func (ctr *container) groupBatchRow(group int) (*batch.Batch, int32, error) {
	if ctr == nil || group < 0 {
		return nil, 0, mpool.ErrAllocationAccountInvalid
	}
	chunk := group / aggBatchSize
	row := group % aggBatchSize
	if chunk >= len(ctr.groupByBatches) || ctr.groupByBatches[chunk] == nil ||
		row >= ctr.groupByBatches[chunk].RowCount() {
		return nil, 0, moerr.NewInternalErrorNoCtxf(
			"distinct spill group %d has no group row", group)
	}
	return ctr.groupByBatches[chunk], int32(row), nil
}

func (ctr *container) drainExactCountDistinct(
	proc *process.Process,
	opAnalyzer process.Analyzer,
) (_ bool, retErr error) {
	if ctr == nil || proc == nil || ctr.allocationAccount == nil ||
		ctr.spillAggregateAllocation == nil {
		return false, nil
	}
	prepared := make([]preparedDistinctDrain, 0, len(ctr.aggList))
	defer func() {
		for i := range prepared {
			if prepared[i].drain != nil {
				prepared[i].drain.Abort()
			}
		}
	}()
	var totalKeys uint64
	for aggregate, exec := range ctr.aggList {
		spill, ok := exec.(aggexec.ExactCountDistinctSpillState)
		if !ok || !spill.SupportsExactCountDistinctSpill() {
			continue
		}
		drain, err := spill.BeginArgumentDrain(ctr.spillAggregateAllocation)
		if err != nil {
			return false, err
		}
		if drain.KeyCount() == 0 {
			drain.Abort()
			continue
		}
		if totalKeys > math.MaxUint64-drain.KeyCount() {
			drain.Abort()
			return false, moerr.NewInternalErrorNoCtx(
				"distinct spill key count overflow")
		}
		totalKeys += drain.KeyCount()
		prepared = append(prepared, preparedDistinctDrain{
			aggregate: aggregate,
			state:     spill,
			drain:     drain,
		})
	}
	if totalKeys == 0 {
		return false, nil
	}

	groupCount := 0
	for _, groups := range ctr.groupByBatches {
		if groups == nil {
			return false, moerr.NewInternalErrorNoCtx(
				"distinct spill contains nil group batch")
		}
		groupCount += groups.RowCount()
	}
	ctr.spillHashCodes, retErr = resizeDiscardableGroupScratch(
		ctr,
		ctr.spillHashCodes,
		groupCount,
		GroupAllocationSiteSpillHashCodes,
	)
	if retErr != nil {
		return false, retErr
	}
	if ctr.mtyp != H0 {
		if ctr.hr.IsEmpty() || int(ctr.hr.Hash.GroupCount()) != groupCount {
			return false, moerr.NewInternalErrorNoCtx(
				"distinct spill group hash count mismatch")
		}
		ctr.spillHashCodes = ctr.hr.Hash.FillGroupHashes(ctr.spillHashCodes[:groupCount])
	}

	if ctr.distinctSpill == nil {
		ctr.distinctSpill, retErr = newDistinctSpillController(ctr)
		if retErr != nil {
			return false, retErr
		}
	}
	controller := ctr.distinctSpill
	if controller.stats == nil && opAnalyzer != nil {
		controller.stats = opAnalyzer.GetOpStats()
	}
	beforeBytes := controller.bytes
	beforeKeys := controller.keys
	wave, err := controller.newPrivateWave()
	if err != nil {
		return false, err
	}
	defer freeDistinctWave(&wave)
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return false, err
	}
	for i := range prepared {
		aggregate := prepared[i].aggregate
		err = prepared[i].drain.ForEach(func(group int, payload []byte) error {
			if err, canceled := vm.CancelCheck(proc); canceled {
				return err
			}
			if group < 0 || group >= len(ctr.spillHashCodes) {
				return moerr.NewInternalErrorNoCtx(
					"distinct spill group exceeds hash state")
			}
			groups, row, err := ctr.groupBatchRow(group)
			if err != nil {
				return err
			}
			hash := controller.hash(ctr.spillHashCodes[group], aggregate, payload)
			bucket := int(hash & (distinctSpillNumBuckets - 1))
			target := wave[bucket]
			if target.file == nil {
				if err := ctr.openSpillBucket(proc, spillfs, target); err != nil {
					return err
				}
			}
			written, err := controller.writeRecord(
				target.writer, hash, ctr.spillHashCodes[group],
				aggregate, groups, row, payload)
			if err != nil {
				return err
			}
			if written <= 0 {
				return moerr.NewInternalErrorNoCtx(
					"distinct spill wrote an empty record")
			}
			target.cnt++
			return nil
		})
		if err != nil {
			return false, err
		}
	}
	for bucket := range wave {
		if wave[bucket] != nil && wave[bucket].writer != nil {
			if err := wave[bucket].flushWriter(); err != nil {
				return false, err
			}
		}
	}
	for i := range prepared {
		if err := prepared[i].drain.Commit(); err != nil {
			return false, err
		}
		prepared[i].drain = nil
	}
	for i := range prepared {
		if err := prepared[i].state.RehomeDistinctArgumentState(
			ctr.aggregateAllocation); err != nil {
			return false, err
		}
	}
	if err := controller.mergeCommittedWave(proc, spillfs, &wave); err != nil {
		return false, err
	}
	if opAnalyzer != nil {
		stats := opAnalyzer.GetOpStats()
		stats.AddExtraStat("GroupDistinctSpillActivations", 1)
		spilledKeys := controller.keys - beforeKeys
		spilledBytes := controller.bytes - beforeBytes
		stats.AddExtraStat("GroupDistinctSpillKeys", int64(min(spilledKeys, math.MaxInt64)))
		stats.AddExtraStat("GroupDistinctSpillBytes", int64(min(spilledBytes, math.MaxInt64)))
	}
	return true, nil
}

func (ctr *container) exactCountDistinctStats() (
	keys uint64,
	retainedBytes uint64,
	err error,
) {
	if ctr == nil {
		return 0, 0, nil
	}
	for _, exec := range ctr.aggList {
		spill, ok := exec.(aggexec.ExactCountDistinctSpillState)
		if !ok || !spill.SupportsExactCountDistinctSpill() {
			continue
		}
		execKeys, execBytes, err := spill.DistinctArgumentStats()
		if err != nil {
			return 0, 0, err
		}
		if keys > math.MaxUint64-execKeys || retainedBytes > math.MaxUint64-execBytes {
			return 0, 0, moerr.NewInternalErrorNoCtx(
				"distinct spill statistics overflow")
		}
		keys += execKeys
		retainedBytes += execBytes
	}
	return keys, retainedBytes, nil
}

func (ctr *container) shouldDrainExactCountDistinct() (bool, error) {
	if ctr.distinctDrainKeysForUT > 0 {
		keys, _, err := ctr.exactCountDistinctStats()
		if err != nil || keys == 0 {
			return false, err
		}
		return keys >= ctr.distinctDrainKeysForUT, nil
	}
	if ctr.spillMem > 0 && ctr.spillMem < 10000 {
		if ctr.mtyp != H0 {
			// Historical sub-10K values are a group-count injection, not a byte
			// budget. Preserve generic group-spill tests and semantics; exact-key
			// injection has its own explicit test threshold above.
			return false, nil
		}
		keys, _, err := ctr.exactCountDistinctStats()
		if err != nil || keys == 0 {
			return false, err
		}
		return keys >= uint64(ctr.spillMem), nil
	}
	if ctr.spillMem <= 0 || ctr.memUsed() <= ctr.spillMem {
		return false, nil
	}
	for _, exec := range ctr.aggList {
		spill, ok := exec.(aggexec.ExactCountDistinctSpillState)
		if !ok || !spill.SupportsExactCountDistinctSpill() {
			continue
		}
		has, err := spill.HasDistinctArguments()
		if err != nil {
			return false, err
		}
		if has {
			return true, nil
		}
	}
	return false, nil
}

func (ctr *container) hasExactCountDistinctArguments() (bool, error) {
	if ctr == nil {
		return false, nil
	}
	for _, exec := range ctr.aggList {
		spill, ok := exec.(aggexec.ExactCountDistinctSpillState)
		if !ok || !spill.SupportsExactCountDistinctSpill() {
			continue
		}
		has, err := spill.HasDistinctArguments()
		if err != nil {
			return false, err
		}
		if has {
			return true, nil
		}
	}
	return false, nil
}

func (ctr *container) readH0DistinctLeaf(
	proc *process.Process,
	controller *distinctSpillController,
	partition *spillBucket,
	groups *batch.Batch,
) (counts []uint64, capacity bool, retErr error) {
	if err := partition.flushWriter(); err != nil {
		return nil, false, err
	}
	if _, err := partition.file.Seek(0, io.SeekStart); err != nil {
		return nil, false, err
	}
	leafAggs, err := ctr.makeSpillAggList(ctr.aggExprs)
	if err != nil {
		return nil, false, err
	}
	defer freeAggList(leafAggs)
	reader, err := newGroupSpillReader(ctr, partition.file, proc.Ctx)
	if err != nil {
		return nil, false, err
	}
	defer reader.Free()
	for {
		_, _, aggregate, payload, eof, err := controller.readRecord(reader, groups)
		if err != nil {
			return nil, false, err
		}
		if eof {
			break
		}
		if aggregate < 0 || aggregate >= len(leafAggs) ||
			aggregate >= len(ctr.aggList) {
			return nil, false, moerr.NewInvalidInputNoCtx(
				"distinct spill aggregate ordinal out of range")
		}
		leaf, ok := leafAggs[aggregate].(aggexec.ExactCountDistinctSpillState)
		if !ok {
			return nil, false, moerr.NewInvalidInputNoCtx(
				"distinct spill record targets unsupported aggregate")
		}
		if err := leaf.InsertDistinctArgument(0, payload); err != nil {
			if mpool.IsRetryableAllocationCapacity(err) {
				return nil, true, err
			}
			return nil, false, err
		}
	}
	counts = make([]uint64, len(leafAggs))
	for aggregate, leafExec := range leafAggs {
		leaf, ok := leafExec.(aggexec.ExactCountDistinctSpillState)
		if !ok {
			continue
		}
		vectors, err := leaf.Flush()
		if err != nil {
			return nil, false, err
		}
		if len(vectors) != 1 || vectors[0] == nil || vectors[0].Length() != 1 {
			for _, result := range vectors {
				if result != nil {
					result.Free(ctr.mp)
				}
			}
			return nil, false, moerr.NewInternalErrorNoCtx(
				"invalid distinct leaf result")
		}
		count := vector.MustFixedColNoTypeCheck[int64](vectors[0])[0]
		vectors[0].Free(ctr.mp)
		if count < 0 {
			return nil, false, moerr.NewInternalErrorNoCtx(
				"negative distinct leaf count")
		}
		counts[aggregate] = uint64(count)
	}
	return counts, false, nil
}

func (ctr *container) addH0DistinctCounts(counts []uint64) error {
	if len(counts) != len(ctr.aggList) {
		return moerr.NewInternalErrorNoCtx(
			"distinct leaf count shape mismatch")
	}
	for aggregate, count := range counts {
		if count == 0 {
			continue
		}
		target, ok := ctr.aggList[aggregate].(aggexec.ExactCountDistinctSpillState)
		if !ok {
			return moerr.NewInternalErrorNoCtx(
				"distinct leaf target changed aggregate family")
		}
		if err := target.AddDistinctCountContribution(
			0, count, ctr.aggregateAllocation); err != nil {
			return err
		}
		if ctr.distinctSpill != nil {
			if ctr.distinctSpill.uniqueKeys > math.MaxUint64-count {
				return moerr.NewInternalErrorNoCtx(
					"distinct unique key count overflow")
			}
			ctr.distinctSpill.uniqueKeys += count
		}
	}
	return nil
}

func (ctr *container) finalizeH0DistinctPartition(
	proc *process.Process,
	controller *distinctSpillController,
	partition *spillBucket,
	groups *batch.Batch,
) (retErr error) {
	if ctr == nil || proc == nil || controller == nil || partition == nil ||
		groups == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	owned := true
	defer func() {
		if owned {
			if err := partition.free(); retErr == nil && err != nil {
				retErr = err
			}
		}
	}()
	if err := partition.flushWriter(); err != nil {
		return err
	}
	stat, err := partition.file.Stat()
	if err != nil {
		return err
	}
	useSort := controller.forceExternalSortForUT ||
		stat.Size() > int64(controller.sortArenaCapacity()/2)
	var counts []uint64
	if !useSort {
		var capacity bool
		counts, capacity, err = ctr.readH0DistinctLeaf(
			proc, controller, partition, groups)
		if capacity {
			useSort = true
		}
	}
	if useSort && partition.lv < spillMaxPass &&
		!controller.forceExternalSortForUT {
		children, progress, repartitionErr := controller.repartition(
			proc, partition, groups)
		if repartitionErr != nil {
			return repartitionErr
		}
		if progress {
			if err := partition.free(); err != nil {
				freeDistinctWave(&children)
				return err
			}
			owned = false
			for bucket := range children {
				child := children[bucket]
				if child == nil || child.cnt == 0 {
					continue
				}
				if err := ctr.finalizeH0DistinctPartition(
					proc, controller, child, groups); err != nil {
					children[bucket] = nil
					freeDistinctWave(&children)
					return err
				}
				children[bucket] = nil
			}
			return nil
		}
	}
	if useSort {
		counts, err = controller.externalSortH0Partition(proc, partition)
	}
	if err != nil {
		return err
	}
	return ctr.addH0DistinctCounts(counts)
}

func (ctr *container) finalizeH0ExactCountDistinct(
	proc *process.Process,
) error {
	if ctr == nil || proc == nil || ctr.mtyp != H0 || ctr.distinctSpill == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	controller := ctr.distinctSpill
	groups, err := ctr.createNewGroupByBatchWithAllocation(
		nil, 1, ctr.spillGroupByAllocation)
	if err != nil {
		return err
	}
	defer groups.Clean(ctr.mp)

	for bucket := range controller.root {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return err
		}
		partition := controller.root[bucket]
		if partition == nil || partition.cnt == 0 {
			continue
		}
		if err := ctr.finalizeH0DistinctPartition(
			proc, controller, partition, groups); err != nil {
			return err
		}
		controller.root[bucket] = nil
	}
	controller.recordCompletion()
	controller.close()
	ctr.distinctSpill = nil
	ctr.distinctFinalized = true
	return nil
}

func (ctr *container) finalizeGroupedDistinctLeaf(
	proc *process.Process,
	controller *distinctSpillController,
	partition *spillBucket,
	groups *batch.Batch,
	toResultSpool bool,
) error {
	if ctr == nil || proc == nil || controller == nil || partition == nil ||
		groups == nil || ctr.mtyp == H0 || !toResultSpool && ctr.hr.IsEmpty() {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := partition.flushWriter(); err != nil {
		return err
	}
	if _, err := partition.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader, err := newGroupSpillReader(ctr, partition.file, proc.Ctx)
	if err != nil {
		return err
	}
	defer reader.Free()

	aggregateVector, err := vector.NewOffHeapVecWithTypeAndAllocation(
		types.T_int32.ToType(), ctr.spillGroupByAllocation)
	if err != nil {
		return err
	}
	defer aggregateVector.Free(ctr.mp)
	payloadVector, err := vector.NewOffHeapVecWithTypeAndAllocation(
		types.T_varbinary.ToType(), ctr.spillGroupByAllocation)
	if err != nil {
		return err
	}
	defer payloadVector.Free(ctr.mp)
	composite := make([]*vector.Vector, len(ctr.groupByTypes)+2)

	uniqueGroups, err := ctr.createNewGroupByBatchWithAllocation(
		nil, min(int(partition.cnt), hashmap.UnitLimit), ctr.spillGroupByAllocation)
	if err != nil {
		return err
	}
	defer uniqueGroups.Clean(ctr.mp)
	uniqueAggregates, err := vector.NewOffHeapVecWithTypeAndAllocation(
		types.T_int32.ToType(), ctr.spillGroupByAllocation)
	if err != nil {
		return err
	}
	defer uniqueAggregates.Free(ctr.mp)
	uniqueHashes, err := vector.NewOffHeapVecWithTypeAndAllocation(
		types.T_uint64.ToType(), ctr.spillGroupByAllocation)
	if err != nil {
		return err
	}
	defer uniqueHashes.Free(ctr.mp)

	var dedup ResHashRelated
	defer dedup.Free0()
	if err := dedup.BuildHashTable(
		proc.Ctx,
		ctr.mp,
		false,
		true,
		true,
		true,
		0,
		ctr.hashAllocation,
		ctr.hashIterator,
	); err != nil {
		return err
	}
	for {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return err
		}
		_, groupHash, aggregate, payload, eof, err := controller.readRecord(
			reader, groups)
		if err != nil {
			return err
		}
		if eof {
			break
		}
		if aggregate < 0 || aggregate >= len(ctr.aggExprs) {
			return moerr.NewInvalidInputNoCtx(
				"distinct spill aggregate ordinal out of range")
		}
		if !toResultSpool {
			if aggregate >= len(ctr.aggList) {
				return moerr.NewInvalidInputNoCtx(
					"distinct spill target aggregate is missing")
			}
			if target, ok := ctr.aggList[aggregate].(aggexec.ExactCountDistinctSpillState); !ok || !target.SupportsExactCountDistinctSpill() {
				return moerr.NewInvalidInputNoCtx(
					"distinct spill record targets unsupported aggregate")
			}
		} else if ctr.aggExprs[aggregate].GetAggID() != aggexec.AggIdOfCountColumn ||
			!ctr.aggExprs[aggregate].IsDistinct() {
			return moerr.NewInvalidInputNoCtx(
				"distinct spill record targets unsupported aggregate")
		}
		aggregateVector.CleanOnlyData()
		payloadVector.CleanOnlyData()
		if err := vector.AppendFixed(
			aggregateVector, int32(aggregate), false, ctr.mp); err != nil {
			return err
		}
		if err := vector.AppendBytes(payloadVector, payload, false, ctr.mp); err != nil {
			return err
		}
		copy(composite, groups.Vecs)
		composite[len(groups.Vecs)] = aggregateVector
		composite[len(groups.Vecs)+1] = payloadVector
		inserted, err := dedup.TxnItr.DetectDup(composite, 0)
		if err != nil {
			return err
		}
		if !inserted {
			continue
		}
		for column := range uniqueGroups.Vecs {
			if err := uniqueGroups.Vecs[column].UnionOne(
				groups.Vecs[column], 0, ctr.mp); err != nil {
				return err
			}
		}
		uniqueGroups.AddRowCount(1)
		if err := vector.AppendFixed(
			uniqueAggregates, int32(aggregate), false, ctr.mp); err != nil {
			return err
		}
		if err := vector.AppendFixed(
			uniqueHashes, groupHash, false, ctr.mp); err != nil {
			return err
		}
	}

	aggregates := vector.MustFixedColNoTypeCheck[int32](uniqueAggregates)
	if toResultSpool {
		err := ctr.writeCompactedDistinctContributions(
			proc, controller, uniqueGroups, uniqueAggregates, uniqueHashes)
		if err != nil {
			return err
		}
		if controller.uniqueKeys > math.MaxUint64-uint64(uniqueGroups.RowCount()) {
			return moerr.NewInternalErrorNoCtx(
				"distinct unique key count overflow")
		}
		controller.uniqueKeys += uint64(uniqueGroups.RowCount())
		return nil
	}
	for row, aggregate32 := range aggregates {
		values, zValues, err := ctr.hr.TxnItr.Find(
			row, 1, uniqueGroups.Vecs)
		if err != nil {
			return err
		}
		if len(values) != 1 || len(zValues) != 1 || values[0] == 0 || zValues[0] == 0 {
			return moerr.NewInternalErrorNoCtx(
				"distinct spill group is not resident during finalization")
		}
		aggregate := int(aggregate32)
		target, ok := ctr.aggList[aggregate].(aggexec.ExactCountDistinctSpillState)
		if !ok {
			return moerr.NewInternalErrorNoCtx(
				"distinct spill target changed aggregate family")
		}
		if err := target.AddDistinctCountContribution(
			int(values[0]-1), 1, ctr.aggregateAllocation); err != nil {
			return err
		}
	}
	if controller.uniqueKeys > math.MaxUint64-uint64(uniqueGroups.RowCount()) {
		return moerr.NewInternalErrorNoCtx(
			"distinct unique key count overflow")
	}
	controller.uniqueKeys += uint64(uniqueGroups.RowCount())
	return nil
}

func (ctr *container) finalizeGroupedDistinctPartition(
	proc *process.Process,
	controller *distinctSpillController,
	partition *spillBucket,
	groups *batch.Batch,
	toResultSpool bool,
) (retErr error) {
	if ctr == nil || proc == nil || controller == nil || partition == nil ||
		groups == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	owned := true
	defer func() {
		if owned {
			if err := partition.free(); retErr == nil && err != nil {
				retErr = err
			}
		}
	}()
	if err := partition.flushWriter(); err != nil {
		return err
	}
	stat, err := partition.file.Stat()
	if err != nil {
		return err
	}
	oversized := stat.Size() > int64(controller.sortArenaCapacity()/2)
	if oversized && partition.lv < spillMaxPass {
		children, progress, err := controller.repartition(proc, partition, groups)
		if err != nil {
			return err
		}
		if progress {
			if err := partition.free(); err != nil {
				freeDistinctWave(&children)
				return err
			}
			owned = false
			for bucket := range children {
				child := children[bucket]
				if child == nil || child.cnt == 0 {
					continue
				}
				if err := ctr.finalizeGroupedDistinctPartition(
					proc, controller, child, groups, toResultSpool); err != nil {
					children[bucket] = nil
					freeDistinctWave(&children)
					return err
				}
				children[bucket] = nil
			}
			return nil
		}
		handled, fallbackErr := ctr.finalizeSingleGroupDistinctPartition(
			proc, controller, partition, groups, toResultSpool)
		if fallbackErr != nil {
			return fallbackErr
		}
		if handled {
			return nil
		}
	}
	err = ctr.finalizeGroupedDistinctLeaf(
		proc, controller, partition, groups, toResultSpool)
	if err == nil {
		return nil
	}
	if !mpool.IsRetryableAllocationCapacity(err) {
		return err
	}
	if partition.lv < spillMaxPass {
		children, progress, repartitionErr := controller.repartition(
			proc, partition, groups)
		if repartitionErr != nil {
			return repartitionErr
		}
		if progress {
			if freeErr := partition.free(); freeErr != nil {
				freeDistinctWave(&children)
				return freeErr
			}
			owned = false
			for bucket := range children {
				child := children[bucket]
				if child == nil || child.cnt == 0 {
					continue
				}
				if err := ctr.finalizeGroupedDistinctPartition(
					proc, controller, child, groups, toResultSpool); err != nil {
					children[bucket] = nil
					freeDistinctWave(&children)
					return err
				}
				children[bucket] = nil
			}
			return nil
		}
	}
	handled, fallbackErr := ctr.finalizeSingleGroupDistinctPartition(
		proc, controller, partition, groups, toResultSpool)
	if fallbackErr != nil {
		return fallbackErr
	}
	if handled {
		return nil
	}
	return err
}

func (ctr *container) writeCompactedDistinctContributions(
	proc *process.Process,
	controller *distinctSpillController,
	uniqueGroups *batch.Batch,
	uniqueAggregates *vector.Vector,
	uniqueHashes *vector.Vector,
) error {
	if ctr == nil || proc == nil || controller == nil || uniqueGroups == nil ||
		uniqueAggregates == nil || uniqueHashes == nil ||
		uniqueGroups.RowCount() != uniqueAggregates.Length() ||
		uniqueGroups.RowCount() != uniqueHashes.Length() {
		return mpool.ErrAllocationAccountInvalid
	}
	rows := uniqueGroups.RowCount()
	if rows == 0 {
		return nil
	}
	var groups ResHashRelated
	defer groups.Free0()
	if err := groups.BuildHashTable(
		proc.Ctx,
		ctr.mp,
		false,
		true,
		true,
		true,
		0,
		ctr.hashAllocation,
		ctr.hashIterator,
	); err != nil {
		return err
	}
	composite := make([]*vector.Vector, len(uniqueGroups.Vecs)+1)
	copy(composite, uniqueGroups.Vecs)
	composite[len(uniqueGroups.Vecs)] = uniqueAggregates

	compactGroups, err := ctr.createNewGroupByBatchWithAllocation(
		nil, min(rows, hashmap.UnitLimit), ctr.spillGroupByAllocation)
	if err != nil {
		return err
	}
	defer compactGroups.Clean(ctr.mp)
	compactAggregates, err := vector.NewOffHeapVecWithTypeAndAllocation(
		types.T_int32.ToType(), ctr.spillGroupByAllocation)
	if err != nil {
		return err
	}
	defer compactAggregates.Free(ctr.mp)
	compactHashes, err := vector.NewOffHeapVecWithTypeAndAllocation(
		types.T_uint64.ToType(), ctr.spillGroupByAllocation)
	if err != nil {
		return err
	}
	defer compactHashes.Free(ctr.mp)
	counts, err := resizeGroupScratch[uint64](
		ctr, nil, rows, GroupAllocationSiteSpillRows)
	if err != nil {
		return err
	}
	defer freeGroupScratch(ctr, counts)
	hashes := vector.MustFixedColNoTypeCheck[uint64](uniqueHashes)
	aggregates := vector.MustFixedColNoTypeCheck[int32](uniqueAggregates)
	for row := 0; row < rows; row++ {
		before := groups.Hash.GroupCount()
		values, zValues, err := groups.TxnItr.Insert(row, 1, composite)
		if err != nil {
			return err
		}
		if len(values) != 1 || len(zValues) != 1 || values[0] == 0 || zValues[0] == 0 {
			return moerr.NewInternalErrorNoCtx(
				"distinct contribution compaction failed")
		}
		index := int(values[0] - 1)
		if index < 0 || index >= len(counts) || counts[index] == math.MaxUint64 {
			return moerr.NewInternalErrorNoCtx(
				"distinct contribution count overflow")
		}
		counts[index]++
		if groups.Hash.GroupCount() == before {
			continue
		}
		if groups.Hash.GroupCount() != before+1 || values[0] != before+1 {
			return moerr.NewInternalErrorNoCtx(
				"distinct contribution group publication mismatch")
		}
		for column := range compactGroups.Vecs {
			if err := compactGroups.Vecs[column].UnionOne(
				uniqueGroups.Vecs[column], int64(row), ctr.mp); err != nil {
				return err
			}
		}
		compactGroups.AddRowCount(1)
		if err := vector.AppendFixed(
			compactAggregates, aggregates[row], false, ctr.mp); err != nil {
			return err
		}
		if err := vector.AppendFixed(
			compactHashes, hashes[row], false, ctr.mp); err != nil {
			return err
		}
	}
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return err
	}
	compactAggValues := vector.MustFixedColNoTypeCheck[int32](compactAggregates)
	compactHashValues := vector.MustFixedColNoTypeCheck[uint64](compactHashes)
	for row := 0; row < compactGroups.RowCount(); row++ {
		if err := controller.writeContribution(
			proc,
			spillfs,
			compactHashValues[row],
			int(compactAggValues[row]),
			compactGroups,
			int32(row),
			counts[row],
		); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) finalizeSingleGroupDistinctPartition(
	proc *process.Process,
	controller *distinctSpillController,
	partition *spillBucket,
	decodeGroups *batch.Batch,
	toResultSpool bool,
) (handled bool, retErr error) {
	if ctr == nil || proc == nil || controller == nil || partition == nil ||
		decodeGroups == nil || ctr.mtyp == H0 {
		return false, mpool.ErrAllocationAccountInvalid
	}
	if err := partition.flushWriter(); err != nil {
		return false, err
	}
	if _, err := partition.file.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	reader, err := newGroupSpillReader(ctr, partition.file, proc.Ctx)
	if err != nil {
		return false, err
	}
	defer reader.Free()
	var groupSet ResHashRelated
	defer groupSet.Free0()
	if err := groupSet.BuildHashTable(
		proc.Ctx, ctr.mp, false, true, true, true, 0,
		ctr.hashAllocation, ctr.hashIterator); err != nil {
		return false, err
	}
	representative, err := ctr.createNewGroupByBatchWithAllocation(
		nil, 1, ctr.spillGroupByAllocation)
	if err != nil {
		return false, err
	}
	defer representative.Clean(ctr.mp)
	var groupHash uint64
	for {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return false, err
		}
		_, recordGroupHash, _, _, eof, err := controller.readRecord(
			reader, decodeGroups)
		if err != nil {
			return false, err
		}
		if eof {
			break
		}
		inserted, err := groupSet.TxnItr.DetectDup(decodeGroups.Vecs, 0)
		if err != nil {
			return false, err
		}
		if !inserted {
			continue
		}
		if groupSet.Hash.GroupCount() > 1 {
			return false, nil
		}
		groupHash = recordGroupHash
		for column := range representative.Vecs {
			if err := representative.Vecs[column].UnionOne(
				decodeGroups.Vecs[column], 0, ctr.mp); err != nil {
				return false, err
			}
		}
		representative.SetRowCount(1)
	}
	if representative.RowCount() != 1 {
		return false, moerr.NewInternalErrorNoCtx(
			"distinct single-group fallback has no group")
	}
	counts, err := controller.externalSortH0Partition(proc, partition)
	if err != nil {
		return false, err
	}
	if len(counts) != len(ctr.aggExprs) {
		return false, moerr.NewInternalErrorNoCtx(
			"distinct single-group fallback count shape mismatch")
	}
	if toResultSpool {
		spillfs, err := proc.GetSpillFileService()
		if err != nil {
			return false, err
		}
		for aggregate, count := range counts {
			if count == 0 {
				continue
			}
			if err := controller.writeContribution(
				proc, spillfs, groupHash, aggregate,
				representative, 0, count); err != nil {
				return false, err
			}
		}
	} else {
		values, zValues, err := ctr.hr.TxnItr.Find(
			0, 1, representative.Vecs)
		if err != nil {
			return false, err
		}
		if len(values) != 1 || len(zValues) != 1 || values[0] == 0 || zValues[0] == 0 {
			return false, moerr.NewInternalErrorNoCtx(
				"distinct single-group fallback target is missing")
		}
		for aggregate, count := range counts {
			if count == 0 {
				continue
			}
			target, ok := ctr.aggList[aggregate].(aggexec.ExactCountDistinctSpillState)
			if !ok || !target.SupportsExactCountDistinctSpill() {
				return false, moerr.NewInternalErrorNoCtx(
					"distinct single-group fallback target changed family")
			}
			if err := target.AddDistinctCountContribution(
				int(values[0]-1), count, ctr.aggregateAllocation); err != nil {
				return false, err
			}
		}
	}
	for _, count := range counts {
		if controller.uniqueKeys > math.MaxUint64-count {
			return false, moerr.NewInternalErrorNoCtx(
				"distinct unique key count overflow")
		}
		controller.uniqueKeys += count
	}
	return true, nil
}

func contributionPathIndex(
	path [spillMaxPass]uint8,
	pathLen int,
) (int, error) {
	if pathLen <= 0 || pathLen > spillMaxPass {
		return 0, mpool.ErrAllocationAccountInvalid
	}
	index := 0
	for level := 0; level < pathLen; level++ {
		if int(path[level]) >= spillNumBuckets {
			return 0, moerr.NewInvalidInputNoCtx(
				"distinct contribution path bucket out of range")
		}
		index = index*spillNumBuckets + int(path[level])
	}
	return index, nil
}

func (c *distinctSpillController) contributionPartition(
	path [spillMaxPass]uint8,
	pathLen int,
) (*spillBucket, error) {
	if c == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	index, err := contributionPathIndex(path, pathLen)
	if err != nil {
		return nil, err
	}
	if pathLen > len(c.result) || index >= len(c.result[pathLen-1]) {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return c.result[pathLen-1][index], nil
}

func (c *distinctSpillController) setContributionPartition(
	path [spillMaxPass]uint8,
	pathLen int,
	partition *spillBucket,
) error {
	if c == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	index, err := contributionPathIndex(path, pathLen)
	if err != nil {
		return err
	}
	if pathLen > len(c.result) || index >= len(c.result[pathLen-1]) {
		return mpool.ErrAllocationAccountInvalid
	}
	c.result[pathLen-1][index] = partition
	return nil
}

func (c *distinctSpillController) contributionPartitionSplit(
	path [spillMaxPass]uint8,
	pathLen int,
) (bool, error) {
	if c == nil || pathLen <= 0 || pathLen >= spillMaxPass {
		return false, mpool.ErrAllocationAccountInvalid
	}
	index, err := contributionPathIndex(path, pathLen)
	if err != nil {
		return false, err
	}
	if pathLen > len(c.resultSplit) || index >= len(c.resultSplit[pathLen-1]) {
		return false, mpool.ErrAllocationAccountInvalid
	}
	return c.resultSplit[pathLen-1][index], nil
}

func (c *distinctSpillController) markContributionPartitionSplit(
	path [spillMaxPass]uint8,
	pathLen int,
) error {
	if c == nil || pathLen <= 0 || pathLen >= spillMaxPass {
		return mpool.ErrAllocationAccountInvalid
	}
	index, err := contributionPathIndex(path, pathLen)
	if err != nil {
		return err
	}
	if pathLen > len(c.resultSplit) || index >= len(c.resultSplit[pathLen-1]) {
		return mpool.ErrAllocationAccountInvalid
	}
	c.resultSplit[pathLen-1][index] = true
	return nil
}

func (c *distinctSpillController) repartitionContributions(
	proc *process.Process,
	path [spillMaxPass]uint8,
	pathLen int,
) (retErr error) {
	if c == nil || c.closed || proc == nil || pathLen <= 0 ||
		pathLen >= spillMaxPass {
		return mpool.ErrAllocationAccountInvalid
	}
	parent, err := c.contributionPartition(path, pathLen)
	if err != nil {
		return err
	}
	if parent == nil || parent.cnt == 0 {
		if parent != nil {
			if err := parent.free(); err != nil {
				return err
			}
			if err := c.setContributionPartition(path, pathLen, nil); err != nil {
				return err
			}
		}
		return c.markContributionPartitionSplit(path, pathLen)
	}
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return err
	}
	if err := parent.flushWriter(); err != nil {
		return err
	}
	if _, err := parent.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader, err := newGroupSpillReader(c.ctr, parent.file, proc.Ctx)
	if err != nil {
		return err
	}
	defer reader.Free()
	groups, err := c.ctr.createNewGroupByBatchWithAllocation(
		nil, 1, c.ctr.spillGroupByAllocation)
	if err != nil {
		return err
	}
	defer groups.Clean(c.ctr.mp)

	var children [spillNumBuckets]*spillBucket
	defer func() {
		if retErr != nil {
			freeDistinctWave(&children)
		}
	}()
	for bucket := range children {
		childPath := path
		childPath[pathLen] = uint8(bucket)
		children[bucket] = &spillBucket{
			lv:      pathLen + 1,
			name:    fmt.Sprintf("%s_%d", parent.name, bucket),
			path:    childPath,
			pathLen: pathLen + 1,
		}
	}
	for {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return err
		}
		groupHash, _, _, eof, err := c.readContribution(reader, groups)
		if err != nil {
			return err
		}
		if eof {
			break
		}
		bucket := distinctGroupBucket(groupHash, pathLen+1)
		if err := c.writeContributionEnvelope(
			proc, spillfs, children[bucket]); err != nil {
			return err
		}
	}
	for bucket := range children {
		if children[bucket].cnt == 0 {
			if err := children[bucket].free(); err != nil {
				return err
			}
			children[bucket] = nil
			continue
		}
		if err := children[bucket].flushWriter(); err != nil {
			return err
		}
	}
	if err := c.setContributionPartition(path, pathLen, nil); err != nil {
		return err
	}
	if err := parent.free(); err != nil {
		return err
	}
	for bucket := range children {
		if children[bucket] == nil {
			continue
		}
		child := children[bucket]
		if err := c.setContributionPartition(
			child.path, child.pathLen, child); err != nil {
			return err
		}
		children[bucket] = nil
	}
	return c.markContributionPartitionSplit(path, pathLen)
}

func (c *distinctSpillController) ensureContributionPartition(
	proc *process.Process,
	path [spillMaxPass]uint8,
	pathLen int,
) (*spillBucket, error) {
	if c == nil || proc == nil || pathLen <= 0 || pathLen > spillMaxPass {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if _, err := contributionPathIndex(path, pathLen); err != nil {
		return nil, err
	}
	for prefixLen := 1; prefixLen < pathLen; prefixLen++ {
		split, err := c.contributionPartitionSplit(path, prefixLen)
		if err != nil {
			return nil, err
		}
		if split {
			continue
		}
		if err := c.repartitionContributions(proc, path, prefixLen); err != nil {
			return nil, err
		}
	}
	return c.contributionPartition(path, pathLen)
}

func (c *distinctSpillController) contributionPathApplied(
	path [spillMaxPass]uint8,
	pathLen int,
) (bool, error) {
	if c == nil || pathLen <= 0 || pathLen > spillMaxPass {
		return false, mpool.ErrAllocationAccountInvalid
	}
	switch pathLen {
	case 1:
		return c.appliedLevel1[int(path[0])], nil
	case 2:
		index := int(path[0])*spillNumBuckets + int(path[1])
		return c.appliedLevel2[index], nil
	case 3:
		index := (int(path[0])*spillNumBuckets+int(path[1]))*
			spillNumBuckets + int(path[2])
		return c.appliedLevel3[index], nil
	default:
		return false, mpool.ErrAllocationAccountInvalid
	}
}

func (c *distinctSpillController) markContributionPathApplied(
	path [spillMaxPass]uint8,
	pathLen int,
) error {
	if c == nil || pathLen <= 0 || pathLen > spillMaxPass {
		return mpool.ErrAllocationAccountInvalid
	}
	switch pathLen {
	case 1:
		c.appliedLevel1[int(path[0])] = true
	case 2:
		index := int(path[0])*spillNumBuckets + int(path[1])
		c.appliedLevel2[index] = true
	case 3:
		index := (int(path[0])*spillNumBuckets+int(path[1]))*
			spillNumBuckets + int(path[2])
		c.appliedLevel3[index] = true
	default:
		return mpool.ErrAllocationAccountInvalid
	}
	return nil
}

func (ctr *container) applyDistinctContributions(
	proc *process.Process,
	bucket *spillBucket,
) error {
	if ctr == nil || proc == nil || bucket == nil || ctr.distinctSpill == nil {
		return nil
	}
	controller := ctr.distinctSpill
	if bucket.pathLen <= 0 || bucket.pathLen > spillMaxPass {
		return moerr.NewInternalErrorNoCtx(
			"invalid group spill path for distinct contributions")
	}
	applied, err := controller.contributionPathApplied(
		bucket.path, bucket.pathLen)
	if err != nil || applied {
		return err
	}
	partition, err := controller.ensureContributionPartition(
		proc, bucket.path, bucket.pathLen)
	if err != nil {
		return err
	}
	if partition == nil || partition.cnt == 0 {
		return controller.markContributionPathApplied(bucket.path, bucket.pathLen)
	}
	if err := partition.flushWriter(); err != nil {
		return err
	}
	if _, err := partition.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader, err := newGroupSpillReader(ctr, partition.file, proc.Ctx)
	if err != nil {
		return err
	}
	defer reader.Free()
	groups, err := ctr.createNewGroupByBatchWithAllocation(
		nil, 1, ctr.spillGroupByAllocation)
	if err != nil {
		return err
	}
	defer groups.Clean(ctr.mp)
	for {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return err
		}
		_, aggregate, count, eof, err := controller.readContribution(
			reader, groups)
		if err != nil {
			return err
		}
		if eof {
			break
		}
		if aggregate < 0 || aggregate >= len(ctr.aggList) {
			return moerr.NewInvalidInputNoCtx(
				"distinct contribution aggregate out of range")
		}
		values, zValues, err := ctr.hr.TxnItr.Find(0, 1, groups.Vecs)
		if err != nil {
			return err
		}
		if len(values) != 1 || len(zValues) != 1 || values[0] == 0 || zValues[0] == 0 {
			return moerr.NewInternalErrorNoCtx(
				"distinct contribution group is missing from spill leaf")
		}
		target, ok := ctr.aggList[aggregate].(aggexec.ExactCountDistinctSpillState)
		if !ok || !target.SupportsExactCountDistinctSpill() {
			return moerr.NewInternalErrorNoCtx(
				"distinct contribution target changed aggregate family")
		}
		if err := target.AddDistinctCountContribution(
			int(values[0]-1), count, ctr.aggregateAllocation); err != nil {
			return err
		}
	}
	if err := controller.markContributionPathApplied(
		bucket.path, bucket.pathLen); err != nil {
		return err
	}
	if err := controller.setContributionPartition(
		bucket.path, bucket.pathLen, nil); err != nil {
		return err
	}
	return partition.free()
}

// finalizeGroupedExactCountDistinctViaSpill externalizes the ordinary group
// state before materializing compact DISTINCT count contributions. Once exact
// key spill has activated, retaining the complete group hash/table while also
// allocating a contribution vector can exceed a hard statement account even
// though the exact-key drain itself made progress. Reusing generic Group spill
// makes one bounded group-hash leaf the contribution accumulator and preserves
// the existing two-phase equality/merge contract.
func (ctr *container) finalizeGroupedExactCountDistinctViaSpill(
	proc *process.Process,
	opAnalyzer process.Analyzer,
) error {
	if ctr == nil || proc == nil || opAnalyzer == nil || ctr.mtyp == H0 ||
		ctr.distinctSpill == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	bytes, rows, err := ctr.spillDataToDisk(proc, opAnalyzer, nil)
	if err != nil {
		return err
	}
	if rows <= 0 {
		return moerr.NewInternalErrorNoCtx(
			"grouped distinct finalization did not spill resident groups")
	}
	opAnalyzer.Spill(bytes)
	opAnalyzer.SpillRows(rows)
	if err := ctr.prepareGroupedDistinctContributions(proc); err != nil {
		return err
	}
	_, err = ctr.loadSpilledData(proc, opAnalyzer, ctr.aggExprs)
	return err
}

func (ctr *container) prepareGroupedDistinctContributions(
	proc *process.Process,
) error {
	if ctr == nil || proc == nil || ctr.mtyp == H0 || ctr.distinctSpill == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	controller := ctr.distinctSpill
	groups, err := ctr.createNewGroupByBatchWithAllocation(
		nil, 1, ctr.spillGroupByAllocation)
	if err != nil {
		return err
	}
	defer groups.Clean(ctr.mp)
	for bucket := range controller.root {
		partition := controller.root[bucket]
		if partition == nil || partition.cnt == 0 {
			continue
		}
		if err := ctr.finalizeGroupedDistinctPartition(
			proc, controller, partition, groups, true); err != nil {
			return err
		}
		controller.root[bucket] = nil
	}
	// Contribution writers carry optional per-file coalescing buffers. Release
	// all of them before generic Group reload borrows the hard-account recovery
	// floor; leaving up to 32 buffers live can otherwise prevent even the first
	// bounded group leaf from materializing.
	if err := controller.flushContributionWriters(); err != nil {
		return err
	}
	ctr.distinctContributionsPrepared = true
	return nil
}

func (ctr *container) finishDistinctContributions() {
	if ctr == nil || !ctr.distinctContributionsPrepared {
		return
	}
	if ctr.distinctSpill != nil {
		ctr.distinctSpill.recordCompletion()
		ctr.distinctSpill.close()
		ctr.distinctSpill = nil
	}
	ctr.distinctContributionsPrepared = false
	ctr.distinctFinalized = true
}

func (ctr *container) resetForDistinctPartialLeaf() {
	if ctr == nil {
		return
	}
	ctr.hr.Free0()
	ctr.freeGroupByBatches()
	ctr.freeAggList()
	ctr.freeSpillAggList()
	ctr.currBatchIdx = 0
}

func (ctr *container) initializeDistinctPartialLeafState() error {
	if ctr == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	ctr.resetForDistinctPartialLeaf()
	var err error
	ctr.aggList, err = ctr.makeAggList(ctr.aggExprs)
	if err != nil {
		return err
	}
	if ctr.mtyp != H0 {
		return nil
	}
	groupByBatch, err := ctr.createNewGroupByBatch(nil, 1)
	if err != nil {
		return err
	}
	groupByBatch.SetRowCount(1)
	ctr.groupByBatches = append(ctr.groupByBatches, groupByBatch)
	return nil
}

func (ctr *container) insertDistinctPartialRecord(
	proc *process.Process,
	decodeGroups *batch.Batch,
	aggregate int,
	payload []byte,
) error {
	if ctr == nil || proc == nil || decodeGroups == nil || aggregate < 0 ||
		aggregate >= len(ctr.aggList) {
		return moerr.NewInvalidInputNoCtx(
			"distinct partial aggregate ordinal out of range")
	}
	target, ok := ctr.aggList[aggregate].(aggexec.ExactCountDistinctSpillState)
	if !ok {
		return moerr.NewInvalidInputNoCtx(
			"distinct partial targets unsupported aggregate")
	}
	group := 0
	if ctr.mtyp != H0 {
		if ctr.hr.IsEmpty() {
			if err := ctr.buildHashTable(proc.Ctx, 0); err != nil {
				return err
			}
		}
		hashKeyVecs := ctr.hashKeyVectors(decodeGroups.Vecs)
		if err := ctr.hr.TxnItr.PreviewInsert(
			0, 1, hashKeyVecs, ctr.hr.Hash.GroupCount(),
			&ctr.hr.insertPlan); err != nil {
			return err
		}
		preview := groupInsertPreview{
			values:    ctr.hr.insertPlan.Values(),
			inserted:  ctr.hr.insertPlan.Inserted(),
			newGroups: int(ctr.hr.insertPlan.NewGroups()),
		}
		if !ctr.recoveryCapacityCovers(preview.newGroups) {
			if err := ctr.ensureRecoveryCapacity(preview.newGroups, nil); err != nil {
				return err
			}
		}
		if err := ctr.hr.Hash.PreAlloc(
			ctr.hr.insertPlan.NewGroups()); err != nil {
			return err
		}
		if err := ctr.preflightBuildChunk(
			decodeGroups.Vecs, 0, 1,
			preview.inserted, preview.newGroups); err != nil {
			ctr.cancelGroupByPreflights()
			return err
		}
		values, more, err := ctr.commitGroupByChunk(
			decodeGroups.Vecs, 0, 1, preview)
		if err != nil {
			if isGroupPrePublicationError(err) {
				ctr.cancelGroupByPreflights()
			}
			return err
		}
		if len(values) != 1 || values[0] == 0 {
			return moerr.NewInternalErrorNoCtx(
				"distinct partial group insertion failed")
		}
		if more > 0 {
			if more != preview.newGroups {
				return moerr.NewInternalErrorNoCtx(
					"distinct partial group publication mismatch")
			}
			for _, aggregateExec := range ctr.aggList {
				if err := aggregateExec.GroupGrow(more); err != nil {
					return err
				}
			}
		}
		group = int(values[0] - 1)
	}
	return target.InsertDistinctArgument(group, payload)
}

func (ctr *container) loadNextDistinctPartialLeaf(
	proc *process.Process,
) (loaded bool, retErr error) {
	if ctr == nil || proc == nil || ctr.distinctSpill == nil {
		return false, nil
	}
	controller := ctr.distinctSpill
	decodeGroups, err := ctr.createNewGroupByBatchWithAllocation(
		nil, 1, ctr.spillGroupByAllocation)
	if err != nil {
		return false, err
	}
	defer decodeGroups.Clean(ctr.mp)
	var partition *spillBucket
	partitionOffset := int64(0)
	owned := false
	defer func() {
		if retErr != nil && owned && partition != nil {
			_ = partition.free()
		}
	}()
	if controller.partialActive != nil {
		partition = controller.partialActive
		partitionOffset = controller.partialActiveOffset
		controller.partialActive = nil
		controller.partialActiveOffset = 0
		owned = true
	}
	for partition == nil {
		partition = controller.takePartialPartition()
		if partition == nil {
			controller.close()
			ctr.distinctSpill = nil
			ctr.distinctFinalized = true
			return false, nil
		}
		owned = true
		if err := partition.flushWriter(); err != nil {
			return false, err
		}
		stat, err := partition.file.Stat()
		if err != nil {
			return false, err
		}
		if stat.Size() <= int64(controller.sortArenaCapacity()/2) ||
			partition.lv >= spillMaxPass {
			break
		}
		children, progress, err := controller.repartition(
			proc, partition, decodeGroups)
		if err != nil {
			return false, err
		}
		if !progress {
			break
		}
		if err := partition.free(); err != nil {
			freeDistinctWave(&children)
			return false, err
		}
		owned = false
		partition = nil
		if err := controller.pushPartialChildren(&children); err != nil {
			freeDistinctWave(&children)
			return false, err
		}
	}

	if err := ctr.initializeDistinctPartialLeafState(); err != nil {
		return false, err
	}
	if err := partition.flushWriter(); err != nil {
		return false, err
	}
	reader, err := newGroupSpillReader(ctr, partition.file, proc.Ctx)
	if err != nil {
		return false, err
	}
	defer reader.Free()
	if err := reader.Rewind(partitionOffset); err != nil {
		return false, err
	}
	loadedRecords := 0
	for {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return false, err
		}
		recordStart := reader.Position()
		_, _, aggregate, payload, eof, err := controller.readRecord(
			reader, decodeGroups)
		if err != nil {
			return false, err
		}
		if eof {
			break
		}
		if err := ctr.insertDistinctPartialRecord(
			proc, decodeGroups, aggregate, payload); err != nil {
			if !mpool.IsRetryableAllocationCapacity(err) {
				return false, err
			}
			if loadedRecords == 0 {
				dropped, rewindErr := reader.DisableReadAheadAndRewind(recordStart)
				if rewindErr != nil {
					return false, rewindErr
				}
				if !dropped {
					return false, err
				}
				if err := ctr.initializeDistinctPartialLeafState(); err != nil {
					return false, err
				}
				continue
			}
			reader.DropReadAhead()
			controller.partialActive = partition
			controller.partialActiveOffset = recordStart
			controller.partialContinuations++
			owned = false
			ctr.currBatchIdx = 0
			return true, nil
		}
		loadedRecords++
	}
	if loadedRecords == 0 {
		return false, moerr.NewInternalErrorNoCtx(
			"distinct partial continuation produced an empty leaf")
	}
	if err := partition.free(); err != nil {
		return false, err
	}
	owned = false
	partition = nil
	ctr.currBatchIdx = 0
	return true, nil
}

func (ctr *container) finalizeExactCountDistinct(
	proc *process.Process,
	opAnalyzer process.Analyzer,
) error {
	if ctr == nil || ctr.distinctFinalized {
		return nil
	}
	if ctr.distinctContributionsPrepared {
		return nil
	}
	if ctr.distinctSpill == nil {
		ctr.distinctFinalized = true
		return nil
	}
	if _, err := ctr.drainExactCountDistinct(proc, opAnalyzer); err != nil {
		return err
	}
	if ctr.distinctSpill == nil {
		ctr.distinctFinalized = true
		return nil
	}
	if ctr.mtyp == H0 {
		return ctr.finalizeH0ExactCountDistinct(proc)
	}
	return ctr.finalizeGroupedExactCountDistinctViaSpill(proc, opAnalyzer)
}
