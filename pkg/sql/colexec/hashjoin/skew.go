// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package hashjoin

import (
	"bufio"
	"io"
	"math"
	"os"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	skewLoadChunk = iota
	skewProbeChunk
	skewFinalizeBuild
	skewFinalizeProbe
	skewDone
)

// The sidecar stores a saturating match count per stable probe row ordinal.
// Exactly one 4 KiB page is resident; the file is never mmap'ed.
type spillMatchState struct {
	file   *os.File
	page   [4096]byte
	pageID int64
	dirty  bool
}

func (s *spillMatchState) flush() error {
	if !s.dirty {
		return nil
	}
	n, err := s.file.WriteAt(s.page[:], s.pageID*int64(len(s.page)))
	if err == nil && n != len(s.page) {
		err = io.ErrShortWrite
	}
	if err == nil {
		s.dirty = false
	}
	return err
}
func (s *spillMatchState) load(id int64) error {
	page := id / int64(len(s.page))
	if s.pageID == page {
		return nil
	}
	if err := s.flush(); err != nil {
		return err
	}
	clear(s.page[:])
	n, err := s.file.ReadAt(s.page[:], page*int64(len(s.page)))
	if err != nil && err != io.EOF {
		return err
	}
	if n < 0 {
		return io.ErrUnexpectedEOF
	}
	s.pageID = page
	return nil
}
func (s *spillMatchState) count(id int64) (byte, error) {
	if err := s.load(id); err != nil {
		return 0, err
	}
	return s.page[id%int64(len(s.page))], nil
}
func (s *spillMatchState) match(id int64) (byte, error) {
	old, err := s.count(id)
	if err != nil {
		return 0, err
	}
	if old < 2 {
		s.page[id%int64(len(s.page))] = old + 1
		s.dirty = true
	}
	return old, nil
}
func (s *spillMatchState) close() {
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
}

type skewJoin struct {
	phase                    int
	build, probe             *os.File
	buildReader, probeReader spillBucketReader
	buildFrame, probeFrame   *batch.Batch
	buildRow                 int
	buildEOF                 bool
	probeRow                 int
	probeID                  int64
	rowOpen                  bool
	matchRows                []int32
	single                   [1]int32
	matchPos                 int
	finalizeRow              int64
	matches                  spillMatchState
	inputLeases              [2]*rscthrottler.InputLease
	inputBytes               int64
}

func (r *spillBucketReader) resetBorrowedFd(fd *os.File) {
	r.close()
	r.empty = fd == nil
	if fd == nil {
		return
	}
	reader := io.NewSectionReader(fd, 0, 1<<63-1)
	if r.reader == nil {
		r.reader = bufio.NewReaderSize(reader, spillIOBufferSize)
		r.buf = make([]byte, 8)
	} else {
		r.reader.Reset(reader)
	}
}

func (hashJoin *HashJoin) startSkew(proc *process.Process, bucket spillBucket) error {
	skew := &skewJoin{build: bucket.buildFd, probe: bucket.probeFd, inputBytes: 64 << 20}
	if proc.Base.ImportMemory != nil {
		skew.inputBytes = proc.Base.ImportMemory.JoinInputBytes
		if hashJoin.ctr.joinInputLeases[0] == nil {
			for i := range skew.inputLeases {
				lease, err := proc.Base.ImportMemory.AcquireJoinInput(proc.Ctx)
				if err != nil {
					for _, old := range skew.inputLeases {
						if old != nil {
							old.Release()
						}
					}
					return err
				}
				skew.inputLeases[i] = lease
			}
		}
	}
	fs, err := hashJoin.ctr.getSpillFS(proc)
	if err != nil {
		for _, lease := range skew.inputLeases {
			if lease != nil {
				lease.Release()
			}
		}
		return err
	}
	uid, err := uuid.NewV7()
	if err != nil {
		for _, lease := range skew.inputLeases {
			if lease != nil {
				lease.Release()
			}
		}
		return err
	}
	file, err := fs.CreateAndRemoveFile(proc.Ctx, "join_"+uid.String()+"_matches")
	if err != nil {
		for _, lease := range skew.inputLeases {
			if lease != nil {
				lease.Release()
			}
		}
		return err
	}
	skew.matches = spillMatchState{file: file, pageID: -1}
	skew.buildReader.resetBorrowedFd(skew.build)
	skew.buildFrame = batch.NewOffHeapWithSize(0)
	skew.probeFrame = batch.NewOffHeapWithSize(0)
	hashJoin.ctr.skew = skew
	hashJoin.ctr.activeBucket = nil
	return nil
}

func (skew *skewJoin) close(proc *process.Process) {
	skew.buildReader.close()
	skew.probeReader.close()
	if skew.build != nil {
		_ = skew.build.Close()
		skew.build = nil
	}
	if skew.probe != nil {
		_ = skew.probe.Close()
		skew.probe = nil
	}
	skew.matches.close()
	if skew.buildFrame != nil {
		skew.buildFrame.Clean(proc.Mp())
		skew.buildFrame = nil
	}
	if skew.probeFrame != nil {
		skew.probeFrame.Clean(proc.Mp())
		skew.probeFrame = nil
	}
	for i, lease := range skew.inputLeases {
		if lease != nil {
			lease.Release()
			skew.inputLeases[i] = nil
		}
	}
}

func (hashJoin *HashJoin) loadSkewChunk(proc *process.Process) error {
	ctr := &hashJoin.ctr
	skew := ctr.skew
	ctr.cleanHashMap()
	ctr.itr = nil
	ctr.rightRowsMatched = nil
	builder := &hashbuild.HashmapBuilder{}
	defer builder.Free(proc)
	if err := builder.Prepare(hashJoin.EqConds[1], -1, -1, nil, proc); err != nil {
		return err
	}
	builder.Batches.TrackAllocation()
	limit := int64(64 << 20)
	if proc.Base.ImportMemory != nil {
		limit = proc.Base.ImportMemory.UnitBytes
	}
	rowsLimit := 8192
	if ctr.spillThreshold > 0 && ctr.spillThreshold <= 100000 {
		rowsLimit = int(max(int64(1), min(int64(rowsLimit), ctr.spillThreshold-1)))
	} else if ctr.spillThreshold > 100000 {
		limit = min(limit, ctr.spillThreshold)
	}
	keyWindow, maxKeyWindow, inputPayload := int64(0), int64(0), int64(0)
	for builder.InputBatchRowCount < rowsLimit {
		if skew.buildRow >= skew.buildFrame.RowCount() {
			if skew.buildEOF {
				break
			}
			frame, err := skew.buildReader.readBatch(proc, skew.buildFrame)
			if err == io.EOF {
				skew.buildEOF = true
				break
			}
			if err != nil {
				return err
			}
			skew.buildRow = 0
			if frame.RowCount() == 0 {
				continue
			}
		}
		view, err := skew.buildFrame.Window(skew.buildRow, skew.buildRow+1)
		if err != nil {
			return err
		}
		growth := builder.Batches.AppendAllocation(view)
		current := builder.Batches.RetainedBytes()
		rowKey, keyOK := hashbuild.EncodedKeyRowBytes(hashJoin.EqConds[1], skew.buildFrame, skew.buildRow)
		nextKeyWindow := keyWindow
		if builder.InputBatchRowCount%hashmap.UnitLimit == 0 {
			nextKeyWindow = 0
		}
		keyOK = keyOK && rowKey <= math.MaxInt64-nextKeyWindow
		if keyOK {
			nextKeyWindow += rowKey
		}
		nextMaxKeyWindow := max(maxKeyWindow, nextKeyWindow)
		viewBytes := int64(0)
		for _, vec := range view.Vecs {
			var ok bool
			viewBytes, ok = checkedEnvelopeAdd(viewBytes, vec.WindowSize(0, view.RowCount()))
			if !ok {
				view.Clean(proc.Mp())
				return moerr.NewInternalError(proc.Ctx, "spill input payload overflows")
			}
		}
		nextInputPayload, payloadOK := checkedEnvelopeAdd(inputPayload, viewBytes)
		expressionBytes := int64(0)
		if !keyOK && payloadOK {
			keys, ok := hashbuild.SpillKeyMemoryBound(hashJoin.EqConds[1], nextInputPayload, builder.InputBatchRowCount+1)
			keyOK = ok
			nextMaxKeyWindow = keys.EncodedUnit
			expressionBytes = keys.ExpressionBytes
		}
		hash, ok := hashmap.BuildMemoryBound(uint64(builder.InputBatchRowCount+1), builder.UsesStringKeys(), nextMaxKeyWindow, true)
		if ok {
			hash, ok = checkedEnvelopeAdd(hash, expressionBytes)
		}
		ok = ok && keyOK && payloadOK
		fits := ok && growth <= limit-current && current+growth+hash <= skew.inputBytes
		if !fits && builder.InputBatchRowCount > 0 {
			view.Clean(proc.Mp())
			break
		}
		if !ok || current+growth+hash > skew.inputBytes {
			view.Clean(proc.Mp())
			return moerr.NewInternalError(proc.Ctx, "single join row exceeds admitted spill working set")
		}
		err = builder.Batches.CopyIntoBatches(view, proc)
		view.Clean(proc.Mp())
		if err != nil {
			return err
		}
		builder.InputBatchRowCount++
		keyWindow, maxKeyWindow, inputPayload = nextKeyWindow, nextMaxKeyWindow, nextInputPayload
		skew.buildRow++
	}
	if builder.InputBatchRowCount == 0 {
		if hashJoin.EmitUnmatchedProbe() {
			skew.phase = skewFinalizeProbe
			skew.rewindProbe()
		} else {
			skew.phase = skewDone
		}
		return nil
	}
	if err := builder.BuildHashmap(hashJoin.HashOnPK, !hashJoin.HashOnPK, false, proc); err != nil {
		return err
	}
	ctr.mp = builder.GetJoinMap(proc.Mp())
	ctr.mp.SetRowCount(int64(builder.InputBatchRowCount))
	ctr.mp.IncRef(1)
	ctr.rightBats = ctr.mp.GetBatches()
	ctr.rightRowCnt = int64(builder.InputBatchRowCount)
	builder.FreeExecutors()
	if hashJoin.EmitUnmatchedBuild() {
		ctr.rightRowsMatched = &bitmap.Bitmap{}
		ctr.rightRowsMatched.InitWithSize(ctr.rightRowCnt)
	}
	skew.phase = skewProbeChunk
	skew.rewindProbe()
	return nil
}

func (skew *skewJoin) rewindProbe() {
	skew.probeReader.resetBorrowedFd(skew.probe)
	skew.probeFrame.CleanOnlyData()
	skew.probeRow = 0
	skew.probeID = 0
	skew.rowOpen = false
	skew.matchRows = nil
	skew.matchPos = 0
}

func (hashJoin *HashJoin) nextSkewProbe(proc *process.Process) (bool, error) {
	ctr := &hashJoin.ctr
	skew := ctr.skew
	for skew.probeRow >= skew.probeFrame.RowCount() {
		frame, err := skew.probeReader.readBatch(proc, skew.probeFrame)
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		skew.probeRow = 0
		if frame.RowCount() == 0 {
			continue
		}
		ctr.leftBat = frame
		if err := ctr.evalJoinCondition(frame, proc); err != nil {
			return false, err
		}
		if hashJoin.NonEqCond != nil {
			if ctr.joinBats[0] == nil {
				ctr.joinBats[0], ctr.cfs1 = colexec.NewJoinBatch(frame, proc.Mp())
			}
			if ctr.joinBats[1] == nil && len(ctr.rightBats) > 0 {
				ctr.joinBats[1], ctr.cfs2 = colexec.NewJoinBatch(ctr.rightBats[0], proc.Mp())
			}
		}
	}
	ctr.leftBat = skew.probeFrame
	return true, nil
}

func (hashJoin *HashJoin) appendSkewBuildRow(proc *process.Process, row int64) error {
	ctr := &hashJoin.ctr
	index, offset := row/8192, row%8192
	for col, projection := range hashJoin.ResultCols {
		if projection.Rel == 0 {
			if err := ctr.resBat.Vecs[col].UnionNull(proc.Mp()); err != nil {
				return err
			}
			if ctr.outputTarget > 0 {
				ctr.outputBytes += int64(hashJoin.LeftTypes[projection.Pos].TypeSize())
			}
		} else {
			vec := ctr.rightBats[index].Vecs[projection.Pos]
			if ctr.outputTarget > 0 {
				bytes, err := preExtendJoinValue(ctr.resBat.Vecs[col], vec, offset, proc)
				if err != nil {
					return err
				}
				ctr.outputBytes += bytes
			}
			if err := ctr.resBat.Vecs[col].UnionOne(vec, offset, proc.Mp()); err != nil {
				return err
			}
		}
	}
	return nil
}

// callSkew advances durable match state and cursors before returning output.
// No W permit is retained across Call; build/probe/sidecar state belongs to I.
func (hashJoin *HashJoin) callSkew(proc *process.Process) (vm.CallResult, bool, error) {
	ctr := &hashJoin.ctr
	skew := ctr.skew
	result := vm.NewCallResult()
	hashJoin.resetResultBat()
	out := 0
	for {
		if err := proc.Ctx.Err(); err != nil {
			return result, false, err
		}
		if ctr.spillWork != nil && ctr.spillWork.Ctx.Err() != nil {
			return result, false, ctr.spillWork.Ctx.Err()
		}
		switch skew.phase {
		case skewLoadChunk:
			if err := hashJoin.loadSkewChunk(proc); err != nil {
				return result, false, err
			}
		case skewProbeChunk:
			if !skew.rowOpen {
				has, err := hashJoin.nextSkewProbe(proc)
				if err != nil {
					return result, false, err
				}
				if !has {
					skew.phase = skewFinalizeBuild
					skew.finalizeRow = 0
					continue
				}
				if ctr.itr == nil {
					ctr.itr = ctr.mp.NewIterator()
				}
				values, valid := ctr.itr.Find(skew.probeRow, 1, ctr.eqCondVecs)
				skew.matchRows = nil
				skew.matchPos = 0
				if valid[0] != 0 && values[0] != 0 {
					if ctr.mp.HashOnUnique() {
						skew.single[0] = int32(values[0] - 1)
						skew.matchRows = skew.single[:]
					} else {
						skew.matchRows = ctr.mp.GetSels(values[0] - 1)
					}
				}
				skew.rowOpen = true
			}
			if skew.matchPos >= len(skew.matchRows) {
				skew.rowOpen = false
				skew.probeRow++
				skew.probeID++
				continue
			}
			buildRow := int64(skew.matchRows[skew.matchPos])
			skew.matchPos++
			if hashJoin.NonEqCond != nil {
				ok, err := ctr.evalNonEqCondition(ctr.leftBat, int64(skew.probeRow), proc, buildRow/8192, buildRow%8192)
				if err != nil {
					return result, false, err
				}
				if !ok {
					continue
				}
			}
			old, err := skew.matches.match(skew.probeID)
			if err != nil {
				return result, false, err
			}
			if hashJoin.IsLeftSingle() && old > 0 {
				return result, false, moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
			}
			if ctr.rightRowsMatched != nil {
				if hashJoin.IsRightSingle() && ctr.rightRowsMatched.Contains(uint64(buildRow)) {
					return result, false, moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
				}
				ctr.rightRowsMatched.Add(uint64(buildRow))
			}
			emit := !hashJoin.IsAnti() && !hashJoin.IsRightSemi() && (!hashJoin.IsLeftSemi() || old == 0)
			if emit {
				if err := ctr.appendOneMatch(hashJoin, proc, int64(skew.probeRow), buildRow/8192, buildRow%8192); err != nil {
					return result, false, err
				}
				out++
			}
		case skewFinalizeBuild:
			if skew.finalizeRow >= ctr.rightRowCnt {
				skew.phase = skewLoadChunk
				continue
			}
			row := skew.finalizeRow
			skew.finalizeRow++
			if !hashJoin.EmitUnmatchedBuild() {
				continue
			}
			matched := ctr.rightRowsMatched.Contains(uint64(row))
			if hashJoin.IsRightSemi() != matched {
				continue
			}
			if err := hashJoin.appendSkewBuildRow(proc, row); err != nil {
				return result, false, err
			}
			out++
		case skewFinalizeProbe:
			has, err := hashJoin.nextSkewProbe(proc)
			if err != nil {
				return result, false, err
			}
			if !has {
				skew.phase = skewDone
				continue
			}
			row, id := skew.probeRow, skew.probeID
			skew.probeRow++
			skew.probeID++
			count, err := skew.matches.count(id)
			if err != nil {
				return result, false, err
			}
			if count == 0 {
				if err := ctr.appendOneNotMatch(hashJoin, proc, int64(row)); err != nil {
					return result, false, err
				}
				out++
			}
		case skewDone:
			ctr.cleanHashMap()
			ctr.leftBat = nil
			skew.close(proc)
			ctr.skew = nil
			if out > 0 {
				ctr.resBat.SetRowCount(out)
				result.Batch = ctr.resBat
				return result, false, nil
			}
			return result, true, nil
		}
		if out >= colexec.DefaultBatchSize || out > 0 && ctr.outputFull() {
			ctr.resBat.SetRowCount(out)
			result.Batch = ctr.resBat
			return result, false, nil
		}
	}
}
