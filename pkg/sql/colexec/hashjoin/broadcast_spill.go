// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package hashjoin

import (
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Each physical probe first spools its bounded input. Only the last registration
// becomes a logical finalizer; every other probe exits without a bitmap wait.
func (hashJoin *HashJoin) buildBroadcastSpill(proc *process.Process, analyzer process.Analyzer) (err error) {
	ctr := &hashJoin.ctr
	defer func() {
		ctr.mp = nil
		if err != nil {
			hashJoin.SpillGroup.Abort(hashJoin.SpillEpoch)
		}
	}()
	writers, err := createRootProbeSpillBucketFiles()
	if err != nil {
		return err
	}
	buffers := make([]*batch.Batch, spillNumBuckets)
	defer func() {
		for i := range writers {
			writers[i].close()
		}
		cleanSpillBuffers(buffers, proc)
	}()
	for {
		input, callErr := vm.ChildrenCall(hashJoin.GetChildren(0), proc, analyzer)
		if callErr != nil {
			return callErr
		}
		if input.Batch == nil {
			break
		}
		if !input.Batch.IsEmpty() {
			if err = ctr.appendProbeBatchToSpillFiles(proc, input.Batch, writers, buffers, analyzer, 0); err != nil {
				return err
			}
		}
	}
	for i, buf := range buffers {
		if _, err = ctr.flushBucketBuffer(proc, buf, &writers[i], analyzer); err != nil {
			return err
		}
	}
	files := make([]*os.File, spillNumBuckets)
	for i := range writers {
		files[i] = writers[i].handOffFd()
	}
	work, err := hashJoin.SpillGroup.Register(hashJoin.SpillEpoch, hashJoin.SpillParticipantID, files)
	if err != nil {
		for _, f := range files {
			if f != nil {
				_ = f.Close()
			}
		}
		return err
	}
	ctr.spillParticipant = spillParticipantRegistered
	if work == nil {
		return nil
	}
	ctr.spillWork, ctr.spillConsumer = work, true
	ctr.spillParticipant = spillParticipantFinalizer
	buildFiles, err := work.Build.TakeFiles()
	if err != nil {
		return err
	}
	uid, err := uuid.NewV7()
	if err != nil {
		for _, f := range buildFiles {
			if f != nil {
				_ = f.Close()
			}
		}
		return err
	}
	ctr.spillQueue = make([]spillBucket, len(buildFiles))
	for i, f := range buildFiles {
		ctr.spillQueue[i] = spillBucket{buildFd: f, baseName: fmt.Sprintf("%s_%d", uid, i), depth: 1}
	}
	// Complete frames are concatenable. ReadAt keeps each source's logical file
	// offset independent, and io.Copy uses bounded scratch rather than whole files.
	for bucket := range ctr.spillQueue {
		var target *os.File
		for participant := range work.Probes {
			if err := work.Ctx.Err(); err != nil {
				return err
			}
			if bucket >= len(work.Probes[participant]) {
				continue
			}
			source := work.Probes[participant][bucket]
			if source == nil {
				continue
			}
			if target == nil {
				fs, fsErr := ctr.getSpillFS(proc)
				if fsErr != nil {
					return fsErr
				}
				target, err = fs.CreateAndRemoveFile(proc.Ctx, fmt.Sprintf("join_%s_%d_combined_probe", uid, bucket))
				if err != nil {
					return err
				}
				ctr.spillQueue[bucket].probeFd = target
			}
			info, statErr := source.Stat()
			if statErr != nil {
				return statErr
			}
			if _, err = io.Copy(target, io.NewSectionReader(source, 0, info.Size())); err != nil {
				return err
			}
			_ = source.Close()
			work.Probes[participant][bucket] = nil
		}
	}
	return nil
}
