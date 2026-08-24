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

package aggexec

import (
	"encoding"
	io "io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var emptyAggregateSelection = []uint8{}

// aggregateChunkSelection interprets a missing/empty chunk as an omitted
// selection, not as UnionBatch's nil="all rows" shorthand. Group spill uses
// this compact shape so it can serialize one aggregate chunk without building
// a zero-filled selector for every other resident chunk.
func aggregateChunkSelection(
	flags [][]uint8,
	chunk int,
	rows int,
) ([]uint8, error) {
	if chunk < 0 || rows < 0 {
		return nil, moerr.NewInvalidInputNoCtx(
			"invalid aggregate selection chunk")
	}
	if chunk >= len(flags) || len(flags[chunk]) == 0 {
		return emptyAggregateSelection, nil
	}
	if len(flags[chunk]) > rows {
		return nil, moerr.NewInvalidInputNoCtxf(
			"aggregate selection length %d exceeds chunk row count %d",
			len(flags[chunk]), rows)
	}
	for _, selected := range flags[chunk] {
		if selected > 1 {
			return nil, moerr.NewInvalidInputNoCtx(
				"aggregate selection flag must be zero or one")
		}
	}
	return flags[chunk], nil
}

func validateAggregateSelections(
	cnt int64,
	flags [][]uint8,
	ret *optSplitResult,
) error {
	if cnt < 0 || ret == nil {
		return moerr.NewInvalidInputNoCtx("invalid aggregate selection")
	}
	var selected int64
	for chunk, result := range ret.resultList {
		selection, err := aggregateChunkSelection(
			flags, chunk, result.Length())
		if err != nil {
			return err
		}
		for _, flag := range selection {
			selected += int64(flag)
		}
	}
	for chunk := len(ret.resultList); chunk < len(flags); chunk++ {
		if len(flags[chunk]) != 0 {
			return moerr.NewInvalidInputNoCtxf(
				"aggregate selection chunk %d exceeds result chunks %d",
				chunk, len(ret.resultList))
		}
	}
	if selected != cnt {
		return moerr.NewInvalidInputNoCtxf(
			"aggregate selection count %d does not match %d", selected, cnt)
	}
	return nil
}

func marshalRetAndGroupsToBuffer[T encoding.BinaryMarshaler](
	cnt int64, flags [][]uint8, writer io.Writer,
	ret *optSplitResult, groups []T, extra [][]byte) error {
	if err := validateAggregateSelections(cnt, flags, ret); err != nil {
		return err
	}
	if err := types.WriteInt64(writer, cnt); err != nil {
		return err
	}
	if cnt == 0 {
		return nil
	}
	if err := ret.marshalToBuffers(flags, writer); err != nil {
		return err
	}

	if len(groups) == 0 {
		if err := types.WriteInt64(writer, 0); err != nil {
			return err
		}
	} else {
		if err := types.WriteInt64(writer, cnt); err != nil {
			return err
		}
		groupIdx := 0
		for chunk, result := range ret.resultList {
			rows := result.Length()
			if groupIdx > len(groups)-rows {
				return moerr.NewInvalidInputNoCtx(
					"aggregate group state is shorter than result state")
			}
			selection, err := aggregateChunkSelection(flags, chunk, rows)
			if err != nil {
				return err
			}
			for row, selected := range selection {
				if selected == 1 {
					bs, err := groups[groupIdx+row].MarshalBinary()
					if err != nil {
						return err
					}
					if err = types.WriteSizeBytes(bs, writer); err != nil {
						return err
					}
				}
			}
			groupIdx += rows
		}
		if groupIdx != len(groups) {
			return moerr.NewInvalidInputNoCtx(
				"aggregate group state is longer than result state")
		}
	}

	cnt = int64(len(extra))
	if err := types.WriteInt64(writer, cnt); err != nil {
		return err
	}
	for i := range extra {
		if err := types.WriteSizeBytes(extra[i], writer); err != nil {
			return err
		}
	}
	return nil
}

func marshalChunkToBuffer[T encoding.BinaryMarshaler](
	chunk int, writer io.Writer,
	ret *optSplitResult, groups []T, extra [][]byte) error {
	if writer == nil || ret == nil || chunk < 0 ||
		chunk >= len(ret.resultList) {
		return moerr.NewInvalidInputNoCtx("invalid aggregate chunk")
	}
	chunkSz := ret.optInformation.chunkSize
	start := chunkSz * chunk
	chunkNGroup := ret.getNthChunkSize(chunk)
	if chunkSz <= 0 || chunkNGroup < 0 {
		return moerr.NewInvalidInputNoCtx("invalid aggregate chunk")
	}

	cnt := int64(chunkNGroup)
	if err := types.WriteInt64(writer, cnt); err != nil {
		return err
	}
	if cnt == 0 {
		return nil
	}

	if err := ret.marshalChunkToBuffer(chunk, writer); err != nil {
		return err
	}

	if len(groups) == 0 {
		if err := types.WriteInt64(writer, 0); err != nil {
			return err
		}
	} else {
		if err := types.WriteInt64(writer, cnt); err != nil {
			return err
		}
		if start < 0 || start > len(groups)-chunkNGroup {
			return moerr.NewInvalidInputNoCtx(
				"aggregate group state is shorter than result chunk")
		}
		for i := 0; i < chunkNGroup; i++ {
			bs, err := groups[start+i].MarshalBinary()
			if err != nil {
				return err
			}
			if err = types.WriteSizeBytes(bs, writer); err != nil {
				return err
			}
		}
	}

	cnt = int64(len(extra))
	if err := types.WriteInt64(writer, cnt); err != nil {
		return err
	}
	for i := range extra {
		if err := types.WriteSizeBytes(extra[i], writer); err != nil {
			return err
		}
	}

	return nil
}

func unmarshalFromReaderNoGroup(reader io.Reader, ret *optSplitResult) (int, error) {
	if reader == nil || ret == nil {
		return 0, moerr.NewInvalidInputNoCtx("invalid aggregate result decoder")
	}
	cnt, err := types.ReadInt64(reader)
	if err != nil {
		return 0, err
	}
	maxInt := int64(^uint(0) >> 1)
	if cnt < 0 || cnt > maxInt {
		return 0, moerr.NewInvalidInputNoCtxf(
			"invalid aggregate result row count %d", cnt)
	}
	if cnt == 0 {
		ret.resetEmpty()
		if ret.optInformation.chunkSize <= 0 {
			ret.optInformation.chunkSize = GetChunkSizeFromType(ret.resultType)
		}
		return 0, nil
	}
	if err := ret.unmarshalFromReader(reader); err != nil {
		return 0, err
	}
	rows := 0
	for _, result := range ret.resultList {
		if result == nil || result.Length() > int(cnt)-rows {
			ret.free()
			return 0, moerr.NewInvalidInputNoCtxf(
				"aggregate result row count exceeds %d", cnt)
		}
		rows += result.Length()
	}
	if rows != int(cnt) {
		ret.free()
		return 0, moerr.NewInvalidInputNoCtxf(
			"aggregate result row count %d does not match %d", rows, cnt)
	}
	if (len(ret.emptyList) != 0) !=
		ret.optInformation.doesThisNeedEmptyList {
		ret.free()
		return 0, moerr.NewInvalidInputNoCtx(
			"aggregate empty-state presence does not match result type")
	}
	if (len(ret.distinct) != 0) != ret.optInformation.hasDistinct {
		ret.free()
		return 0, moerr.NewInvalidInputNoCtx(
			"aggregate distinct-state presence does not match result type")
	}
	if len(ret.emptyList) != 0 {
		if len(ret.emptyList) != len(ret.resultList) {
			ret.free()
			return 0, moerr.NewInvalidInputNoCtx(
				"aggregate empty-state chunks do not match result chunks")
		}
		for i := range ret.emptyList {
			if ret.emptyList[i] == nil ||
				ret.emptyList[i].Length() != ret.resultList[i].Length() {
				ret.free()
				return 0, moerr.NewInvalidInputNoCtx(
					"aggregate empty-state rows do not match result rows")
			}
		}
	}
	if len(ret.distinct) != 0 {
		if len(ret.distinct) != len(ret.resultList) {
			ret.free()
			return 0, moerr.NewInvalidInputNoCtx(
				"aggregate distinct-state chunks do not match result chunks")
		}
		for i := range ret.distinct {
			if len(ret.distinct[i].maps) != ret.resultList[i].Length() {
				ret.free()
				return 0, moerr.NewInvalidInputNoCtx(
					"aggregate distinct-state rows do not match result rows")
			}
		}
	}
	ret.optInformation.chunkSize = rows
	return rows, nil
}

func readAggregateExtra(reader io.Reader) error {
	cnt, err := types.ReadInt64(reader)
	if err != nil {
		return err
	}
	if cnt < 0 {
		return moerr.NewInvalidInputNoCtxf(
			"invalid aggregate extra state count %d", cnt)
	}
	for i := int64(0); i < cnt; i++ {
		size, readErr := types.ReadInt32(reader)
		if readErr != nil {
			return readErr
		}
		if size < 0 {
			return moerr.NewInvalidInputNoCtxf(
				"invalid aggregate extra state size %d", size)
		}
		if _, readErr = io.CopyN(io.Discard, reader, int64(size)); readErr != nil {
			return readErr
		}
	}
	return nil
}
