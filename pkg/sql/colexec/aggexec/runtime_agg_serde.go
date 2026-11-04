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
	"bytes"
	"encoding"
	io "io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Median groups can write their existing wire representation directly. Other
// aggregate states keep their BinaryMarshaler path.
type runtimeAggGroupWriter interface {
	MarshalBinarySize() (int, error)
	MarshalBinaryTo(io.Writer) error
}

func writeRuntimeAggGroup(group encoding.BinaryMarshaler, writer io.Writer) error {
	if streamed, ok := group.(runtimeAggGroupWriter); ok {
		size, err := streamed.MarshalBinarySize()
		if err != nil {
			return err
		}
		if size < 0 || size > math.MaxInt32 {
			return moerr.NewInvalidInputNoCtx("aggregate group exceeds marshal format")
		}
		prefix := int32(size)
		written, err := writer.Write(types.EncodeInt32(&prefix))
		if err != nil {
			return err
		}
		if written != 4 {
			return io.ErrShortWrite
		}
		return streamed.MarshalBinaryTo(writer)
	}
	data, err := group.MarshalBinary()
	if err != nil {
		return err
	}
	return types.WriteSizeBytes(data, writer)
}

// Aggregate selections omit missing or empty chunks. This differs from
// Vector.UnionBatch, where a nil selection means all rows.
func validateRuntimeAggSelection(cnt int64, flags [][]uint8, ret *optSplitResult, groupCount int) error {
	if cnt <= 0 || ret == nil {
		return moerr.NewInvalidInputNoCtx("invalid aggregate selection")
	}
	if len(ret.emptyList) != 0 && len(ret.emptyList) != len(ret.resultList) {
		return moerr.NewInvalidInputNoCtx("aggregate result and empty chunks do not match")
	}
	var selected int64
	rows := 0
	for i, result := range ret.resultList {
		n := result.Length()
		rows += n
		if len(ret.emptyList) != 0 && ret.emptyList[i].Length() != n {
			return moerr.NewInvalidInputNoCtx("aggregate result and empty rows do not match")
		}
		if i >= len(flags) {
			continue
		}
		if len(flags[i]) > n {
			return moerr.NewInvalidInputNoCtx("aggregate selection exceeds chunk length")
		}
		ones := bytes.Count(flags[i], []byte{1})
		if ones+bytes.Count(flags[i], []byte{0}) != len(flags[i]) {
			return moerr.NewInvalidInputNoCtx("aggregate selection flag must be zero or one")
		}
		selected += int64(ones)
	}
	for i := len(ret.resultList); i < len(flags); i++ {
		if len(flags[i]) != 0 {
			return moerr.NewInvalidInputNoCtx("aggregate selection exceeds chunk count")
		}
	}
	if selected != cnt {
		return moerr.NewInvalidInputNoCtx("aggregate selection count does not match")
	}
	if groupCount != 0 && groupCount != rows {
		return moerr.NewInvalidInputNoCtx("aggregate group and result counts do not match")
	}
	return nil
}

func marshalRetAndGroupsToBuffer[T encoding.BinaryMarshaler](
	cnt int64, flags [][]uint8, buf *bytes.Buffer,
	ret *optSplitResult, groups []T, extra [][]byte) error {
	if err := validateRuntimeAggSelection(cnt, flags, ret, len(groups)); err != nil {
		return err
	}
	types.WriteInt64(buf, cnt)
	if err := ret.marshalToBuffers(flags, buf); err != nil {
		return err
	}

	if len(groups) == 0 {
		types.WriteInt64(buf, 0)
	} else {
		types.WriteInt64(buf, cnt)
		groupIdx := 0
		for i, result := range ret.resultList {
			if i < len(flags) {
				for j, selected := range flags[i] {
					if selected == 1 {
						if err := writeRuntimeAggGroup(groups[groupIdx+j], buf); err != nil {
							return err
						}
					}
				}
			}
			groupIdx += result.Length()
		}
	}

	cnt = int64(len(extra))
	types.WriteInt64(buf, cnt)
	for i := range extra {
		if err := types.WriteSizeBytes(extra[i], buf); err != nil {
			return err
		}
	}
	return nil
}

func marshalChunkToBuffer[T encoding.BinaryMarshaler](
	chunk int, buf *bytes.Buffer,
	ret *optSplitResult, groups []T, extra [][]byte) error {
	if chunk < 0 || chunk >= len(ret.resultList) {
		return moerr.NewInternalErrorNoCtx("invalid chunk number")
	}
	chunkSz := ret.optInformation.chunkSize
	start := chunkSz * chunk
	chunkNGroup := ret.getNthChunkSize(chunk)

	cnt := int64(chunkNGroup)
	buf.Write(types.EncodeInt64(&cnt))

	if err := ret.marshalChunkToBuffer(chunk, buf); err != nil {
		return err
	}

	if len(groups) == 0 {
		types.WriteInt64(buf, 0)
	} else {
		types.WriteInt64(buf, cnt)
		for i := 0; i < chunkNGroup; i++ {
			if err := writeRuntimeAggGroup(groups[start+i], buf); err != nil {
				return err
			}
		}
	}

	cnt = int64(len(extra))
	types.WriteInt64(buf, cnt)
	for i := range extra {
		if err := types.WriteSizeBytes(extra[i], buf); err != nil {
			return err
		}
	}

	return nil
}

func unmarshalFromReaderNoGroup(reader io.Reader, ret *optSplitResult) error {
	cnt, err := types.ReadInt64(reader)
	if err != nil {
		return err
	}
	ret.optInformation.chunkSize = int(cnt)
	if err := ret.unmarshalFromReader(reader); err != nil {
		return err
	}
	return nil
}
