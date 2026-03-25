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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func marshalRetAndGroupsToBuffer[T encoding.BinaryMarshaler](
	cnt int64, flags [][]uint8, buf *bytes.Buffer,
	ret *optSplitResult, groups []T, extra [][]byte) error {
	types.WriteInt64(buf, cnt)
	if cnt == 0 {
		return nil
	}
	if err := ret.marshalToBuffers(flags, buf); err != nil {
		return err
	}

	if len(groups) == 0 {
		types.WriteInt64(buf, 0)
	} else {
		types.WriteInt64(buf, cnt)
		groupIdx := 0
		for i := range flags {
			for j := range flags[i] {
				if flags[i][j] == 1 {
					bs, err := groups[groupIdx].MarshalBinary()
					if err != nil {
						return err
					}
					if err = types.WriteSizeBytes(bs, buf); err != nil {
						return err
					}
				}
				groupIdx += 1
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

func marshalChunkToBuffer[T encoding.BinaryMarshaler](
	chunk int, buf *bytes.Buffer,
	ret *optSplitResult, groups []T, extra [][]byte) error {
	chunkSz := ret.optInformation.chunkSize
	start := chunkSz * chunk
	chunkNGroup := ret.getNthChunkSize(chunk)
	if chunkSz < 0 {
		return moerr.NewInternalErrorNoCtx("invalid chunk number.")
	}

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
			bs, err := groups[start+i].MarshalBinary()
			if err != nil {
				return err
			}
			if err = types.WriteSizeBytes(bs, buf); err != nil {
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
