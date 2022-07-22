// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"bytes"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

func WriteValueInto[T any](tool *CodecTool, v T) (n int64, err error) {
	nr, err := tool.Write(types.EncodeFixed(v))
	n = int64(nr)
	return
}

func GetValueFrom[T any](tool *CodecTool, i int) (v T) {
	buf := tool.Get(i)
	v = types.DecodeFixed[T](buf)
	return
}

type CodecTool struct {
	storage *vector[[]byte]
}

func NewCodecTool() *CodecTool {
	return &CodecTool{
		storage: NewVector[[]byte](types.Type_CHAR.ToType(), false),
	}
}

func (tool *CodecTool) Close() { tool.storage.Close() }
func (tool *CodecTool) Reset() { tool.storage.Reset() }
func (tool *CodecTool) Write(buf []byte) (n int, err error) {
	tool.storage.Append(buf)
	n = len(buf)
	return
}

func (tool *CodecTool) WriteTo(w io.Writer) (n int64, err error) {
	return tool.storage.WriteTo(w)
}

func (tool *CodecTool) ReadFrom(r io.Reader) (n int64, err error) {
	return tool.storage.ReadFrom(r)
}

func (tool *CodecTool) ReadFromFile(f common.IVFile, buffer *bytes.Buffer) (err error) {
	return tool.storage.ReadFromFile(f, buffer)
}

func (tool *CodecTool) Allocated() int           { return tool.storage.Allocated() }
func (tool *CodecTool) Elements() int            { return tool.storage.Length() }
func (tool *CodecTool) Get(i int) []byte         { return any(tool.storage.Get(i)).([]byte) }
func (tool *CodecTool) Update(i int, buf []byte) { tool.storage.Update(i, buf) }
func (tool *CodecTool) Delete(i int)             { tool.storage.Delete(i) }
