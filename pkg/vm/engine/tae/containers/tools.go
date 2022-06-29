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
