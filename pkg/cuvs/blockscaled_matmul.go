//go:build gpu

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

package cuvs

/*
#include "../../cgo/cuvs/blockscaled_matmul_c.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// BlockScaledMatmul scores tiles of vecf8/vecf4 cells against a fixed set of query cells
// on a GPU with cuBLASLt. It is not safe for concurrent use.
type BlockScaledMatmul struct {
	ptr       C.gpu_blockscaled_matmul_c
	nq        int
	cellBytes int
	maxRows   int
}

// NewBlockScaledMatmul creates an engine for nq query cells of format and dim, packed
// back to back in queryCells; a tile holds at most maxRows cells (rounded up to 128).
func NewBlockScaledMatmul(format types.BlockScaledFormat, dim, nq int, queryCells []byte, maxRows int) (*BlockScaledMatmul, error) {
	cellBytes := types.BlockScaledCellSize(format, dim)
	if dim <= 0 || nq <= 0 || maxRows <= 0 || len(queryCells) != nq*cellBytes {
		return nil, moerr.NewInvalidInputNoCtxf("block-scaled matmul: invalid dim %d, query count %d, tile %d or query bytes %d",
			dim, nq, maxRows, len(queryCells))
	}
	var errmsg *C.char
	ptr := C.gpu_blockscaled_matmul_new(C.int(format), C.uint32_t(dim), C.uint32_t(nq),
		(*C.uint8_t)(unsafe.Pointer(&queryCells[0])), C.uint64_t(maxRows), unsafe.Pointer(&errmsg))
	runtime.KeepAlive(queryCells)
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}
	return &BlockScaledMatmul{
		ptr:       ptr,
		nq:        nq,
		cellBytes: cellBytes,
		maxRows:   int(C.gpu_blockscaled_matmul_max_rows(ptr)),
	}, nil
}

// MaxRows returns the tile row capacity.
func (m *BlockScaledMatmul) MaxRows() int { return m.maxRows }

// CellBytes returns the byte length of one cell.
func (m *BlockScaledMatmul) CellBytes() int { return m.cellBytes }

// Run scores the cells packed back to back in cells (at most MaxRows) into scores, row
// major: scores[r*nq+q] is the dot product of cell r and query q.
func (m *BlockScaledMatmul) Run(cells []byte, scores []float32) error {
	if len(cells)%m.cellBytes != 0 {
		return moerr.NewInvalidInputNoCtxf("block-scaled matmul: %d bytes is not a whole number of %d-byte cells", len(cells), m.cellBytes)
	}
	n := len(cells) / m.cellBytes
	if n == 0 {
		return nil
	}
	if n > m.maxRows || len(scores) < n*m.nq {
		return moerr.NewInvalidInputNoCtxf("block-scaled matmul: %d cells exceed the tile of %d rows or the %d scores", n, m.maxRows, len(scores))
	}
	var errmsg *C.char
	C.gpu_blockscaled_matmul_run(m.ptr, (*C.uint8_t)(unsafe.Pointer(&cells[0])), C.uint64_t(n),
		(*C.float)(unsafe.Pointer(&scores[0])), unsafe.Pointer(&errmsg))
	runtime.KeepAlive(cells)
	runtime.KeepAlive(scores)
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Close releases the engine.
func (m *BlockScaledMatmul) Close() {
	if m.ptr != nil {
		C.gpu_blockscaled_matmul_destroy(m.ptr)
		m.ptr = nil
	}
}
