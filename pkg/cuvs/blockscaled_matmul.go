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
)

// BlockScaledMatmul scores tiles of vector cells against a fixed set of query cells on a GPU
// with cuBLASLt: vecf8/vecf4 block-scaled cells, or raw vecf32/vecf16/vecbf16/vecint8/
// vecuint8 vectors. It is not safe for concurrent use.
type BlockScaledMatmul struct {
	ptr       C.gpu_blockscaled_matmul_c
	nq        int
	cellBytes int
	maxRows   int
	topk      int
}

// Engine formats, as in cgo/cuvs/blockscaled_matmul_c.h.
const (
	BlockScaledMatmulMXFP8 = int(C.GPU_BLOCKSCALED_MXFP8)
	BlockScaledMatmulNVFP4 = int(C.GPU_BLOCKSCALED_NVFP4)
	BlockScaledMatmulF32   = int(C.GPU_BLOCKSCALED_F32)
	BlockScaledMatmulF16   = int(C.GPU_BLOCKSCALED_F16)
	BlockScaledMatmulI8    = int(C.GPU_BLOCKSCALED_I8)
	BlockScaledMatmulU8    = int(C.GPU_BLOCKSCALED_U8)
	BlockScaledMatmulBF16  = int(C.GPU_BLOCKSCALED_BF16)
)

// Engine metrics, as in cgo/cuvs/blockscaled_matmul_c.h. The scores are the negated
// distances (largest is nearest): the dot product, -(cosine distance) or -(squared L2).
const (
	BlockScaledMatmulInnerProduct = int(C.GPU_BLOCKSCALED_METRIC_INNER_PRODUCT)
	BlockScaledMatmulCosine       = int(C.GPU_BLOCKSCALED_METRIC_COSINE)
	BlockScaledMatmulL2sq         = int(C.GPU_BLOCKSCALED_METRIC_L2SQ)
)

// BlockScaledMatmulDeviceCount returns the number of visible devices meeting the engine's
// baseline, compute capability 10.0 or newer; it is queried once per process.
func BlockScaledMatmulDeviceCount() int {
	return int(C.gpu_blockscaled_matmul_device_count())
}

// BlockScaledMatmulHostBytes returns the host memory NewBlockScaledMatmul allocates for
// this shape.
func BlockScaledMatmulHostBytes(format, dim, nq, maxRows int) uint64 {
	return uint64(C.gpu_blockscaled_matmul_host_bytes(C.int(format), C.uint32_t(dim), C.uint32_t(nq), C.uint64_t(maxRows)))
}

// NewBlockScaledMatmul creates an engine for nq query cells of format and dim, each
// cellBytes long and packed back to back in queryCells; a tile holds at most maxRows cells
// (rounded up to 128). topk > 0 enables RunTopK, which keeps min(topk, MaxRows) hits per
// query. metric is BlockScaledMatmulInnerProduct, BlockScaledMatmulCosine or
// BlockScaledMatmulL2sq.
func NewBlockScaledMatmul(format, dim, nq int, queryCells []byte, cellBytes, maxRows, topk, metric int) (*BlockScaledMatmul, error) {
	if dim <= 0 || nq <= 0 || maxRows <= 0 || cellBytes <= 0 || topk < 0 || len(queryCells) != nq*cellBytes {
		return nil, moerr.NewInvalidInputNoCtxf("block-scaled matmul: invalid dim %d, query count %d, tile %d or query bytes %d",
			dim, nq, maxRows, len(queryCells))
	}
	var errmsg *C.char
	ptr := C.gpu_blockscaled_matmul_new(C.int(format), C.uint32_t(dim), C.uint32_t(nq),
		(*C.uint8_t)(unsafe.Pointer(&queryCells[0])), C.uint64_t(maxRows), C.uint32_t(topk), C.int(metric), unsafe.Pointer(&errmsg))
	runtime.KeepAlive(queryCells)
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}
	m := &BlockScaledMatmul{
		ptr:       ptr,
		nq:        nq,
		cellBytes: cellBytes,
		maxRows:   int(C.gpu_blockscaled_matmul_max_rows(ptr)),
	}
	m.topk = min(topk, m.maxRows)
	return m, nil
}

// MaxRows returns the tile row capacity.
func (m *BlockScaledMatmul) MaxRows() int { return m.maxRows }

// CellBytes returns the byte length of one cell.
func (m *BlockScaledMatmul) CellBytes() int { return m.cellBytes }

// Run scores the cells packed back to back in cells (at most MaxRows) into scores, row
// major: scores[r*nq+q] is the rank score of cell r and query q, the negated distance of
// the metric (NaN as -Inf).
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

// TopK returns the hits per query kept by RunTopK: min(topk, MaxRows).
func (m *BlockScaledMatmul) TopK() int { return m.topk }

// RunTopK scores the cells like Run and keeps the TopK best rows per query on the GPU (NaN
// as -Inf). topScores and topRows receive nq*TopK entries, query major; topRows holds an
// index into cells or -1 for an unused slot. When query q has more rows tied at its TopK-th
// score than were kept, tied[q] is 1 and full[q*n:(q+1)*n] receives its n scores, where n
// is the cell count; otherwise that range of full is not written.
func (m *BlockScaledMatmul) RunTopK(cells []byte, topScores []float32, topRows []int32, full []float32, tied []uint8) error {
	if m.topk == 0 {
		return moerr.NewInvalidInputNoCtx("block-scaled matmul: engine created without topk")
	}
	if len(cells)%m.cellBytes != 0 {
		return moerr.NewInvalidInputNoCtxf("block-scaled matmul: %d bytes is not a whole number of %d-byte cells", len(cells), m.cellBytes)
	}
	n := len(cells) / m.cellBytes
	if n == 0 {
		return nil
	}
	if n > m.maxRows || len(topScores) < m.nq*m.topk || len(topRows) < m.nq*m.topk || len(full) < n*m.nq || len(tied) < m.nq {
		return moerr.NewInvalidInputNoCtxf("block-scaled matmul: %d cells exceed the tile of %d rows or the output buffers", n, m.maxRows)
	}
	var errmsg *C.char
	C.gpu_blockscaled_matmul_run_topk(m.ptr, (*C.uint8_t)(unsafe.Pointer(&cells[0])), C.uint64_t(n),
		(*C.float)(unsafe.Pointer(&topScores[0])), (*C.int32_t)(unsafe.Pointer(&topRows[0])),
		(*C.float)(unsafe.Pointer(&full[0])), (*C.uint8_t)(unsafe.Pointer(&tied[0])), unsafe.Pointer(&errmsg))
	runtime.KeepAlive(cells)
	runtime.KeepAlive(topScores)
	runtime.KeepAlive(topRows)
	runtime.KeepAlive(full)
	runtime.KeepAlive(tied)
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
