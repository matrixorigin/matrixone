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

package readutil

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"go.uber.org/zap"
)

const (
	// scanPaceMin is how far a scan goes before its own pace is used.
	scanPaceMinBlocks = 8
	scanPaceMinTime   = 50 * time.Millisecond
	// scanPaceWeight is the weight of a scan's pace in the pace of scans.
	scanPaceWeight = 0.2
)

// scanPaces is the average pace of the scans, in blocks a second.
var scanPaces atomic.Uint64

func init() {
	scanPaces.Store(math.Float64bits(100))
}

// ScanPrefetch is the prefetch state of one scan: what its prefetches are for, and where
// and when its pace is measured from.
type ScanPrefetch struct {
	scan       *fileservice.PrefetchScan
	paceStart  time.Time
	paceCursor int
}

// Scan returns what the scan's prefetches are for.
func (p *ScanPrefetch) Scan() *fileservice.PrefetchScan {
	if p.scan == nil {
		p.scan = new(fileservice.PrefetchScan)
	}
	return p.scan
}

// End ends the scan's prefetches.
func (p *ScanPrefetch) End() {
	p.scan.End()
	p.scan = nil
}

// Pace returns the blocks a second the scan reads, at cursor: its own pace once it has
// read enough to measure it, which counts toward the pace of scans, or that pace before.
func (p *ScanPrefetch) Pace(now time.Time, cursor int) float64 {
	if p.paceStart.IsZero() {
		p.paceStart, p.paceCursor = now, cursor
	}
	blocks, elapsed := cursor-p.paceCursor, now.Sub(p.paceStart)
	average := math.Float64frombits(scanPaces.Load())
	if blocks < scanPaceMinBlocks || elapsed < scanPaceMinTime {
		return average
	}
	pace := float64(blocks) / elapsed.Seconds()
	scanPaces.Store(math.Float64bits((1-scanPaceWeight)*average + scanPaceWeight*pace))
	return pace
}

// PrefetchColumns prefetches the ranges that hold the columns seqNums across all the
// blocks of the object of blk, due at due. It prefetches nothing when fs prefetches no
// objects, or the object records no ranges for the columns.
func (p *ScanPrefetch) PrefetchColumns(
	ctx context.Context, fs fileservice.FileService, blk *objectio.BlockInfo, seqNums []uint16, due int64,
) {
	prefetcher, ok := fs.(fileservice.ObjectPrefetcher)
	if !ok {
		return
	}
	ranges, bytes, recorded, err := ColumnRanges(ctx, fs, blk, seqNums)
	if err != nil {
		logutil.Debug("prefetch columns", zap.String("blk", blk.String()), zap.Error(err))
		return
	}
	if !recorded || len(ranges) == 0 {
		return
	}
	err = prefetcher.PrefetchObject(ctx, fileservice.ObjectPrefetch{
		FilePath: blk.MetaLocation().Name().String(),
		Size:     bytes, Due: due, Ranges: ranges, Scan: p.Scan(),
	})
	if err != nil {
		logutil.Debug("prefetch columns", zap.String("blk", blk.String()), zap.Error(err))
	}
}

// ColumnRanges returns the ranges that hold the columns seqNums across all the blocks of
// the object of blk, and their bytes in all. It returns false when the object stores a
// column of seqNums without recording its range.
func ColumnRanges(
	ctx context.Context, fs fileservice.FileService, blk *objectio.BlockInfo, seqNums []uint16,
) ([]fileservice.PrefetchRange, int64, bool, error) {
	loc := blk.MetaLocation()
	meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
	if err != nil {
		return nil, 0, false, err
	}
	data := meta.MustDataMeta()
	var ranges []fileservice.PrefetchRange
	var bytes int64
	recorded := true
	for _, seq := range seqNums {
		if seq > data.BlockHeader().MaxSeqnum() {
			continue
		}
		ext := data.MustGetColumn(seq).Location()
		if ext.Length() == 0 {
			recorded = false
			continue
		}
		ranges = append(ranges, fileservice.PrefetchRange{Offset: int64(ext.Offset()), Size: int64(ext.Length())})
		bytes += int64(ext.Length())
	}
	return ranges, bytes, recorded, nil
}
