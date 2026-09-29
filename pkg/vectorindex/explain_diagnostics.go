// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vectorindex

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const ivfSearchRoundDiagnosticHeading = "__mo_ivf_search_round_v1"
const ivfExecutionDiagnosticHeading = "__mo_ivf_execution_v1"

// IvfSearchRoundDiagnostic is the bounded, execution-owned account of one
// adaptive IVF search round. It is transported through Query.Headings because
// operator statistics already carry background Query values across CN
// boundaries, while the direct relation reader no longer creates nested SQL
// plans.
type IvfSearchRoundDiagnostic struct {
	Round        uint64
	BucketOffset uint64
	BucketCount  uint64
	RowLimit     uint64
	OutputRows   uint64
	Exhausted    bool
}

// EncodeIvfSearchRoundDiagnostic creates a plan-protobuf-safe diagnostic. It
// deliberately contains no Expr oneofs, so physical-plan JSON transport can
// carry it without the remote decoding problem of historical background SQL
// plans.
func EncodeIvfSearchRoundDiagnostic(d IvfSearchRoundDiagnostic) *plan.Query {
	exhausted := "0"
	if d.Exhausted {
		exhausted = "1"
	}
	return &plan.Query{Headings: []string{
		ivfSearchRoundDiagnosticHeading,
		strconv.FormatUint(d.Round, 10),
		strconv.FormatUint(d.BucketOffset, 10),
		strconv.FormatUint(d.BucketCount, 10),
		strconv.FormatUint(d.RowLimit, 10),
		strconv.FormatUint(d.OutputRows, 10),
		exhausted,
	}}
}

// DecodeIvfSearchRoundDiagnostic recognizes diagnostics emitted by
// EncodeIvfSearchRoundDiagnostic. Malformed or future-version values are left
// to the ordinary background-query path instead of being partially rendered.
func DecodeIvfSearchRoundDiagnostic(q *plan.Query) (IvfSearchRoundDiagnostic, bool) {
	var d IvfSearchRoundDiagnostic
	if q == nil || len(q.Headings) != 7 || q.Headings[0] != ivfSearchRoundDiagnosticHeading {
		return d, false
	}
	values := []*uint64{&d.Round, &d.BucketOffset, &d.BucketCount, &d.RowLimit, &d.OutputRows}
	for i, dst := range values {
		value, err := strconv.ParseUint(q.Headings[i+1], 10, 64)
		if err != nil {
			return IvfSearchRoundDiagnostic{}, false
		}
		*dst = value
	}
	switch q.Headings[6] {
	case "0":
	case "1":
		d.Exhausted = true
	default:
		return IvfSearchRoundDiagnostic{}, false
	}
	if d.Round == 0 || d.BucketCount == 0 {
		return IvfSearchRoundDiagnostic{}, false
	}
	return d, true
}

// IvfExecutionDiagnostic is one bounded reader-generation summary. Parallel
// readers emit independent values which the EXPLAIN renderer combines.
type IvfExecutionDiagnostic struct {
	SearchCount             uint64
	ReaderCount             uint64
	MetadataBlocks          uint64
	MetadataRows            uint64
	MetadataTimeNS          uint64
	CentroidBlocks          uint64
	CentroidRows            uint64
	CentroidTimeNS          uint64
	EntryBlocksSelected     uint64
	EntryBlocksRead         uint64
	EntryOutputRows         uint64
	EntryTimeNS             uint64
	StorageFilterInputRows  uint64
	StorageFilterOutputRows uint64
	VectorRowsScored        uint64
	VectorChunksRead        uint64
	VectorChunkCacheHits    uint64
	VectorCompressedBytes   uint64
	VectorDecodedBytes      uint64
	TopKOutputRows          uint64
	OutputRows              uint64
}

// Merge adds one reader-generation summary to the receiver.
func (d *IvfExecutionDiagnostic) Merge(other IvfExecutionDiagnostic) {
	if d == nil {
		return
	}
	d.SearchCount += other.SearchCount
	d.ReaderCount += other.ReaderCount
	d.MetadataBlocks += other.MetadataBlocks
	d.MetadataRows += other.MetadataRows
	d.MetadataTimeNS += other.MetadataTimeNS
	d.CentroidBlocks += other.CentroidBlocks
	d.CentroidRows += other.CentroidRows
	d.CentroidTimeNS += other.CentroidTimeNS
	d.EntryBlocksSelected += other.EntryBlocksSelected
	d.EntryBlocksRead += other.EntryBlocksRead
	d.EntryOutputRows += other.EntryOutputRows
	d.EntryTimeNS += other.EntryTimeNS
	d.StorageFilterInputRows += other.StorageFilterInputRows
	d.StorageFilterOutputRows += other.StorageFilterOutputRows
	d.VectorRowsScored += other.VectorRowsScored
	d.VectorChunksRead += other.VectorChunksRead
	d.VectorChunkCacheHits += other.VectorChunkCacheHits
	d.VectorCompressedBytes += other.VectorCompressedBytes
	d.VectorDecodedBytes += other.VectorDecodedBytes
	d.TopKOutputRows += other.TopKOutputRows
	d.OutputRows += other.OutputRows
}

// EncodeIvfExecutionDiagnostic creates a remote-plan-safe summary carrier.
func EncodeIvfExecutionDiagnostic(d IvfExecutionDiagnostic) *plan.Query {
	values := []uint64{
		d.SearchCount,
		d.ReaderCount,
		d.MetadataBlocks,
		d.MetadataRows,
		d.MetadataTimeNS,
		d.CentroidBlocks,
		d.CentroidRows,
		d.CentroidTimeNS,
		d.EntryBlocksSelected,
		d.EntryBlocksRead,
		d.EntryOutputRows,
		d.EntryTimeNS,
		d.StorageFilterInputRows,
		d.StorageFilterOutputRows,
		d.VectorRowsScored,
		d.VectorChunksRead,
		d.VectorChunkCacheHits,
		d.VectorCompressedBytes,
		d.VectorDecodedBytes,
		d.TopKOutputRows,
		d.OutputRows,
	}
	headings := make([]string, 1, len(values)+1)
	headings[0] = ivfExecutionDiagnosticHeading
	for _, value := range values {
		headings = append(headings, strconv.FormatUint(value, 10))
	}
	return &plan.Query{Headings: headings}
}

// DecodeIvfExecutionDiagnostic recognizes a complete v1 execution summary.
func DecodeIvfExecutionDiagnostic(q *plan.Query) (IvfExecutionDiagnostic, bool) {
	var d IvfExecutionDiagnostic
	if q == nil || len(q.Headings) != 22 || q.Headings[0] != ivfExecutionDiagnosticHeading {
		return d, false
	}
	destinations := []*uint64{
		&d.SearchCount,
		&d.ReaderCount,
		&d.MetadataBlocks,
		&d.MetadataRows,
		&d.MetadataTimeNS,
		&d.CentroidBlocks,
		&d.CentroidRows,
		&d.CentroidTimeNS,
		&d.EntryBlocksSelected,
		&d.EntryBlocksRead,
		&d.EntryOutputRows,
		&d.EntryTimeNS,
		&d.StorageFilterInputRows,
		&d.StorageFilterOutputRows,
		&d.VectorRowsScored,
		&d.VectorChunksRead,
		&d.VectorChunkCacheHits,
		&d.VectorCompressedBytes,
		&d.VectorDecodedBytes,
		&d.TopKOutputRows,
		&d.OutputRows,
	}
	for i, destination := range destinations {
		value, err := strconv.ParseUint(q.Headings[i+1], 10, 64)
		if err != nil {
			return IvfExecutionDiagnostic{}, false
		}
		*destination = value
	}
	return d, true
}
