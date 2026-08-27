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

package mergesort

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

const transferMappingDigestDomain = "MO-LIFECYCLE-TRANSFER-v1"

// TransferMappingDigest fingerprints the exact CreatedObjs order and decoded
// TransferTable produced by DoMergeAndWrite. It is a transport/integrity
// check; it intentionally does not attempt to re-evaluate Lifecycle business
// classification on TN.
func TransferMappingDigest(
	createdObjectStats [][]byte,
	table *TransferTable,
) [sha256.Size]byte {
	sum := sha256.New()
	_, _ = sum.Write([]byte(transferMappingDigestDomain))
	writeDigestUint32(sum, uint32(len(createdObjectStats)))
	for _, stats := range createdObjectStats {
		writeDigestUint32(sum, uint32(len(stats)))
		_, _ = sum.Write(stats)
	}
	if table == nil {
		writeDigestUint32(sum, 0)
		var result [sha256.Size]byte
		copy(result[:], sum.Sum(nil))
		return result
	}
	writeDigestUint32(sum, uint32(table.Len()))
	for blockOrdinal := 0; blockOrdinal < table.Len(); blockOrdinal++ {
		mapping := table.GetBlockMap(blockOrdinal)
		writeDigestUint32(sum, uint32(blockOrdinal))
		writeDigestUint32(sum, uint32(len(mapping)))
		for rowOffset, destination := range mapping {
			writeDigestUint32(sum, uint32(rowOffset))
			_, _ = sum.Write([]byte{destination.ObjIdx})
			if destination.ObjIdx == api.NoTransfer {
				// NoTransfer's remaining fields have no meaning. Canonicalize
				// them so stale bytes cannot create cross-version drift.
				writeDigestUint16(sum, 0)
				writeDigestUint32(sum, 0)
				continue
			}
			writeDigestUint16(sum, destination.BlkIdx)
			writeDigestUint32(sum, destination.RowIdx)
		}
	}
	var result [sha256.Size]byte
	copy(result[:], sum.Sum(nil))
	return result
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeDigestUint16(writer digestWriter, value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeDigestUint32(writer digestWriter, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}
