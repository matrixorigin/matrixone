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

package batch

import (
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// Older CNs appended a ROLLUP grouping extension after the stable batch,
// framed by this footer; its last byte versions the extension. Readers strip
// and discard it: every rolled-up row also carries its NULL bit, and GROUPING
// is folded by the planner, so the bits carry nothing a reader needs.
const pipelineGroupingMagic = "MOGROUP\x01"
const pipelineGroupingFooterSize = 16

// UnmarshalBinaryForPipeline decodes a stable batch, discarding a legacy
// grouping extension when one is present. The caller owns cleanup after
// errors, as with the stable decoder.
func (bat *Batch) UnmarshalBinaryForPipeline(data []byte, mp *mpool.MPool) error {
	prefix, err := stableBatchPayloadLength(data)
	if err != nil {
		return err
	}
	end := len(data) - pipelineGroupingFooterSize
	// Inspect only bytes outside the stable payload: user values or ExtraBuf
	// may legitimately end with the footer magic.
	if end >= prefix && string(data[end:end+8]) == pipelineGroupingMagic {
		size := binary.LittleEndian.Uint64(data[end+8:])
		if size > uint64(end-prefix) || size < 12 {
			return moerr.NewInvalidInputNoCtx("invalid pipeline grouping payload size")
		}
		data = data[:end-int(size)]
	}
	return bat.unmarshalBinaryWithMetadataAtPrefix(data, prefix, mp)
}
