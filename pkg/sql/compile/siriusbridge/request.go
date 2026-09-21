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

package siriusbridge

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// MaxDescriptorBytes bounds the transient CGo arena before the first C
// allocation. Native query metadata has separate, configurable admission
// (256 MiB by default, including its own expansion/copy charges).
const MaxDescriptorBytes = 64 << 20

const maxMetadataTextBytes = 1 << 20
const maxColumns = 1024

func validateRequest(req Request) error {
	invalid := func() error { return moerr.NewInvalidInputNoCtx("invalid or oversized Sirius query descriptors") }
	if req.AccountID > uint64(^uint32(0)) || len(req.Reads) > 16 || len(req.Columns) == 0 || len(req.Columns) > maxColumns || len(req.Plan) == 0 || len(req.Plan) > 16<<20 || len(req.QueryID) == 0 || len(req.QueryID) > 256 {
		return invalid()
	}
	validText := func(text string, required bool) bool {
		return (!required || len(text) > 0) && len(text) <= maxMetadataTextBytes && strings.IndexByte(text, 0) < 0
	}
	// Conservative ABI-v1/Linux-amd64 descriptor envelopes include padding,
	// output arrays and both read-column/input-schema arrays. Their bounds are
	// intentionally independent of opaque C++ sizeof values.
	total := uint64(128 + len(req.Plan) + len(req.QueryID) + 64*len(req.Columns))
	add := func(size uint64) bool {
		if size > MaxDescriptorBytes-total {
			return false
		}
		total += size
		return true
	}
	names := 0
	for _, column := range req.Columns {
		if len(column.Name) > maxMetadataTextBytes-names || !validText(column.Name, true) {
			return invalid()
		}
		names += len(column.Name)
	}
	if !add(uint64(names)) {
		return invalid()
	}
	ids := make(map[uint64]struct{}, len(req.Reads))
	for _, read := range req.Reads {
		if read.BindingID == 0 || read.BindingID > uint64(^uint64(0)>>1) || len(read.Columns) == 0 || len(read.Columns) > maxColumns || len(read.TAEManifest) > 64<<20 {
			return invalid()
		}
		if _, exists := ids[read.BindingID]; exists {
			return invalid()
		}
		ids[read.BindingID] = struct{}{}
		if (read.Producer == nil) == (len(read.TAEManifest) == 0) || (read.Producer != nil && read.DataRoot != "") || (len(read.TAEManifest) > 0 && read.DataRoot == "") {
			return invalid()
		}
		canonical := uint64(256 + 128*len(read.Columns))
		addText := func(text string, required bool) bool {
			if uint64(len(text)) > maxMetadataTextBytes-canonical || !validText(text, required) {
				return false
			}
			canonical += uint64(len(text))
			return true
		}
		for _, identity := range []string{read.Database, read.Table, read.Schema, read.DataRoot} {
			if !addText(identity, false) {
				return invalid()
			}
		}
		for _, column := range read.Columns {
			if column.Sequence > 65535 || !addText(column.Name, true) {
				return invalid()
			}
		}
		if canonical > maxMetadataTextBytes || !add(canonical) || !add(uint64(len(read.TAEManifest))) {
			return invalid()
		}
	}
	return nil
}
